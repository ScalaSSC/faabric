#include <faabric/batch-scheduler/SchedulingDecision.h>
#include <faabric/planner/Planner.h>
#include <faabric/proto/faabric.pb.h>
#include <faabric/scheduler/FunctionCallClient.h>
#include <faabric/snapshot/SnapshotClient.h>
#include <faabric/state/FunctionStateClient.h>
#include <faabric/state/State.h>
#include <faabric/transport/PointToPointBroker.h>
#include <faabric/util/batch.h>
#include <faabric/util/clock.h>
#include <faabric/util/config.h>
#include <faabric/util/environment.h>
#include <faabric/util/func.h>
#include <faabric/util/gids.h>
#include <faabric/util/locks.h>
#include <faabric/util/logging.h>
#include <faabric/util/map.h>
#include <faabric/util/message.h>
#include <faabric/util/string_tools.h>

#include <cmath>
#include <fstream>
#include <future>
#include <map>
#include <memory>
#include <rapidjson/document.h>
#include <rapidjson/prettywriter.h>
#include <rapidjson/stringbuffer.h>
#include <rapidjson/writer.h>
#include <string>
#include <thread>
#include <vector>

namespace faabric::planner {

const std::string PLANNER_ENQUEUE_TIME_KEY = "planner_queue_time_key";

#define CONTINUE_IF_OUTPUTTING                                                 \
    if (isOutputting) {                                                        \
        continue;                                                              \
    }
#define RETURN_IF_OUTPUTTING                                                   \
    if (isOutputting) {                                                        \
        return;                                                                \
    }

// ----------------------
// Static methods
// ----------------------

static faabric::batch_scheduler::HostMap convertToHostMap(
  std::map<std::string, std::shared_ptr<Host>> hostMapIn)
{
    faabric::batch_scheduler::HostMap hostMap;

    for (const auto& [ip, host] : hostMapIn) {
        hostMap[ip] = std::make_shared<faabric::batch_scheduler::HostState>(
          host->ip(), host->slots(), host->usedslots());
    }

    return hostMap;
}

// ----------------------
// Planner
// ----------------------

// Planner is used globally as a static variable. This constructor relies on
// the fact that C++ static variable's initialisation is thread-safe
Planner::Planner()
  : snapshotRegistry(faabric::snapshot::getSnapshotRegistry())
{
    // Note that we don't initialise the config in a separate method to prevent
    // that method from being called elsewhere in the codebase (as it would be
    // thread-unsafe)
    config.set_ip(faabric::util::getSystemConfig().endpointHost);
    config.set_hosttimeout(std::stoi(
      faabric::util::getEnvVar("PLANNER_HOST_KEEPALIVE_TIMEOUT", "5")));
    config.set_numthreadshttpserver(
      std::stoi(faabric::util::getEnvVar("PLANNER_HTTP_SERVER_THREADS", "20")));

    printConfig();

    // Register the information for parallelism scaling
    lastParallelismUpdate = faabric::util::getGlobalClock().epochMillis();
    parallelismUpdateInterval =
      faabric::util::getSystemConfig().parallelismUpdateInterval;
    isPreloadParallelism = faabric::util::getSystemConfig().preloadParallelism;

    dequeueScheduledMsgsThread =
      std::thread(&Planner::dequeueScheduledMsgs, this);

    updateRuntimeStatsThread = std::thread(&Planner::updateRuntimeStats, this);
    processWaitingQueueThread =
      std::thread(&Planner::processWaitingQueueLoop, this);
}

Planner::~Planner()
{
    // Stop the batch timer thread
    stopThreadTimer = true;
    if (dequeueScheduledMsgsThread.joinable()) {
        dequeueScheduledMsgsThread.join();
    }
    if (updateRuntimeStatsThread.joinable()) {
        updateRuntimeStatsThread.join();
    }
    if (processWaitingQueueThread.joinable()) {
        processWaitingQueueThread.join();
    }
}

PlannerConfig Planner::getConfig()
{
    return config;
}

void Planner::printConfig() const
{
    SPDLOG_INFO("--- Planner Conifg ---");
    SPDLOG_INFO("HOST_KEEP_ALIVE_TIMEOUT    {}", config.hosttimeout());
    SPDLOG_INFO("HTTP_SERVER_THREADS        {}", config.numthreadshttpserver());
}

bool Planner::reset()
{
    SPDLOG_INFO("Resetting planner");

    flushSchedulingState();

    flushExecutors();

    flushHosts();

    faabric::state::getGlobalState().resetPersistentLockState();
    faabric::state::getGlobalState().persistentLock = false;

    return true;
}

bool Planner::flush(faabric::planner::FlushType flushType)
{
    switch (flushType) {
        case faabric::planner::FlushType::Hosts:
            SPDLOG_INFO("Planner flushing available hosts state");
            flushHosts();
            return true;
        case faabric::planner::FlushType::Executors:
            SPDLOG_INFO("Planner flushing executors");
            flushExecutors();
            return true;
        case faabric::planner::FlushType::SchedulingState:
            SPDLOG_INFO("Planner flushing scheduling state");
            flushSchedulingState();
            return true;
        default:
            SPDLOG_ERROR("Unrecognised flush type");
            return false;
    }
}

void Planner::flushHosts()
{
    faabric::util::FullLock lock(plannerMx);

    state.hostMap.clear();
    state.activeHosts.clear();
}

void Planner::flushExecutors()
{
    faabric::util::FullLock lock(plannerMx);
    faabric::util::FullLock rflock(reconfigMx);

    if (stateAwareScheduler) {
        stateAwareScheduler->resetScheduler();
    }

    migrationVersion = 0;
    migrationDurations.clear();

    auto availableHosts = getAvailableHosts(true);
    for (const auto& host : availableHosts) {
        SPDLOG_INFO("Planner sending EXECUTOR flush to {}", host->ip());
        faabric::scheduler::getFunctionCallClient(host->ip())->sendFlush();
    }
}

void Planner::flushSchedulingState()
{
    faabric::util::FullLock lock(plannerMx);
    faabric::util::FullLock rflock(reconfigMx);

    state.inFlightReqs.clear();
    state.appResults.clear();
    state.appResultWaiters.clear();
    state.numMigrations = 0;
    state.inFlightApps.clear();
    state.appStartTimes.clear();
    state.applicationMetrics =
      std::make_unique<ApplicationMetrics>("defaultApp", 1);

    state.activeHosts = convertToHostMap(state.hostMap);
    schedHostNum = 0;
}

std::vector<std::shared_ptr<Host>> Planner::getAvailableHosts(bool locked)
{
    SPDLOG_DEBUG("Planner received request to get available hosts");

    // Acquire a full lock because we will also remove the hosts that have
    // timed out
    std::unique_ptr<faabric::util::FullLock> lock;
    if (!locked) {
        lock = std::make_unique<faabric::util::FullLock>(plannerMx);
    }

    std::vector<std::string> hostsToRemove;
    std::vector<std::shared_ptr<Host>> availableHosts;
    auto timeNowMs = faabric::util::getGlobalClock().epochMillis();
    for (const auto& [ip, host] : state.hostMap) {
        if (isHostExpired(host, timeNowMs)) {
            hostsToRemove.push_back(ip);
        } else {
            availableHosts.push_back(host);
        }
    }

    for (const auto& host : hostsToRemove) {
        state.hostMap.erase(host);
    }

    return availableHosts;
}

// Deliberately take a const reference as an argument to force a copy and take
// ownership of the host
bool Planner::registerHost(const Host& hostIn, bool overwrite)
{
    SPDLOG_TRACE("Planner received request to register host {}", hostIn.ip());

    // Sanity check the input argument
    if (hostIn.slots() < 0) {
        SPDLOG_ERROR(
          "Received erroneous request to register host {} with {} slots",
          hostIn.ip(),
          hostIn.slots());
        return false;
    }

    bool requireHostSync = false;
    faabric::util::FullLock lock(plannerMx);
    faabric::util::FullLock rflock(reconfigMx);
    auto it = state.hostMap.find(hostIn.ip());
    if (it == state.hostMap.end() || isHostExpired(it->second)) {
        // If the host entry has expired, we remove it and treat the host
        // as a new one
        if (it != state.hostMap.end()) {
            state.hostMap.erase(it);
        }

        // If its the first time we see this IP, give it a UID and add it to
        // the map
        SPDLOG_INFO(
          "Registering host {} with {} slots", hostIn.ip(), hostIn.slots());
        auto regHost = std::make_shared<Host>(hostIn);
        state.hostMap.emplace(
          std::make_pair<std::string, std::shared_ptr<Host>>(
            (std::string)hostIn.ip(), std::move(regHost)));
        requireHostSync = true;
    } else if (it != state.hostMap.end() && overwrite) {
        // We allow overwritting the host state by sending another register
        // request with same IP but different host resources. This is useful
        // for testing and resetting purposes
        SPDLOG_INFO("Overwritting host {} with {} slots (used {})",
                    hostIn.ip(),
                    hostIn.slots(),
                    hostIn.usedslots());
        it->second->set_slots(hostIn.slots());
        it->second->set_usedslots(hostIn.usedslots());
        requireHostSync = true;
    } else if (it != state.hostMap.end()) {
        SPDLOG_TRACE("NOT overwritting host {} with {} slots (used {})",
                     hostIn.ip(),
                     hostIn.slots(),
                     hostIn.usedslots());
    }

    if (requireHostSync) {
        // Update the host map to decentralized schedulers.
        for (const auto& [ip, host] : state.hostMap) {
            host->set_hostsync(true);
        }
        state.activeHosts = convertToHostMap(state.hostMap);
    }

    // Irrespective, set the timestamp
    SPDLOG_TRACE("Setting timestamp for host {}", hostIn.ip());
    state.hostMap.at(hostIn.ip())
      ->mutable_registerts()
      ->set_epochms(faabric::util::getGlobalClock().epochMillis());

    return true;
}

const std::pair<bool, HostPtrMap&> Planner::getRegisteredHost(
  std::string hostIp)
{
    // FullLock: hostsync is a write, SharedLock would be a data race.
    faabric::util::FullLock lock(plannerMx);

    auto it = state.hostMap.find(hostIp);
    if (it == state.hostMap.end()) {
        SPDLOG_ERROR("Host {} not found in planner", hostIp);
        throw std::runtime_error("Host not found in planner");
    }
    if (it->second->hostsync()) {
        it->second->set_hostsync(false);
        return { true, state.hostMap };
    }

    return { false, state.hostMap };
}

void Planner::removeHost(const Host& hostIn)
{
    SPDLOG_INFO("Planner received request to remove host {}", hostIn.ip());

    // We could acquire first a read lock to see if the host is in the host
    // map, and then acquire a write lock to remove it, but we don't do it
    // as we don't expect that much throughput in the planner
    faabric::util::FullLock lock(plannerMx);

    auto it = state.hostMap.find(hostIn.ip());
    if (it != state.hostMap.end()) {
        SPDLOG_DEBUG("Planner removing host {}", hostIn.ip());
        state.hostMap.erase(it);
    }
    state.activeHosts = convertToHostMap(state.hostMap);
}

bool Planner::isHostExpired(std::shared_ptr<Host> host, long epochTimeMs)
{
    // Allow calling the method without a timestamp, and we calculate it now
    if (epochTimeMs == 0) {
        epochTimeMs = faabric::util::getGlobalClock().epochMillis();
    }

    long hostTimeoutMs = getConfig().hosttimeout() * 1000;
    return (epochTimeMs - host->registerts().epochms()) > hostTimeoutMs;
}

// IMPORTANT : A -> B. But the message result of B can be set before A.
void Planner::setMessageResultBatch(
  std::shared_ptr<faabric::BatchExecuteRequest> batchMsg)
{
    SPDLOG_DEBUG("Planner received message result batch with {} messages",
                 batchMsg->messages_size());

    RETURN_IF_OUTPUTTING
    faabric::util::FullLock reqStatusLock(state.reqStatusMx);
    RETURN_IF_OUTPUTTING

    SPDLOG_DEBUG("InFlightApps size before set: {}", state.inFlightApps.size());
    for (int msgIdx = 0; msgIdx < batchMsg->messages_size(); msgIdx++) {
        auto msg = batchMsg->messages(msgIdx);
        int appId = msg.appid();
        int msgId = msg.id();

        // This check ensures we don't process results for an app that
        // should have already been cleaned up.
        if (!state.inFlightApps.contains(appId)) {
            SPDLOG_WARN(
              "App {} is not in flight but received result for msg {}",
              appId,
              msgId);
            continue;
        }

        // Store the result for the current message
        state.appResults[appId][msgId] =
          std::make_shared<faabric::Message>(msg);

        // For each message this one chained to, add it to the in-flight set
        // ONLY if we haven't already seen its result.
        for (int32_t chainedMsgId : msg.chainedmsgids()) {
            auto resultRecord = state.appResults.at(appId);
            if (!resultRecord.contains(chainedMsgId)) {
                state.inFlightApps[appId].insert(chainedMsgId);
            }
        }

        // Remove the current message from the in-flight set
        state.inFlightApps[appId].erase(msgId);

        // If this was the last in-flight message for the app, clean up.
        if (state.inFlightApps.at(appId).empty()) {
            int inFlightCount = state.inFlightApps.size() - 1;
            // Record metrics before erasing the results
            int64_t recordStartTime = state.appStartTimes[appId];
            state.applicationMetrics->record(
              state.appResults.at(appId), inFlightCount, recordStartTime);
            // Erase the app from tracking
            state.appResults.erase(appId);
            state.inFlightApps.erase(appId);
            state.appStartTimes.erase(appId);
        }
    }
    SPDLOG_DEBUG("InFlightApps size after set: {}", state.inFlightApps.size());
}

std::shared_ptr<faabric::BatchExecuteRequestStatus> Planner::getBatchResults(
  int32_t appId)
{
    auto berStatus = faabric::util::batchExecStatusFactory(appId);

    // Acquire a read lock to copy all the results we have for this batch
    {
        faabric::util::SharedLock lock(plannerMx);

        if (!state.appResults.contains(appId)) {
            return nullptr;
        }

        // If it's not finish, we just return the empty result
        if (state.inFlightApps.contains(appId)) {
            berStatus->set_finished(false);
            return berStatus;
        }

        for (auto msgResultPair : state.appResults.at(appId)) {
            *berStatus->add_messageresults() = *(msgResultPair.second);
        }

        // Set the finished condition
        berStatus->set_finished(!state.inFlightApps.contains(appId));

        // WARNING: It might affect the get message result function and the
        // ExecGraph function. But in stream processing, it is not a problem.
        // Clean the queried results.
        if (berStatus->finished()) {
            state.appResults.erase(appId);
        }
    }

    return berStatus;
}

std::shared_ptr<faabric::batch_scheduler::SchedulingDecision>
Planner::getSchedulingDecision(std::shared_ptr<BatchExecuteRequest> req)
{
    int appId = req->appid();

    // Acquire a read lock to get the scheduling decision for the requested app
    faabric::util::SharedLock lock(plannerMx);

    if (state.inFlightReqs.find(appId) == state.inFlightReqs.end()) {
        return nullptr;
    }

    return state.inFlightReqs.at(appId).second;
}

int Planner::getNumMigrations()
{
    return state.numMigrations.load(std::memory_order_acquire);
}

bool Planner::enqueueBatchRequest(
  std::shared_ptr<faabric::BatchExecuteRequest> req)
{
    int msgCount = req->messages_size();
    state.applicationMetrics->recordInputRate(msgCount);
    if (waitingMessageQueue.size() + msgCount > maxWaitingQueueSize) {
        SPDLOG_DEBUG("Waiting message queue is full (Current: {}, Incoming: "
                     "{}). Rejecting request.",
                     waitingMessageQueue.size(),
                     msgCount);
        return false;
    }

    int64_t enqueueTimeMicros = faabric::util::getGlobalClock().epochMicros();
    for (int i = 0; i < msgCount; i++) {
        auto msgPtr = std::make_shared<faabric::Message>(req->messages(i));
        (*msgPtr->mutable_metricrecorder())[PLANNER_ENQUEUE_TIME_KEY] =
          enqueueTimeMicros;
        waitingMessageQueue.enqueue(msgPtr);
    }

    SPDLOG_DEBUG("Enqueued {} messages. Current waiting queue size: {}",
                 msgCount,
                 waitingMessageQueue.size());
    return true;
}

void Planner::processWaitingQueueLoop()
{
    while (!stopThreadTimer) {
        std::this_thread::sleep_for(std::chrono::milliseconds(dispatchPeriod));

        CONTINUE_IF_OUTPUTTING

        // Checking in-flight apps and schedule if with available slots.
        int currentInflight = getInFlightAppsSize();
        int availableSlots = maxInflightApps.load() - currentInflight;

        if (availableSlots > 0 && waitingMessageQueue.size() > 0) {

            int msgsToSchedule = std::min(
              availableSlots, static_cast<int>(waitingMessageQueue.size()));
            auto tempBatchReq =
              std::make_shared<faabric::BatchExecuteRequest>();

            for (int i = 0; i < msgsToSchedule; ++i) {
                std::shared_ptr<faabric::Message> msg;
                if (waitingMessageQueue.try_dequeue(msg)) {
                    *tempBatchReq->add_messages() = *msg;
                } else {
                    break;
                }
            }
            if (tempBatchReq->messages_size() > 0) {
                SPDLOG_DEBUG("Dequeued {} messages. Sending to schedule...",
                             tempBatchReq->messages_size());
                scheduleMessages(tempBatchReq, false);
            }
        }
    }
}

// Schedule Messages should not called when reschedule state, outputting the
// result.
void Planner::scheduleMessages(std::shared_ptr<BatchExecuteRequest> req,
                               bool isChained)
{
    SPDLOG_DEBUG("Planner is Scheduling {} messages", req->messages_size());
    auto currentTime = faabric::util::getGlobalClock().epochMicros();

    // When outputing result, we doesn't allow any schedule message operation.
    RETURN_IF_OUTPUTTING
    // First loop: handle state without creating a shared_ptr
    faabric::util::FullLock lock(plannerMx);
    // check again
    RETURN_IF_OUTPUTTING
    int i = 0;
    while (i < req->messages_size()) {
        auto* message = req->mutable_messages(i); // Use a pointer directly

        // Record the chained call count
        int appid = message->appid();
        int chainedid = message->chainedid();
        // Now we asssume AppId is equal to ChainedId
        if (chainedid != appid) {
            SPDLOG_ERROR("ChainedId is not equal to AppId");
            throw std::runtime_error("ChainedId is not equal to AppId");
        }
        {
            faabric::util::FullLock reqStatusLock(state.reqStatusMx);
            if (!isChained) {
                // Flush the old chainedId
                // SPDLOG_DEBUG("Scheduling message with appId {} and msgId {}",
                //              appid,
                //              message->id());
                state.inFlightApps[appid].clear();
                state.inFlightApps[appid].insert(message->id());
                state.appStartTimes[appid] =
                  faabric::util::getGlobalClock().epochMicros();
                message->set_plannerqueuetime(currentTime);
            }
            // state.inFlightApps[appid]++;
        }

        i++; // Only increment i if a message was not removed
    }

    // Second loop: prepare messages and transfer ownership
    std::vector<std::unique_ptr<faabric::Message>> messages;
    messages.reserve(req->messages_size());
    for (int i = 0; i < req->messages_size(); i++) {
        auto message =
          std::make_unique<faabric::Message>(*req->mutable_messages(i));
        messages.push_back(std::move(message));
    }
    auto hosts =
      stateAwareScheduler->scheduleMessagesBatch(state.activeHosts, messages);

    doEnqueueSchedMessages(hosts, std::move(messages)); // Move ownership
}

void Planner::doEnqueueSchedMessages(
  std::vector<std::string> hosts,
  std::vector<std::unique_ptr<faabric::Message>> msgs)
{
    std::map<std::string, std::map<std::string, int>> msgCallsCounter;

    auto currentTime = faabric::util::getGlobalClock().epochMicros();
    for (int i = 0; i < msgs.size(); i++) {
        auto msg = std::move(msgs[i]);
        auto host = hosts[i];
        msg->set_plannerpoptime(currentTime);

        std::string userFuncPar = faabric::util::getUserFuncPar(*msg);
        msgCallsCounter[userFuncPar][host]++;

        state.scheduledMsgsMap[host].push_back(std::move(msg));
    }

    for (auto& [instancesName, hostCounter] : msgCallsCounter) {
        for (auto& [host, count] : hostCounter) {
            runtimeStats.instanceGenerate(instancesName, host, count);
        }
    }
}

void Planner::dequeueScheduledMsgs()
{
    while (!stopThreadTimer) {
        // Sleep for a while to batch the scheduled requests
        std::this_thread::sleep_for(std::chrono::milliseconds(dispatchPeriod));
        // Lock only for copying and clearing `scheduledMsgsMap`
        CONTINUE_IF_OUTPUTTING
        faabric::util::FullLock lock(plannerMx);
        CONTINUE_IF_OUTPUTTING
        if (state.scheduledMsgsMap.empty()) {
            lock.unlock();
            continue;
        }
        auto currentTime = faabric::util::getGlobalClock().epochMicros();

        // Create a local filtered copy of scheduledRequestsMap
        std::map<std::string, std::list<std::unique_ptr<faabric::Message>>>
          msgsCallMap;
        for (auto& [hostIp, msgsList] : state.scheduledMsgsMap) {
            if (msgsList.empty()) {
                continue;
            }
            for (auto& msg : msgsList) {
                // We only set the dispatch time for input.
                // For chained message, dispatch time is set from worker (it is
                // not zero).
                if (msg->plannerdispatchtime() == 0) {
                    msg->set_plannerdispatchtime(currentTime);
                }
            }
            msgsCallMap[hostIp] = std::move(msgsList); // Move ownership
        }
        state.scheduledMsgsMap.clear();

        // Parallel execution of function calls for each host
        std::vector<std::thread> threads;
        for (auto& [hostIp, msgs] : msgsCallMap) {
            threads.emplace_back(
              [hostIp](
                std::list<std::unique_ptr<faabric::Message>> msgsIn) mutable {
                  faabric::scheduler::getFunctionCallClient(hostIp)
                    ->executeFunctionsBatch(std::move(msgsIn));
              },
              std::move(msgs));
        }
        // Join all threads to ensure they complete before next iteration
        for (auto& t : threads) {
            if (t.joinable()) {
                t.join();
            }
        }
    }
}

int Planner::getInFlightAppsSize()
{
    faabric::util::SharedLock reqStatusLock(state.reqStatusMx);
    // SPDLOG_DEBUG("Getting in-flight apps size: {}",
    // state.inFlightApps.size());
    return state.inFlightApps.size();
}

std::map<std::string, FunctionMetrics> Planner::collectMetrics()
{
    // metrics states contains two types information: topology and function
    // Topology info is stored with source function name: UserFunction
    // Function info is stored with function name with parallelism:
    // UserFuncPar
    std::map<std::string, FunctionMetrics> metricsStats;
    SPDLOG_ERROR("Collecting metrics is not implemented yet");
    throw std::runtime_error("Collecting metrics is not implemented yet");
    return metricsStats;
}

/***
 * The logic of registerApp is save the application workflow in the scheduler
 ***/

bool Planner::registerApp(faabric::planner::RegisterApplicationRequest& rawReq,
                          std::unique_ptr<batch_scheduler::Application> app)
{
    SPDLOG_INFO("Planner registers application {}", app->getName());
    faabric::util::FullLock lock(plannerMx);
    stateAwareScheduler->registerApp(std::move(app));
    distributeApp(rawReq);

    if (schedHostNum != 0 && schedHostNum < state.hostMap.size()) {
        auto tempHostMap = convertToHostMap(state.hostMap);
        state.activeHosts =
          faabric::util::getFirstNElements(tempHostMap, schedHostNum);
    }
    stateAwareScheduler->initApp(state.activeHosts);
    stateAwareScheduler->scheduleApp(state.activeHosts);
    int curVersion = migrationVersion++;
    doDistributeStatesInfo(curVersion, {}, true);

    return true;
}

void Planner::distributeApp(
  faabric::planner::RegisterApplicationRequest& rawReq)
{
    SPDLOG_INFO("Planner distributes application {} to workers",
                rawReq.appname());
    auto req =
      std::make_shared<faabric::planner::RegisterApplicationRequest>(rawReq);
    std::vector<std::thread> threads;
    for (const auto& [ip, host] : state.hostMap) {
        threads.emplace_back([&, ip]() {
            try {
                SPDLOG_DEBUG("Planner distributes application to host {}", ip);
                faabric::scheduler::getFunctionCallClient(ip)
                  ->registerApplication(req);
            } catch (const std::exception& e) {
                SPDLOG_ERROR("Failed to distributes application to host {}: {}",
                             ip,
                             e.what());
                throw e;
            }
        });
    }
    for (auto& t : threads) {
        if (t.joinable()) {
            t.join();
        }
    }
}

void Planner::doDistributeCustomInfo(
  std::shared_ptr<faabric::CustomRequest> msg)
{
    int hostSize = state.hostMap.size();
    SPDLOG_INFO("Planner distribute custom info to {} hosts", hostSize);
    std::vector<std::thread> threads;
    for (const auto& [ip, host] : state.hostMap) {
        threads.emplace_back([&, ip]() {
            try {
                SPDLOG_DEBUG("Planner distributes custom to host {}", ip);
                faabric::scheduler::getFunctionCallClient(ip)->custom(msg);
            } catch (const std::exception& e) {
                SPDLOG_ERROR("Failed to distributes application to host {}: {}",
                             ip,
                             e.what());
                throw e;
            }
        });
    }
    for (auto& t : threads) {
        if (t.joinable()) {
            t.join();
        }
    }
}

void logMigrationMap(std::string_view title,
                     const std::map<std::string, std::set<std::string>>& map,
                     const char* arrow)
{
    std::stringstream ss;
    ss << title << ": ";
    for (const auto& [key, valSet] : map) {
        ss << "[" << key << " " << arrow << " { ";
        for (const auto& item : valSet) {
            ss << item << " ";
        }
        ss << "}] ";
    }
    SPDLOG_INFO("{}", ss.str());
}

template<typename ProtoMapType>
void fillProtoMap(ProtoMapType* protoMap,
                  const std::map<std::string, std::set<std::string>>& srcMap)
{
    if (!protoMap)
        return;

    for (const auto& [ip, hosts] : srcMap) {
        auto& hostList = (*protoMap)[ip];
        for (const auto& host : hosts) {
            hostList.add_hosts(host);
        }
    }
}

void Planner::doDistributeStatesInfo(
  int curVersion,
  const std::map<std::string, faabric::batch_scheduler::ScheduledOperator>
    preOperatorsMap,
  bool initialize)
{
    int hostSize = state.hostMap.size();
    SPDLOG_INFO("Planner distribute state info to {} hosts", hostSize);

    // Prepare state info sync request
    auto registStatesInfo = stateAwareScheduler->getStateInfo();
    auto req = std::make_shared<faabric::planner::SyncStatesInfoRequest>();
    for (const auto& [func, info] : registStatesInfo) {
        auto stateInfo = req->add_statesinfo();
        stateInfo->set_functionname(info.functionName);
        stateInfo->set_partitionby(info.partitionBy);
        stateInfo->set_statekey(info.stateKey);
        stateInfo->set_parallelism(info.parallelism);
        for (const auto& [idx, host] : info.stateHost) {
            stateInfo->mutable_statehost()->insert({ idx, host });
        }
    }

    auto scheduledOperatorsMap =
      stateAwareScheduler->getScheduledOperatorsMap();

    faabric::util::serializeScheduledOperatorMap(req, scheduledOperatorsMap);

    long startTime = faabric::util::getGlobalClock().epochMillis();

    // If not initialize, workers has to migration state and messages.
    if (!initialize) {
        std::map<std::string, std::set<std::string>> transDestinationMap;
        std::map<std::string, std::set<std::string>> transSourceMap;

        for (const auto& [userFunc, preState] : preOperatorsMap) {
            auto it = scheduledOperatorsMap.find(userFunc);
            if (it == scheduledOperatorsMap.end()) {
                SPDLOG_ERROR(
                  "Scheduled operator map does not contain user function {}",
                  userFunc);
                throw std::runtime_error(
                  "Scheduled operator map does not contain user function");
            }

            const auto& currentHosts = it->second.weightDist;
            for (const auto& [oldHost, _] : preState.weightDist) {
                for (const auto& [curHost, _] : currentHosts) {
                    transDestinationMap[oldHost].insert(curHost);
                    transSourceMap[curHost].insert(oldHost);
                }
            }
        }

        logMigrationMap("Migration Destination Map", transDestinationMap, "->");
        logMigrationMap("Migration Source Map", transSourceMap, "<-");

        fillProtoMap(req->mutable_transdestinationmap(), transDestinationMap);
        fillProtoMap(req->mutable_transsourcemap(), transSourceMap);
    }

    state.applicationMetrics->setVersion(curVersion);

    req->set_migrationversion(curVersion);
    req->set_is_initialize(initialize);

    SPDLOG_INFO("Planner begin sending states");
    std::vector<std::thread> threads;
    for (const auto& [ip, host] : state.hostMap) {
        threads.emplace_back([&, ip]() {
            try {
                SPDLOG_DEBUG("Planner sync state info to host {}", ip);
                faabric::scheduler::getFunctionCallClient(ip)->syncStateInfo(
                  req);
            } catch (const std::exception& e) {
                SPDLOG_ERROR(
                  "Failed to sync state info to host {}: {}", ip, e.what());
                throw e;
            }
        });
    }
    for (auto& t : threads) {
        if (t.joinable()) {
            t.join();
        }
    }

    long endTime = faabric::util::getGlobalClock().epochMillis();
    int migrationDuration = endTime - startTime;

    std::set<std::string> oldHostSet, newHostSet;
    for (const auto& [func, op] : preOperatorsMap) {
        for (const auto& [host, _] : op.weightDist)
            oldHostSet.insert(host);
    }
    for (const auto& [func, op] : scheduledOperatorsMap) {
        for (const auto& [host, _] : op.weightDist)
            newHostSet.insert(host);
    }
    migrationDurations[curVersion] = { migrationDuration,
                                       (int)oldHostSet.size(),
                                       (int)newHostSet.size() };

    SPDLOG_INFO("Planner distributes state info finished");
}

void Planner::doRescheduleMessages()
{
    SPDLOG_DEBUG("Planner starts reschedule messages");

    // Gather all messages from scheduledMsgsMap into a vector.
    std::vector<std::unique_ptr<faabric::Message>> messages;
    for (auto& [host, msgList] : state.scheduledMsgsMap) {
        // Move each message into our vector.
        for (auto& msg : msgList) {
            messages.push_back(std::move(msg));
        }
        msgList.clear();
    }
    state.scheduledMsgsMap.clear();

    if (messages.empty()) {
        SPDLOG_DEBUG("No messages to reschedule.");
        return;
    }

    // Recalculate scheduling decisions using the updated batch scheduler host
    // map.
    auto newHosts =
      stateAwareScheduler->scheduleMessagesBatch(state.activeHosts, messages);

    // Re-enqueue the messages: this sets a new planner pop time and assigns
    // them to the new hosts.
    doEnqueueSchedMessages(newHosts, std::move(messages));
}

bool Planner::resetParameter(
  const faabric::planner::ResetStreamParameterRequest& req)
{
    const std::string& key = req.parameter();
    const int32_t value = req.value();

    static const std::unordered_set<std::string> plannerParams = {
        "is_outputting",
        "is_warmup",
        "auto_scaling_enabled",
        "num_hosts_scheduled",
        "runtime_reconfig_period",
        "max_inflight_reqs",
        "max_waiting_queue_size",
        "worker_queue_num_threshold",
        "worker_queue_time_threshold",
        "planner_queue_size_threshold",
        "planner_queue_age_threshold",
        "coeff_c",
        "coeff_a",
        "coeff_b",
        "coeff_g",
        "coeff_param_fix",
        "rls_lambda",
        "rls_p",
        "rls_w_ema",
        "rls_y_norm",
        "rls_chain_norm",
        "rls_remote_norm",
        "rls_host_norm",
        "periodic_reschedule_interval",
        "input_rate_stability_window",
        "input_rate_deviation",
        "worker_load_headroom",
        "capacity_search_tol",
        "soft_affinity_ws",
        "soft_affinity_wb",
        "nl_min_fit_samples",
    };

    faabric::util::FullLock lock(plannerMx);

    if (plannerParams.contains(key)) {
        SPDLOG_INFO("Planner reset parameter {} to {}", key, value);
        if (key == "is_outputting") {
            isOutputting = value == 1;
            SPDLOG_INFO("Planner outputting mode: {}",
                        isOutputting ? "ON" : "OFF");
        } else if (key == "is_warmup") {
            isWarmup.store(value == 1);
            SPDLOG_INFO("Planner warmup mode: {}",
                        isWarmup.load() ? "ON" : "OFF");
        } else if (key == "auto_scaling_enabled") {
            autoScalingEnabled.store(value == 1);
            SPDLOG_INFO("Planner auto-scaling: {}",
                        autoScalingEnabled.load() ? "ON" : "OFF");
        } else if (key == "num_hosts_scheduled") {
            schedHostNum = value;
        } else if (key == "runtime_reconfig_period") {
            runtimeReconfigPeriod = value;
        } else if (key == "max_inflight_reqs") {
            maxInflightApps.store(value);
        } else if (key == "max_waiting_queue_size") {
            maxWaitingQueueSize = value;
        } else if (key == "coeff_param_fix") {
            if (state.applicationMetrics) {
                state.applicationMetrics->setCoeff(key,
                                                   static_cast<double>(value));
            }
        } else if (key == "coeff_c" || key == "coeff_a" || key == "coeff_b" ||
                   key == "coeff_g") {
            if (state.applicationMetrics) {
                state.applicationMetrics->setCoeff(key, req.value_double());
            }
        } else if (key == "rls_lambda" || key == "rls_p" ||
                   key == "rls_w_ema" || key == "rls_y_norm" ||
                   key == "rls_chain_norm" || key == "rls_remote_norm" ||
                   key == "rls_host_norm") {
            if (state.applicationMetrics) {
                state.applicationMetrics->setRlsParam(key, req.value_double());
            }
        } else if (key == "periodic_reschedule_interval") {
            periodicRescheduleIntervalMs = value;
        } else if (key == "input_rate_stability_window") {
            inputRateStabilityWindowMs = value;
        } else if (key == "input_rate_deviation") {
            inputRateDeviationRatio = req.value_double();
        } else if (key == "worker_load_headroom") {
            workerLoadHeadroom = req.value_double();
            if (stateAwareScheduler) {
                stateAwareScheduler->setCapacityHeadroom(workerLoadHeadroom);
            }
            SPDLOG_INFO("Planner worker load headroom set to {:.3f}",
                        workerLoadHeadroom);
        } else if (key == "capacity_search_tol") {
            capacitySearchTol = req.value_double();
            if (stateAwareScheduler) {
                stateAwareScheduler->setCapacitySearchTol(capacitySearchTol);
            }
            SPDLOG_INFO("Planner capacity search tolerance set to {:.1f} req/s",
                        capacitySearchTol);
        } else if (key == "soft_affinity_ws" || key == "soft_affinity_wb") {
            if (key == "soft_affinity_ws") {
                softAffinityWs = req.value_double();
            } else {
                softAffinityWb = req.value_double();
            }
            if (stateAwareScheduler) {
                stateAwareScheduler->setSoftAffinityWeights(softAffinityWs,
                                                            softAffinityWb);
            }
            SPDLOG_INFO(
              "Planner soft affinity weights set to ws={:.2f} wb={:.2f}",
              softAffinityWs,
              softAffinityWb);
        } else if (key == "nl_min_fit_samples") {
            nlMinFitSamples = value;
            if (stateAwareScheduler) {
                stateAwareScheduler->setNlMinFitSamples(nlMinFitSamples);
            }
            SPDLOG_INFO("Planner non-linear min fit samples set to {}",
                        nlMinFitSamples);
        } else if (state.applicationMetrics &&
                   state.applicationMetrics->setThreshold(key, value)) {
            // threshold keys handled inside ApplicationMetrics
        }
        return true;
    }
    if (key == "persistent_lock") {
        bool lockVal = (value == 1);
        faabric::state::getGlobalState().resetPersistentLockState();
        faabric::state::getGlobalState().persistentLock = lockVal;
        SPDLOG_INFO("Persistent lock: {}", lockVal ? "ON" : "OFF");
    }
    if (key == "schedule_mode") {
        // Schedule Mode 0: Decentralized Scheduler with Binpack.
        // Schedule Mode 3: FaaSFlow Scheduler.
        // Scheduler Mode 5: LcSched.
        // Scheduler Mode 7: Centralized Scheduler With FaaSFlow
        SPDLOG_INFO("Planner reset schedule mode to {}", value);
        stateAwareScheduler->setScheduleMode(value);
        scheduleMode = value;
    }
    if (key == "dispatch_period") {
        dispatchPeriod = value;
    }
    if (key == "runtime_reconfig") {
        stateAwareScheduler->setRuntimeReconfig(value == 1);
    }
    if (key == "alpha") {
        double newAlpha = value / 1000.0;
        SPDLOG_INFO("Alpha is set to {}", newAlpha);
        stateAwareScheduler->setAlpha(newAlpha);
    }

    // Forward to worker hosts
    auto reqPtr =
      std::make_shared<faabric::planner::ResetStreamParameterRequest>(req);
    auto availableHosts = getAvailableHosts(true);
    for (const auto& host : availableHosts) {
        SPDLOG_INFO(
          "Planner reset {} parameter {} to {}", host->ip(), key, value);
        faabric::scheduler::getFunctionCallClient(host->ip())
          ->resetParameter(reqPtr);
    }

    SPDLOG_DEBUG("Planner reschedules messages done");
    return true;
}

void Planner::rescheduleApp(int rescheduleMode, int hostNum)
{
    SPDLOG_INFO("Planner reschedules application");
    lastPeriodicRescheduleMs = faabric::util::getGlobalClock().epochMillis();
    // rescheduleMode == 0 means reschedule immediately.
    // rescheduleMode == 1 means wait until all running messages are finished.

    faabric::util::FullLock lock(plannerMx);

    if (hostNum > 0 && hostNum <= (int)state.hostMap.size()) {
        state.activeHosts = faabric::util::getFirstNElements(
          convertToHostMap(state.hostMap), hostNum);
        schedHostNum = hostNum;
    }

    // If reschedule mode is 1, we want to wait until no inflight requests and
    // clear the state. It happens during the pre-warm phase.
    if (rescheduleMode == 1) {
        // Wait until all in-flight apps are finished
        while (getInFlightAppsSize() > 0) {
            SPDLOG_DEBUG(
              "Waiting for in-flight apps to finish before rescheduling");
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
        }
    }

    // Update the application node processed tuples.
    // Fetch the workload from metrics at first.
    auto operatorWorkloadMap = state.applicationMetrics->getOptWorkloads();
    auto edgeWeightMap = state.applicationMetrics->getEdgeWeightMap();

    auto preOperatorsMap = stateAwareScheduler->getScheduledOperatorsMap();
    // Update the processed tuples.
    stateAwareScheduler->updateApp(operatorWorkloadMap, edgeWeightMap);
    stateAwareScheduler->rescheduleApp(state.activeHosts,
                                       state.applicationMetrics.get());

    // For adaptive mode (3) and open-ended Binpack (0) the scheduler may use
    // fewer workers than activeHosts. Rebuild activeHosts from the IPs that are
    // actually in the schedule, so schedHostNum reflects the worker count the
    // packer chose (the "predicted N" is now an output of the packer, not a
    // separate model).
    if (scheduleMode == 3 || scheduleMode == 0) {
        std::set<std::string> usedIps;
        for (const auto& [func, op] :
             stateAwareScheduler->getScheduledOperatorsMap()) {
            for (const auto& [ip, _] : op.weightDist)
                usedIps.insert(ip);
        }
        faabric::batch_scheduler::HostMap newActiveHosts;
        for (const auto& [ip, host] : state.activeHosts) {
            if (usedIps.count(ip))
                newActiveHosts[ip] = host;
        }
        if (!newActiveHosts.empty()) {
            state.activeHosts = newActiveHosts;
            schedHostNum = static_cast<int>(newActiveHosts.size());
            SPDLOG_INFO("Adaptive: activeHosts updated to {} workers",
                        schedHostNum);
        }
    }

    SPDLOG_INFO("Planner reschedules application done");

    // Reschedule the states and messages in queue
    // stateAwareScheduler->updateReqDist();
    int curVersion = migrationVersion++;
    bool initialize = (rescheduleMode == 1) ? true : false;
    doDistributeStatesInfo(curVersion, preOperatorsMap, initialize);
    doRescheduleMessages();

    // If reschedule mode is pre-warm, clear the statistics
    if (rescheduleMode == 1) {
        state.applicationMetrics->reset();
    }
}

void Planner::setPersistentState(const faabric::planner::MapMessage& mapMsg)
{
    faabric::util::FullLock lock(plannerMx);
    auto availableHosts = getAvailableHosts(true);
    auto mapMsgShared = std::make_shared<faabric::planner::MapMessage>(mapMsg);

    for (const auto& host : availableHosts) {
        SPDLOG_INFO("Planner set persistent state to {}", host->ip());
        faabric::scheduler::getFunctionCallClient(host->ip())
          ->setPersistentState(mapMsgShared);
    }
}

std::string Planner::getPersistentStateFromWorker(const std::string& key)
{
    return faabric::state::getGlobalState().readPersistentState(key);
}

void Planner::setPersistentStateFromWorker(
  const faabric::planner::MapMessage& mapMsg)
{
    auto& localState = faabric::state::getGlobalState();
    for (const auto& [k, v] : mapMsg.payload()) {
        std::string key = k;
        std::string val = v;
        localState.writePersistentState(key, val);
    }
}

std::string Planner::outputResult()
{
    faabric::util::FullLock reqStatusLock(state.reqStatusMx);
    isOutputting = true;
    // We have to unlock, since the output operation may take a long time
    auto doc = state.applicationMetrics->getMetrics();
    reqStatusLock.unlock();

    SPDLOG_INFO("Planner fetch worker statistics");

    auto& alloc = doc.GetAllocator();
    rapidjson::Value workerStatsObj(rapidjson::kObjectType);
    rapidjson::Value maxReplicaObj(rapidjson::kObjectType);
    rapidjson::Value migrationHistoryObj(rapidjson::kObjectType);
    rapidjson::Value migrationDurationsObj(rapidjson::kObjectType);
    // rapidjson::Value allWorkerMetricsObj(rapidjson::kObjectType);

    for (const auto& [ip, host] : state.hostMap) {
        SPDLOG_DEBUG("Planner fetch stats from host {}", ip);
        auto stats =
          faabric::scheduler::getFunctionCallClient(ip)->getWorkerStats();

        SPDLOG_DEBUG("Planner parse replica stats from host {}", ip);
        // ===== instance replicas =====
        rapidjson::Value replicasArr(rapidjson::kArrayType);
        for (const auto& inst : stats->instancereplicas()) {
            rapidjson::Value instObj(rapidjson::kObjectType);
            instObj.AddMember(
              "instanceName",
              rapidjson::Value(inst.instancename().c_str(), alloc).Move(),
              alloc);
            instObj.AddMember("replicas", inst.replicas(), alloc);
            replicasArr.PushBack(instObj, alloc);
        }
        maxReplicaObj.AddMember(
          rapidjson::Value(ip.c_str(), alloc).Move(), replicasArr, alloc);

        SPDLOG_DEBUG("Planner parse migration history from host {}", ip);
        // ===== migration history =====
        rapidjson::Value migrationArr(rapidjson::kArrayType);
        for (const auto& [version, count] : stats->migrationhistory()) {
            SPDLOG_DEBUG(
              "Migration version {} with duration {}", version, count);
            rapidjson::Value migObj(rapidjson::kObjectType);
            migObj.AddMember("version", version, alloc);
            migObj.AddMember("duration", count, alloc);
            migrationArr.PushBack(migObj, alloc);
        }
        // Add to migrationHistoryObj under the IP key
        migrationHistoryObj.AddMember(
          rapidjson::Value(ip.c_str(), alloc).Move(), migrationArr, alloc);
    }

    for (const auto& [version, record] : migrationDurations) {
        std::string versionStr = std::to_string(version);
        rapidjson::Value k(versionStr.c_str(), alloc);
        rapidjson::Value recordObj(rapidjson::kObjectType);
        recordObj.AddMember("duration", record.duration, alloc);
        recordObj.AddMember("oldHosts", record.oldHosts, alloc);
        recordObj.AddMember("newHosts", record.newHosts, alloc);
        migrationDurationsObj.AddMember(k, recordObj, alloc);
    }

    doc.AddMember("workerStats", workerStatsObj, alloc);
    doc.AddMember("maxReplicaInfo", maxReplicaObj, alloc);
    doc.AddMember("migrationHistory", migrationHistoryObj, alloc);
    doc.AddMember("migrationDurations", migrationDurationsObj, alloc);
    // doc.AddMember("allWorkerMetrics", allWorkerMetricsObj, alloc);

    isOutputting = false;
    // Write out the JSON document to a string.
    rapidjson::StringBuffer buffer;
    rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);
    doc.Accept(writer);
    return buffer.GetString();
}

std::map<std::string, std::unique_ptr<faabric::WorkerStats>>
Planner::fetchWorkerStatsAsync(const std::vector<std::string>& targetIps)
{
    std::map<std::string, std::unique_ptr<faabric::WorkerStats>> results;
    std::mutex resultsMutex;
    std::vector<std::future<void>> futures;
    for (const auto& ip : targetIps) {
        try {
            futures.emplace_back(
              std::async(std::launch::async, [&results, &resultsMutex, ip]() {
                  try {
                      //   SPDLOG_DEBUG("Fetching runtime stats from host {}",
                      //   ip);
                      auto stats = faabric::scheduler::getFunctionCallClient(ip)
                                     ->getWorkerRuntimeStats();
                      std::lock_guard<std::mutex> lock(resultsMutex);
                      results[ip] = std::move(stats);
                  } catch (const std::exception& e) {
                      SPDLOG_ERROR(
                        "Failed to fetch runtime stats from host {}: {}",
                        ip,
                        e.what());
                  } catch (...) {
                      SPDLOG_ERROR(
                        "Unknown exception fetching runtime stats from host {}",
                        ip);
                  }
              }));
        } catch (const std::exception& e) {
            SPDLOG_ERROR(
              "Failed to spawn async thread for {}: {}", ip, e.what());
        }
    }
    // SPDLOG_DEBUG("All async tasks for fetching stats have been launched");
    for (auto& fut : futures) {
        try {
            if (fut.valid()) {
                fut.get();
            }
        } catch (const std::exception& e) {
            SPDLOG_ERROR("Async task threw an exception during get(): {}",
                         e.what());
        } catch (...) {
            SPDLOG_ERROR("Unknown exception during async task get()");
        }
    }
    return results;
}

// This is a function updating runtime statistics periodically.
void Planner::updateRuntimeStats()
{
    while (!stopThreadTimer) {
        std::this_thread::sleep_for(
          std::chrono::milliseconds(runtimeReconfigPeriod));

        if (stopThreadTimer) {
            break;
        }

        SPDLOG_TRACE("Planner starts fetching runtime stats from workers");
        // If no hosts or registered applications, skip fetching stats.
        std::vector<std::string> targetIps;
        int maxHostNum = 0;
        {
            faabric::util::SharedLock lock(plannerMx);
            if (state.hostMap.empty()) {
                continue;
            }
            maxHostNum = (int)state.hostMap.size();
            for (const auto& [ip, hostInfo] : state.activeHosts) {
                targetIps.push_back(ip);
            }
        }
        if (targetIps.empty()) {
            continue;
        }

        // Scope reconfigMx to fetch + process only.
        // Must NOT hold reconfigMx when acquiring plannerMx (scaling decision),
        // because flushExecutors/flushSchedulingState acquire plannerMx →
        // reconfigMx in that order — the opposite would deadlock.

        faabric::util::FullLock rflock(reconfigMx);
        auto results = fetchWorkerStatsAsync(targetIps);

        if (state.applicationMetrics) {
            state.applicationMetrics->recordWorkerMetrics(results);
        }
        rflock.unlock();

        // Capture planner-level waiting queue snapshot.
        if (state.applicationMetrics) {
            int qSize = (int)waitingMessageQueue.size();
            int64_t qAgeMicros = 0;
            auto frontMsg = waitingMessageQueue.peek_front();
            if (frontMsg) {
                const auto& metrics = (*frontMsg)->metricrecorder();
                auto it = metrics.find(PLANNER_ENQUEUE_TIME_KEY);
                if (it != metrics.end() && it->second > 0) {
                    int64_t nowMicros =
                      faabric::util::getGlobalClock().epochMicros();
                    qAgeMicros = nowMicros - it->second;
                    if (qAgeMicros < 0)
                        qAgeMicros = 0;
                }
            }
            state.applicationMetrics->recordQueueSnapshot(qSize, qAgeMicros);
        }

        // SPDLOG_DEBUG("Planner finished updating runtime stats");

        if (!state.applicationMetrics || isWarmup.load() ||
            !autoScalingEnabled.load()) {
            continue;
        }

        auto signals = state.applicationMetrics->getScalingSignals(
          scalingDecisionPeriodMs / 1000);

        // Compute chainedMultiplier from Application DAG:
        // totalLoad = inputRate × (totalOperatorCount / inputOperatorCount)
        if (stateAwareScheduler) {
            const auto& inputNodeNames =
              stateAwareScheduler->getInputNodeNames();
            if (!inputNodeNames.empty()) {
                auto workloads = state.applicationMetrics->getOptWorkloads();
                long inputCount = 0, totalCount = 0;
                for (const auto& [name, count] : workloads)
                    totalCount += count;
                for (const auto& inputName : inputNodeNames)
                    if (workloads.count(inputName))
                        inputCount += workloads.at(inputName);
                if (inputCount > 0)
                    signals.chainedMultiplier =
                      static_cast<double>(totalCount - inputCount) / inputCount;
            }
        }

        evaluateReschedule(signals, maxHostNum);
    }
}

int Planner::computeTargetHostNum(
  const faabric::planner::ApplicationMetrics::ScalingSignals& signals,
  int currentHostNum,
  int maxHostNum)
{
    // Require a valid C estimate before making any scaling decision.
    if (signals.coeffC <= 0.0 || signals.avgExecTime <= 0.0)
        return currentHostNum;

    double alpha = std::max(0.0, signals.alpha);
    double beta = std::max(0.0, signals.beta);
    double gamma = std::max(0.0, signals.gamma);

    // totalLoad = inputRate + inputRate × chainedMultiplier
    // where chainedMultiplier = chainedOperatorCount / inputOperatorCount
    // (e.g., 1.0 for a two-operator pipeline: A→B, each input creates one
    // chain)
    double totalLoad =
      signals.avgInputRate * signals.chainedMultiplier + signals.avgInputRate;
    if (totalLoad <= 0.0)
        return currentHostNum;

    double R = signals.avgChainedRatio;

    // --- Per-worker feasibility search (preferred) ---------------------------
    // The Binpack layout spreads each operator unevenly across workers, so the
    // observed local/remote ratios and process load differ per worker. Rather
    // than collapsing to one average capacity, simulate the Binpack placement
    // at each candidate N and require *every* worker's predicted CPU load to
    // stay within budget C (with headroom). This captures the load imbalance
    // the aggregate model below cannot. Pick the smallest feasible N.
    if (stateAwareScheduler) {
        const double budget = signals.coeffC * workerLoadHeadroom;
        int hi = std::max(1, maxHostNum);
        bool predictionUsable = false;
        for (int n = 1; n <= hi; ++n) {
            auto loads = stateAwareScheduler->predictBinpackWorkerLoads(
              n, totalLoad, signals.avgExecTime, alpha, beta, gamma, R);
            if (loads.empty()) {
                // Prediction not possible (e.g. before the first reschedule);
                // abandon the per-worker path and fall back to the aggregate
                // model below.
                predictionUsable = false;
                break;
            }
            predictionUsable = true;
            double maxLoad = 0.0;
            for (double l : loads)
                maxLoad = std::max(maxLoad, l);
            if (maxLoad <= budget) {
                SPDLOG_DEBUG(
                  "computeTargetHostNum(per-worker): C={:.0f}us/s, "
                  "budget={:.0f}us/s, totalLoad={:.1f}, R={:.3f} -> targetN={} "
                  "(maxLoad={:.0f}us/s)",
                  signals.coeffC,
                  budget,
                  totalLoad,
                  R,
                  n,
                  maxLoad);
                return n;
            }
        }
        // No N within [1, maxHostNum] keeps every worker under budget: the
        // cluster is saturated, so request the maximum.
        if (predictionUsable) {
            SPDLOG_DEBUG("computeTargetHostNum(per-worker): saturated, "
                         "totalLoad={:.1f} -> targetN={} (maxHostNum)",
                         totalLoad,
                         maxHostNum);
            return std::max(1, maxHostNum);
        }
    }

    // --- Aggregate fallback --------------------------------------------------
    // The capacity model is:
    //   capPerWorker = C / (t_e + alpha × localRatio + beta × remoteRatio)
    // The observed local/remote chained ratios are cluster-size-dependent: as
    // we add workers the Binpack scheduler spreads each operator over more
    // hosts, so chained calls increasingly cross host boundaries (remoteRatio
    // grows, localRatio shrinks). Reusing the *observed* ratios — measured at
    // the current host count — therefore over-estimates capacity at a larger N.
    //
    // Instead we keep the total chained ratio R (an application property,
    // R = avgChainedRatio) fixed and re-derive the local/remote split for each
    // candidate N by simulating the deterministic Binpack split
    // (predictBinpackLocalShare). Because the split depends on N and N depends
    // on the split, iterate to a fixed point.
    // budget = C minus the fixed per-worker host-fanout overhead (gamma per
    // distinct remote dest host). The remaining budget is shared by the
    // throughput-proportional process + chained-call cost.
    auto capacityFor = [&](
                         double budget, double localRatio, double remoteRatio) {
        return budget /
               (signals.avgExecTime + alpha * localRatio + beta * remoteRatio);
    };

    int targetN = std::max(1, std::min(currentHostNum, maxHostNum));
    double localRatio = signals.avgLocalChainedRatio;
    double remoteRatio = signals.avgRemoteChainedRatio;
    double localShare = -1.0;
    double nHosts = 0.0;
    const int maxIter = 8;
    for (int iter = 0; iter < maxIter; ++iter) {
        // Predict the local/remote chained split at the candidate host count.
        localShare = stateAwareScheduler
                       ? stateAwareScheduler->predictBinpackLocalShare(targetN)
                       : -1.0;
        if (localShare >= 0.0 && R > 0.0) {
            localRatio = R * localShare;
            remoteRatio = R * (1.0 - localShare);
        }
        // else: fall back to the observed ratios (best available estimate).

        // Fixed per-worker host overhead at this candidate N. A worker can fan
        // out to at most (targetN - 1) other hosts; use the observed distinct
        // dest-host count as the estimate, capped by that physical bound. (No
        // placement is available on this fallback path, so this is the best
        // estimate we have.)
        nHosts = std::min(static_cast<double>(std::max(0, targetN - 1)),
                          std::max(0.0, signals.avgNumDestHosts));
        double budget = signals.coeffC - gamma * nHosts;
        if (budget <= 0.0) {
            // Host-fanout overhead alone exceeds the CPU budget: the cluster
            // cannot keep up at this size, request the maximum.
            return std::max(1, maxHostNum);
        }

        double capPerWorker = capacityFor(budget, localRatio, remoteRatio);
        if (capPerWorker <= 0.0)
            return currentHostNum;

        int newN = std::max(
          1,
          std::min(static_cast<int>(std::ceil(totalLoad / capPerWorker)),
                   maxHostNum));
        if (newN == targetN)
            break;
        targetN = newN;
    }

    SPDLOG_DEBUG(
      "computeTargetHostNum: C={:.0f}us/s, alpha={:.3f}, "
      "beta={:.3f}, gamma={:.1f}, nHosts={:.1f}, execTime={:.1f}us, "
      "R={:.3f}, localShare={:.3f}, localRatio={:.3f}, "
      "remoteRatio={:.3f}, inputRate={:.1f}, chainedMultiplier={:.2f}, "
      "totalLoad={:.1f} -> targetN={}",
      signals.coeffC,
      signals.alpha,
      signals.beta,
      gamma,
      nHosts,
      signals.avgExecTime,
      R,
      localShare,
      localRatio,
      remoteRatio,
      signals.avgInputRate,
      signals.chainedMultiplier,
      totalLoad,
      targetN);

    return targetN;
}

bool Planner::evaluateReschedule(
  const faabric::planner::ApplicationMetrics::ScalingSignals& signals,
  int maxHostNum)
{
    /***
     Detect whether reschedule should be triggered.
     The conditions to trigger reschedule (Logical OR):
     1. Last reschedule exceeds the periodic reschedule interval.
     2. The input rate stably changes for some seconds.
     Only compute targetN and perform reschedule if a condition is met.
     ***/
    long nowMs = faabric::util::getGlobalClock().epochMillis();
    double currentRate = signals.avgInputRate;

    // If stableInputRate is not set, initialize it with the current rate.
    if (stableInputRate <= 0.0 && currentRate > 0.0)
        stableInputRate = currentRate;

    bool inputRateTriggered = false;
    if (stableInputRate > 0.0 && currentRate > 0.0) {
        double relChange =
          std::abs(currentRate - stableInputRate) / stableInputRate;

        if (relChange <= inputRateDeviationRatio) {
            // Rate returned to baseline — cancel any pending detection.
            inputRateChangeDetectedMs = 0;
        } else if (inputRateChangeDetectedMs == 0) {
            // First deviation observed — start the stability window.
            inputRateChangeDetectedMs = nowMs;
            pendingInputRate = currentRate;
            SPDLOG_DEBUG(
              "Input rate change detected: {:.1f} -> {:.1f} ({:.1f}%)",
              stableInputRate,
              currentRate,
              relChange * 100.0);
        } else {
            // Already tracking — check whether the new rate has stabilized.
            double pendingDev =
              pendingInputRate > 0.0
                ? std::abs(currentRate - pendingInputRate) / pendingInputRate
                : 1.0;
            bool stableEnough = pendingDev <= pendingDevToleranceRatio;
            bool windowElapsed =
              nowMs - inputRateChangeDetectedMs >= inputRateStabilityWindowMs;
            if (!stableEnough) {
                // Rate still drifting — restart the stability window.
                inputRateChangeDetectedMs = nowMs;
                pendingInputRate = currentRate;
            } else if (windowElapsed) {
                inputRateTriggered = true;
            }
        }
    }

    bool periodicTriggered =
      (nowMs - lastPeriodicRescheduleMs >= periodicRescheduleIntervalMs);

    if (!inputRateTriggered && !periodicTriggered)
        return false;

    if (scheduleMode == 0) {
        // Open-ended Binpack: do NOT predict the worker count. Hand the full
        // host set to the capacity-aware packer (groupNodesCapacity), which
        // opens workers on demand under the per-worker capacity budget and lets
        // the actual N fall out of the placement. rescheduleApp() then rebuilds
        // activeHosts/schedHostNum from the workers the packer actually used.
        lastPeriodicRescheduleMs = nowMs;
        stableInputRate = currentRate;
        inputRateChangeDetectedMs = 0;
        rescheduleApp(0, maxHostNum);
    } else if (scheduleMode == 3) {
        int targetN = state.applicationMetrics
                        ? state.applicationMetrics->computeAdaptiveHostCount(
                            signals.avgInputRate, maxHostNum)
                        : maxHostNum;
        lastPeriodicRescheduleMs = nowMs;
        if (targetN <= 0 || targetN == schedHostNum)
            return false;

        rescheduleApp(0, maxHostNum);
        stableInputRate = currentRate;
        inputRateChangeDetectedMs = 0;
    } else if (scheduleMode == 4) {
        // Non-linear AutoTuning: v1 does not drive host scaling, so keep the
        // current active host set (hostNum = 0). The reschedule itself is the
        // continuous-fitting loop: it snapshots the overhead metrics, refits
        // r(p) and retunes per-operator parallelism + placement.
        lastPeriodicRescheduleMs = nowMs;
        stableInputRate = currentRate;
        inputRateChangeDetectedMs = 0;
        rescheduleApp(0, 0);
    }

    return true;
}

int Planner::predictHostNum(double inputRate)
{
    faabric::util::SharedLock lock(plannerMx);
    if (!state.applicationMetrics) {
        return schedHostNum;
    }
    int maxHostNum = (int)state.hostMap.size();
    if (maxHostNum == 0) {
        return 0;
    }

    if (scheduleMode == 3) {
        return state.applicationMetrics->computeAdaptiveHostCount(inputRate,
                                                                  maxHostNum);
    }

    if (scheduleMode == 4) {
        // Non-linear model prediction: instance demand from the fitted r(p)
        // curves, folded to a host count. STRICTLY READ-ONLY — this only
        // computes and returns; schedHostNum / activeHosts are not touched.
        if (!stateAwareScheduler) {
            return schedHostNum;
        }
        auto signals = state.applicationMetrics->getScalingSignals(
          scalingDecisionPeriodMs / 1000);
        long totalSlots = 0;
        for (const auto& [ip, host] : state.hostMap) {
            totalSlots += std::max(1, static_cast<int>(host->slots()));
        }
        if (totalSlots <= 0) {
            return schedHostNum;
        }
        double avgSlots = static_cast<double>(totalSlots) /
                          static_cast<double>(state.hostMap.size());
        long totalInstances =
          stateAwareScheduler->predictNonlinearInstanceTotal(
            inputRate, signals, avgSlots, totalSlots);
        if (totalInstances <= 0) {
            // Cold estimator / no application: no better answer than the
            // currently scheduled host count.
            SPDLOG_INFO("predictHostNum(mode 4): model cold, returning "
                        "current schedHostNum={}",
                        schedHostNum);
            return schedHostNum;
        }
        int hosts = static_cast<int>(
          std::ceil(static_cast<double>(totalInstances) / avgSlots));
        hosts = std::min(std::max(hosts, 1), maxHostNum);
        SPDLOG_INFO("predictHostNum(mode 4): rate={:.1f} -> {} instances "
                    "-> {} hosts (avgSlots={:.1f}, max={})",
                    inputRate,
                    totalInstances,
                    hosts,
                    avgSlots,
                    maxHostNum);
        return hosts;
    }

    // scheduleMode == 0 (default): model-based prediction
    auto signals = state.applicationMetrics->getScalingSignals(
      scalingDecisionPeriodMs / 1000);
    signals.avgInputRate = inputRate;

    if (stateAwareScheduler) {
        const auto& inputNodeNames = stateAwareScheduler->getInputNodeNames();
        if (!inputNodeNames.empty()) {
            auto workloads = state.applicationMetrics->getOptWorkloads();
            long inputCount = 0, totalCount = 0;
            for (const auto& [name, count] : workloads)
                totalCount += count;
            for (const auto& inputName : inputNodeNames)
                if (workloads.count(inputName))
                    inputCount += workloads.at(inputName);
            if (inputCount > 0)
                signals.chainedMultiplier =
                  static_cast<double>(totalCount - inputCount) / inputCount;
        }
    }

    return computeTargetHostNum(signals, schedHostNum, maxHostNum);
}

Planner& getPlanner()
{
    static Planner planner;
    return planner;
}
}
