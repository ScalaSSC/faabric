#include <faabric/batch-scheduler/SchedulingDecision.h>
#include <faabric/planner/Planner.h>
#include <faabric/proto/faabric.pb.h>
#include <faabric/scheduler/FunctionCallClient.h>
#include <faabric/snapshot/SnapshotClient.h>
#include <faabric/state/FunctionStateClient.h>
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

static faabric::batch_scheduler::HostMap convertToBatchSchedHostMap(
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
    state.batchSchedHostMap.clear();
}

void Planner::flushExecutors()
{
    faabric::util::FullLock lock(plannerMx);

    if (stateAwareScheduler) {
        stateAwareScheduler->resetScheduler();
    }

    auto availableHosts = getAvailableHosts(true);
    for (const auto& host : availableHosts) {
        SPDLOG_INFO("Planner sending EXECUTOR flush to {}", host->ip());
        faabric::scheduler::getFunctionCallClient(host->ip())->sendFlush();
    }
}

void Planner::flushSchedulingState()
{
    faabric::util::FullLock lock(plannerMx);

    state.inFlightReqs.clear();
    state.appResults.clear();
    state.appResultWaiters.clear();
    state.numMigrations = 0;
    state.inFlightApps.clear();
    state.appStartTimes.clear();
    state.applicationMetrics =
      std::make_unique<ApplicationMetrics>("defaultApp", 1);

    state.batchSchedHostMap = convertToBatchSchedHostMap(state.hostMap);
    numHostsScheduled = 0;
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
        state.batchSchedHostMap = convertToBatchSchedHostMap(state.hostMap);
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
    faabric::util::SharedLock lock(plannerMx);

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
    state.batchSchedHostMap = convertToBatchSchedHostMap(state.hostMap);
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
            auto recordStartTime = state.appStartTimes[appId];
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
    auto hosts = stateAwareScheduler->scheduleMessagesBatch(
      state.batchSchedHostMap, messages);

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
    SPDLOG_DEBUG("Getting in-flight apps size: {}", state.inFlightApps.size());
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
                          std::unique_ptr<batch_scheduler::Application> app,
                          bool init)
{
    SPDLOG_INFO("Planner registers application {}", app->getName());
    faabric::util::FullLock lock(plannerMx);
    stateAwareScheduler->registerApp(std::move(app));
    distributeApp(rawReq);
    if (init) {
        if (numHostsScheduled != 0 &&
            numHostsScheduled < state.batchSchedHostMap.size()) {
            state.batchSchedHostMap = faabric::util::getFirstNElements(
              state.batchSchedHostMap, numHostsScheduled);
        }
        stateAwareScheduler->initApp(state.batchSchedHostMap);
        stateAwareScheduler->rescheduleApp(state.batchSchedHostMap);
        doDistributeStatesInfo();
        doRescheduleMessages();
    }
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

void Planner::doDistributeStatesInfo()
{
    int hostSize = state.hostMap.size();
    SPDLOG_INFO("Planner distribute state info to {} hosts", hostSize);
    // Stores the number of workers needed to migrate the state info
    migratingHostNum.store(hostSize);

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
    auto newHosts = stateAwareScheduler->scheduleMessagesBatch(
      state.batchSchedHostMap, messages);

    // Re-enqueue the messages: this sets a new planner pop time and assigns
    // them to the new hosts.
    doEnqueueSchedMessages(newHosts, std::move(messages));
}

bool Planner::resetParameter(const std::string& key,
                             const int32_t value,
                             bool plannerParameter)
{
    faabric::util::FullLock lock(plannerMx);

    // Reset the parameter of planner
    if (plannerParameter) {
        SPDLOG_INFO("Planner reset parameter {} to {}", key, value);
        if (key == "is_outputting") {
            isOutputting = value == 1;
        } else if (key == "num_hosts_scheduled") {
            numHostsScheduled = value;
        } else if (key == "runtime_reconfig") {
            runtimeReconfig = value == 1;
        }
        return true;
    }
    if (key == "schedule_mode") {
        // Schedule Mode 0: Decentralized Scheduler.
        // Schedule Mode 1: Decentralized Scheduler with Default Logic (colocate
        // stateful requests with their requried states and rountrobin for
        // stateless requests).
        // Schedule Mode 2: Centralized Scheduler.
        // Schedule Mode 3: FaaSFlow Scheduler.
        // Scheduler Mode 5: Our Method.
        SPDLOG_INFO("Planner reset schedule mode to {}", value);
        stateAwareScheduler->setScheduleMode(value);
        scheduleMode = value;
    }
    if (key == "dispatch_period") {
        dispatchPeriod = value;
    }

    // Reset the parameter of the worker hosts
    auto availableHosts = getAvailableHosts(true);
    faabric::planner::ResetStreamParameterRequest req;
    req.set_parameter(key);
    req.set_value(value);

    for (const auto& host : availableHosts) {
        SPDLOG_INFO(
          "Planner reset {} parameter {} to {}", host->ip(), key, value);
        faabric::scheduler::getFunctionCallClient(host->ip())
          ->resetParameter(
            std::make_shared<faabric::planner::ResetStreamParameterRequest>(
              req));
    }

    SPDLOG_DEBUG("Planner reschedules messages done");
    return true;
}

void Planner::rescheduleApp(int rescheduleMode)
{
    SPDLOG_INFO("Planner reschedules application");
    // If reschedule mode is 1, we want to wait until no inflight requests and
    // clear the state.
    if (rescheduleMode == 1) {
        // Wait until all in-flight apps are finished
        while (getInFlightAppsSize() > 0) {
            SPDLOG_DEBUG(
              "Waiting for in-flight apps to finish before rescheduling");
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
        }
    }

    faabric::util::FullLock lock(plannerMx);
    // Update the application node processed tuples.
    // Fetch the workload from metrics at first.
    auto operatorWorkloadMap = state.applicationMetrics->getOptWorkloads();
    auto edgeWeightMap = state.applicationMetrics->getEdgeWeightMap();

    // Update the processed tuples.
    stateAwareScheduler->updateApp(operatorWorkloadMap, edgeWeightMap);
    stateAwareScheduler->rescheduleApp(state.batchSchedHostMap);
    SPDLOG_INFO("Planner reschedules application done");

    // Reschedule the states and messages in queue
    // stateAwareScheduler->updateReqDist();
    doDistributeStatesInfo();
    doRescheduleMessages();

    if (rescheduleMode == 1) {
        // Clear the states
        state.applicationMetrics->reset();
        // Temporary don't need flush. Since we don't migrate state.
        // auto msgShared = std::make_shared<faabric::CustomRequest>();
        // msgShared->set_payload("flush_state");
        // doDistributeCustomInfo(msgShared);
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

bool Planner::migratingComplete()
{
    int oldValue = migratingHostNum.fetch_sub(1);
    if (oldValue == 1) {
        migratingHostNum.notify_all();
    } else {
        int current = migratingHostNum.load();
        while (current != 0) {
            migratingHostNum.wait(current);
            current = migratingHostNum.load();
        }
    }
    return true;
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

    for (const auto& [ip, host] : state.hostMap) {
        SPDLOG_DEBUG("Planner fetch stats from host {}", ip);
        auto stats =
          faabric::scheduler::getFunctionCallClient(ip)->getWorkerStats();

        // ===== history =====
        rapidjson::Value historyArr(rapidjson::kArrayType);
        for (const auto& rec : stats->history()) {
            rapidjson::Value recObj(rapidjson::kObjectType);
            recObj.AddMember("cpuExecutePct", rec.cpuexecutepct(), alloc);
            recObj.AddMember("cpuSchedulePct", rec.cpuschedulepct(), alloc);
            historyArr.PushBack(recObj, alloc);
        }

        // Add to workerStatsObj under the IP key
        workerStatsObj.AddMember(
          rapidjson::Value(ip.c_str(), alloc).Move(), historyArr, alloc);

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
    }

    doc.AddMember("workerStats", workerStatsObj, alloc);
    doc.AddMember("maxReplicaInfo", maxReplicaObj, alloc);

    isOutputting = false;
    // Write out the JSON document to a string.
    rapidjson::StringBuffer buffer;
    rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);
    doc.Accept(writer);
    return buffer.GetString();
}

void Planner::updateRuntimeStats()
{
    // It doesn't need to be thread-safe, since all the stats in each worker
    // are thread-safe. Map to accumulate the runtime stats from each host.
    std::map<std::string, std::unique_ptr<faabric::RuntimeStatsResult>> results;
    std::mutex resultsMutex; // Protects access to results.
    faabric::RuntimeStatsUpdateRequest request;

    // We fetch the runtime stats periodically. Each iteration sends the stats
    // in the last iteration and feteches the new stats.
    while (!stopThreadTimer) {
        // Sleep for a while to batch the scheduled requests
        std::this_thread::sleep_for(
          std::chrono::milliseconds(runtimeStatsUpdatePeriod));

        if (scheduleMode != 0 && scheduleMode != 5 && scheduleMode != 6) {
            continue; // Only run in decentralized scheduler mode
        }

        if (!runtimeReconfig) {
            continue;
        }

        std::vector<std::future<void>> futures;
        // Iterate over all hosts.
        for (const auto& [ip, hostInfo] : state.batchSchedHostMap) {
            // Launch an asynchronous task for each host.
            futures.emplace_back(std::async(
              std::launch::async, [&results, &resultsMutex, &request, ip]() {
                  // Fetch the runtime stats for the host using its IP.
                  auto stats = faabric::scheduler::getFunctionCallClient(ip)
                                 ->getRuntimeStats(request);

                  // Lock the results map before writing.
                  std::lock_guard<std::mutex> lock(resultsMutex);
                  results[ip] = std::move(stats);
              }));
        }

        // Wait for all the async tasks to complete.
        for (auto& fut : futures) {
            fut.get();
        }

        // Update the request with the results.
        request.clear_collectedstats();
        for (const auto& [ip, stats] : results) {
            if (!stats) {
                continue;
            }
            auto* newStats = request.add_collectedstats();
            newStats->CopyFrom(*stats);
        }

        int totalCount = 0;
        auto localStats = runtimeStats.getAllStats();
        std::map<std::string, std::map<std::string, int>> localChainedMap;
        for (const auto& [instName, stats] : localStats) {
            localChainedMap[instName] = stats.chainedCallStats;
            totalCount += stats.chainedCallStats.size();
        }
        if (totalCount > 0) {
            stateAwareScheduler->runtimeDistTune(localChainedMap);
        }

        results.clear();
    }
}

Planner& getPlanner()
{
    static Planner planner;
    return planner;
}
}
