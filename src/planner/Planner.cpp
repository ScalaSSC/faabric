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

#include <fstream>
#include <map>
#include <memory>
#include <rapidjson/document.h>
#include <rapidjson/prettywriter.h>
#include <rapidjson/stringbuffer.h>
#include <rapidjson/writer.h>
#include <string>

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
}

Planner::~Planner()
{
    // Stop the batch timer thread
    stopThreadTimer = true;
    if (dequeueScheduledMsgsThread.joinable()) {
        dequeueScheduledMsgsThread.join();
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

    flushHosts();

    faabric::util::FullLock lock(plannerMx);

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
    for (const auto& [ip, host] : state.hostMap) {
        host->set_hostsync(true);
    }
}

void Planner::flushExecutors()
{
    // Flush Aware Scheduler State
    // If preload the parallelism desision, we change the parallelism here
    if (stateAwareScheduler) {
        stateAwareScheduler->resetScheduler();
    }

    auto availableHosts = getAvailableHosts();
    for (const auto& [ip, host] : state.hostMap) {
        host->set_statesync(true);
    }
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
}

std::vector<std::shared_ptr<Host>> Planner::getAvailableHosts()
{
    SPDLOG_DEBUG("Planner received request to get available hosts");

    // Acquire a full lock because we will also remove the hosts that have
    // timed out
    faabric::util::FullLock lock(plannerMx);

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
        regHost->set_hostsync(true);
        regHost->set_statesync(true);
        state.hostMap.emplace(
          std::make_pair<std::string, std::shared_ptr<Host>>(
            (std::string)hostIn.ip(), std::move(regHost)));
        // Update the host map to decentralized schedulers.
        for (const auto& [ip, host] : state.hostMap) {
            host->set_hostsync(true);
        }

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
    } else if (it != state.hostMap.end()) {
        SPDLOG_TRACE("NOT overwritting host {} with {} slots (used {})",
                     hostIn.ip(),
                     hostIn.slots(),
                     hostIn.usedslots());
    }

    // Irrespective, set the timestamp
    SPDLOG_TRACE("Setting timestamp for host {}", hostIn.ip());
    state.hostMap.at(hostIn.ip())
      ->mutable_registerts()
      ->set_epochms(faabric::util::getGlobalClock().epochMillis());

    state.batchSchedHostMap = convertToBatchSchedHostMap(state.hostMap);
    return true;
}

const std::
  pair<bool, std::map<std::string, faabric::batch_scheduler::FunctionStateInfo>>
  Planner::retrieveStateInfo(std::string hostIp)
{
    faabric::util::SharedLock lock(plannerMx);

    if (!stateAwareScheduler) {
        return { false, {} };
    }

    const auto it = state.hostMap.find(hostIp);
    if (it != state.hostMap.end()) {
        auto host = it->second;
        if (host->statesync()) {
            host->set_statesync(false);
            return { true, stateAwareScheduler->getStateInfo() };
        }
    }

    return { false, {} };
}

const std::pair<bool, HostPtrMap&> Planner::getRegisteredHost(
  std::string hostIp)
{
    faabric::util::SharedLock lock(plannerMx);

    auto it = state.hostMap.find(hostIp);
    if (it != state.hostMap.end() && it->second->hostsync()) {
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

void Planner::setMessageResultBatch(
  std::shared_ptr<faabric::BatchExecuteRequest> batchMsg)
{
    SPDLOG_DEBUG("Planner received message result batch with {} messages",
                 batchMsg->messages_size());
    // When outputing result, we doesn't allow any set result operation.
    RETURN_IF_OUTPUTTING
    faabric::util::FullLock lock(plannerStateMx);
    // Check again
    RETURN_IF_OUTPUTTING
    SPDLOG_DEBUG("InFlightApps size before set: {}", state.inFlightApps.size());
    for (int msgIdx = 0; msgIdx < batchMsg->messages_size(); msgIdx++) {
        auto msg = batchMsg->messages(msgIdx);
        int appId = msg.appid();
        int msgId = msg.id();
        int chainedId = msg.chainedid();
        if (appId != chainedId) {
            SPDLOG_ERROR("App Id and Chained ID are different: {} and {}",
                         appId,
                         chainedId);
            throw std::runtime_error("Message ID and Chained ID are different");
        }
        if (!state.inFlightApps.contains(appId)) {
            SPDLOG_ERROR("App {} is not in flight", appId);
            continue;
        }
        state.appResults[appId][msgId] =
          std::make_shared<faabric::Message>(msg);

        int chainedMsgNum = msg.chainedmsgnum();
        state.inFlightApps[appId] += chainedMsgNum;
        int inFlightAppCount = --state.inFlightApps[appId];
        if (inFlightAppCount <= 0) {
            state.inFlightApps.erase(appId);
        }
    }
    SPDLOG_DEBUG("InFlightApps size after set: {}", state.inFlightApps.size());
}

std::shared_ptr<faabric::Message> Planner::getMessageResult(
  std::shared_ptr<faabric::Message> msg)
{
    int appId = msg->appid();
    int msgId = msg->id();

    {
        faabric::util::SharedLock lock(plannerMx);

        // We debug and not error these messages as they happen frequently
        // when polling for results
        if (state.appResults.find(appId) == state.appResults.end()) {
            SPDLOG_DEBUG("App {} not registered in app results", appId);
        } else if (state.appResults[appId].find(msgId) ==
                   state.appResults[appId].end()) {
            SPDLOG_DEBUG("Msg {} not registered in app results (app id: {})",
                         msgId,
                         appId);
        } else {
            return state.appResults[appId][msgId];
        }
    }

    // If we are here, it means that we have not found the message result, so
    // we register the calling-host's interest if the calling-host has
    // provided a main host. The main host is set when dispatching a message
    // within faabric, but not when sending an HTTP request
    if (!msg->mainhost().empty()) {
        faabric::util::FullLock lock(plannerMx);

        // Check again if the result is not set, as it could have been set
        // between releasing the shared lock and acquiring the full lock
        if (state.appResults.contains(appId) &&
            state.appResults[appId].contains(msgId)) {
            return state.appResults[appId][msgId];
        }

        // Definately the message result is not set, so we add the host to the
        // waiters list
        SPDLOG_DEBUG("Adding host {} on the waiting list for message {}",
                     msg->mainhost(),
                     msgId);
        state.appResultWaiters[msgId].push_back(msg->mainhost());
    }

    return nullptr;
}

void Planner::preloadSchedulingDecision(
  int32_t appId,
  std::shared_ptr<batch_scheduler::SchedulingDecision> decision)
{
    faabric::util::FullLock lock(plannerMx);

    if (state.preloadedSchedulingDecisions.contains(appId)) {
        SPDLOG_ERROR(
          "ERROR: preloaded scheduling decisions already contain app {}",
          appId);
        return;
    }

    SPDLOG_INFO("Pre-loading scheduling decision for app {}", appId);
    state.preloadedSchedulingDecisions[appId] = decision;
}

std::shared_ptr<batch_scheduler::SchedulingDecision>
Planner::getPreloadedSchedulingDecision(
  int32_t appId,
  std::shared_ptr<BatchExecuteRequest> ber)
{
    SPDLOG_DEBUG("Requesting pre-loaded scheduling decision for app {}", appId);
    // WARNING: this method is currently only called from the main Planner
    // entrypoint (callBatch) which has a FullLock, thus we don't need to
    // acquire a (SharedLock) here. In general, we would need a read-lock
    // to read the dict from the planner's state
    auto decision = state.preloadedSchedulingDecisions.at(appId);
    assert(decision != nullptr);

    // Only include in the returned scheduling decision the group indexes that
    // are in this BER. This can happen when consuming a preloaded decision
    // in two steps (e.g. for MPI)
    std::shared_ptr<batch_scheduler::SchedulingDecision> filteredDecision =
      std::make_shared<batch_scheduler::SchedulingDecision>(decision->appId,
                                                            decision->groupId);
    for (const auto& msg : ber->messages()) {
        int groupIdx = msg.groupidx();
        int idxInDecision = std::distance(decision->groupIdxs.begin(),
                                          std::find(decision->groupIdxs.begin(),
                                                    decision->groupIdxs.end(),
                                                    groupIdx));
        assert(idxInDecision < decision->groupIdxs.size());

        // Add the schedulign for this group idx to the filtered decision.
        // Make sure we also maintain the message IDs that come from the BER
        // (as we can not possibly predict them in the preloaded decision)
        filteredDecision->addMessage(decision->hosts.at(idxInDecision),
                                     msg.id(),
                                     decision->appIdxs.at(idxInDecision),
                                     decision->groupIdxs.at(idxInDecision));
    }
    assert(filteredDecision->hosts.size() == ber->messages_size());

    return filteredDecision;
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

bool Planner::registerFuncState(const std::string& userFunction,
                                const std::string& partitionBy,
                                const std::string& stateKey)
{
    SPDLOG_DEBUG("Planner received request to register function state for {}",
                 userFunction);
    faabric::util::FullLock lock(plannerStateMx);
    bool registerResult = stateAwareScheduler->registerFunctionState(
      userFunction, partitionBy, stateKey, state.batchSchedHostMap);

    for (const auto& [ip, host] : state.hostMap) {
        host->set_statesync(true);
    }
    return registerResult;
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

faabric::batch_scheduler::InFlightReqs Planner::getInFlightReqs()
{
    faabric::util::SharedLock lock(plannerMx);

    // Deliberately deep copy here
    faabric::batch_scheduler::InFlightReqs inFlightReqsCopy;
    for (const auto& [appId, inFlightPair] : state.inFlightReqs) {
        inFlightReqsCopy[appId] = std::make_pair(
          std::make_shared<BatchExecuteRequest>(*inFlightPair.first),
          std::make_shared<faabric::batch_scheduler::SchedulingDecision>(
            *inFlightPair.second));
    }

    return inFlightReqsCopy;
}

int Planner::getNumMigrations()
{
    return state.numMigrations.load(std::memory_order_acquire);
}

void Planner::scheduleMessages(std::shared_ptr<BatchExecuteRequest> req,
                               bool isChained)
{
    SPDLOG_DEBUG("Planner is Scheduling {} messages", req->messages_size());
    auto currentTime = faabric::util::getGlobalClock().epochMicros();

    if (isChained) {
        SPDLOG_ERROR("Decentralized Scheduler schedule chained calls");
        throw std::runtime_error("Chained call scheduling is not supported "
                                 "in centralized schedulere");
    }

    // When outputing result, we doesn't allow any schedule message operation.
    RETURN_IF_OUTPUTTING
    // First loop: handle state without creating a shared_ptr
    faabric::util::FullLock lock(plannerStateMx);
    // check again
    RETURN_IF_OUTPUTTING
    int i = 0;
    while (i < req->messages_size()) {
        auto* message = req->mutable_messages(i); // Use a pointer directly

        // Record planner enqueue time
        message->set_plannerqueuetime(currentTime);

        // Record the chained call count
        int appid = message->appid();
        int chainedid = message->chainedid();
        // Now we asssume AppId is equal to ChainedId
        if (chainedid != appid) {
            SPDLOG_ERROR("ChainedId is not equal to AppId");
            throw std::runtime_error("ChainedId is not equal to AppId");
        }

        if (state.inFlightApps.contains(appid)) {
            SPDLOG_ERROR("app Id {} is already running", appid);
            // Flush the old chainedId
            state.inFlightApps[appid] = 0;
        }

        state.inFlightApps[appid]++;
        i++; // Only increment i if a message was not removed
    }
    lock.unlock();

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

    enqueueMessageBatch(hosts, std::move(messages)); // Move ownership
}

void Planner::enqueueMessageBatch(
  std::vector<std::string> hosts,
  std::vector<std::unique_ptr<faabric::Message>> msgs)
{
    auto currentTime = faabric::util::getGlobalClock().epochMicros();
    faabric::util::FullLock lock(state.scheduledMsgsMapMx);
    for (int i = 0; i < msgs.size(); i++) {
        auto msg = std::move(msgs[i]);
        auto host = hosts[i];
        msg->set_plannerpoptime(currentTime);
        state.scheduledMsgsMap[host].push_back(std::move(msg));
    }
}

void Planner::dequeueScheduledMsgs()
{
    while (!stopThreadTimer) {
        // Sleep for a while to batch the scheduled requests
        std::this_thread::sleep_for(std::chrono::milliseconds(dispatchPeriod));
        // Lock only for copying and clearing `scheduledMsgsMap`
        CONTINUE_IF_OUTPUTTING
        faabric::util::FullLock lock(state.scheduledMsgsMapMx);
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
                msg->set_plannerdispatchtime(currentTime);
            }
            msgsCallMap[hostIp] = std::move(msgsList); // Move ownership
        }
        state.scheduledMsgsMap.clear();
        lock.unlock();

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
    faabric::util::SharedLock lock(plannerStateMx);
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

bool Planner::updateFuncParallelism(const std::string& userFunction,
                                    int changedParallelism)
{
    SPDLOG_DEBUG("Planner received request to changed parallelism for {} by {}",
                 userFunction,
                 changedParallelism);
    faabric::util::FullLock lock(plannerMx);

    if (!stateAwareScheduler) {
        SPDLOG_ERROR("State-aware scheduler is not enabled");
        return false;
    }
    stateAwareScheduler->increaseFunctionParallelism(
      changedParallelism, userFunction, state.batchSchedHostMap);

    for (const auto& [ip, host] : state.hostMap) {
        host->set_statesync(true);
    }
    return true;
}

bool Planner::resetBatchsize(int32_t newSize)
{
    auto availableHosts = getAvailableHosts();
    faabric::planner::BatchResetRequest req;
    req.set_batchsize(newSize);

    for (const auto& host : availableHosts) {
        SPDLOG_INFO(
          "Planner resize the batchsize {} to {}", host->ip(), newSize);
        faabric::scheduler::getFunctionCallClient(host->ip())
          ->resetBatchSize(
            std::make_shared<faabric::planner::BatchResetRequest>(req));
    }

    return true;
}

bool Planner::resetMaxReplicas(int32_t maxReplicas)
{
    auto availableHosts = getAvailableHosts();
    faabric::planner::MaxReplicasRequest req;
    req.set_maxnum(maxReplicas);

    for (const auto& host : availableHosts) {
        SPDLOG_INFO("Planner max replicas {} to {}", host->ip(), maxReplicas);
        faabric::scheduler::getFunctionCallClient(host->ip())
          ->resetMaxReplicas(
            std::make_shared<faabric::planner::MaxReplicasRequest>(req));
    }

    return true;
}

bool Planner::resetParameter(const std::string& key,
                             const int32_t value,
                             bool plannerParameter)
{
    // Reset the parameter of planner
    if (plannerParameter) {
        if (key == "dispatch_period") {
            SPDLOG_INFO("Planner reset dispatchPeriod to {}", value);
            dispatchPeriod = value;
        } else if (key == "is_outputting") {
            SPDLOG_INFO("Planner reset isOutputting to {}", value == 1);
            isOutputting = value == 1;
        }
        return true;
    }
    // Reset the parameter of the worker hosts
    auto availableHosts = getAvailableHosts();
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

    return true;
}

void Planner::outputAppResultsToJson()
{
    faabric::util::FullLock lock(plannerStateMx);
    isOutputting = true;
    // We have to unlock, since the output operation may take a long time
    lock.unlock();
    if (state.appResults.empty()) {
        SPDLOG_INFO("No results to output");
        return;
    }

    try {
        rapidjson::Document document;
        document.SetObject();
        rapidjson::Document::AllocatorType& allocator = document.GetAllocator();

        for (const auto& [appId, messages] : state.appResults) {
            if (state.inFlightApps.contains(appId)) {
                SPDLOG_DEBUG("App {} is still in flight", appId);
                continue;
            }
            rapidjson::Value appData(rapidjson::kArrayType);
            for (const auto& [messageId, message] : messages) {
                rapidjson::Value messageData(rapidjson::kObjectType);

                // Populate the messageData with the specified fields
                messageData.AddMember("id", message->id(), allocator);
                messageData.AddMember("appId", message->appid(), allocator);
                messageData.AddMember(
                  "user",
                  rapidjson::StringRef(message->user().c_str()),
                  allocator);
                messageData.AddMember(
                  "function",
                  rapidjson::StringRef(message->function().c_str()),
                  allocator);
                messageData.AddMember(
                  "output_data",
                  rapidjson::StringRef(message->outputdata().c_str()),
                  allocator);
                messageData.AddMember(
                  "start_ts", message->starttimestamp(), allocator);
                messageData.AddMember(
                  "finish_ts", message->finishtimestamp(), allocator);
                messageData.AddMember(
                  "plannerQueueTime", message->plannerqueuetime(), allocator);
                messageData.AddMember(
                  "plannerPopTime", message->plannerpoptime(), allocator);
                messageData.AddMember("plannerDispatchTime",
                                      message->plannerdispatchtime(),
                                      allocator);
                messageData.AddMember(
                  "workerQueueTime", message->workerqueuetime(), allocator);
                messageData.AddMember(
                  "workerPopTime", message->workerpoptime(), allocator);
                messageData.AddMember("ExecutorPrepareTime",
                                      message->executorpreparetime(),
                                      allocator);
                messageData.AddMember("workerExecuteStart",
                                      message->workerexecutestart(),
                                      allocator);
                messageData.AddMember(
                  "workerExecuteEnd", message->workerexecuteend(), allocator);
                messageData.AddMember(
                  "chainedId", message->chainedid(), allocator);
                messageData.AddMember(
                  "parallelismId", message->parallelismid(), allocator);

                appData.PushBack(messageData, allocator);
            }
            document.AddMember(
              rapidjson::Value(std::to_string(appId).c_str(), allocator).Move(),
              appData,
              allocator);
        }

        // Write the JSON to file
        rapidjson::StringBuffer buffer;
        rapidjson::PrettyWriter<rapidjson::StringBuffer> writer(buffer);
        document.Accept(writer);

        std::ofstream outFile("/tmp/faasm_result.txt");
        if (!outFile.is_open()) {
            throw std::runtime_error("Unable to open output file");
        }
        outFile << buffer.GetString();
        outFile.close();

        SPDLOG_INFO("Successfully wrote results to /tmp/faasm_result.txt");
    } catch (const std::exception& e) {
        SPDLOG_ERROR("Error outputting results to JSON: {}", e.what());
    } catch (...) {
        SPDLOG_ERROR("Unknown error occurred while outputting results to JSON");
    }

    state.appResults.clear();
}

Planner& getPlanner()
{
    static Planner planner;
    return planner;
}
}
