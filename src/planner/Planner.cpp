#include <faabric/batch-scheduler/BatchScheduler.h>
#include <faabric/batch-scheduler/SchedulingDecision.h>
#include <faabric/batch-scheduler/StateAwareScheduler.h>
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

// ----------------------
// Utility Functions
// ----------------------

// static void claimHostSlots(std::shared_ptr<Host> host, int slotsToClaim = 1)
// {
//     host->set_usedslots(host->usedslots() + slotsToClaim);
//     assert(host->usedslots() <= host->slots());
// }

// static void releaseHostSlots(std::shared_ptr<Host> host, int slotsToRelease =
// 1)
// {
//     host->set_usedslots(host->usedslots() - slotsToRelease);
//     assert(host->usedslots() >= 0);
// }

// static void printHostState(std::map<std::string, std::shared_ptr<Host>>
// hostMap,
//                            const std::string& logLevel = "debug")
// {
//     std::string printedText;
//     std::string header = "\n-------------- Host Map --------------";
//     std::string subhead = "Ip\t\tSlots";
//     std::string footer = "--------------------------------------";

//     printedText += header + "\n" + subhead + "\n";
//     for (const auto& [ip, hostState] : hostMap) {
//         printedText += fmt::format(
//           "{}\t{}/{}\n", ip, hostState->usedslots(), hostState->slots());
//     }
//     printedText += footer;

//     if (logLevel == "debug") {
//         SPDLOG_DEBUG(printedText);
//     } else if (logLevel == "info") {
//         SPDLOG_INFO(printedText);
//     } else if (logLevel == "warn") {
//         SPDLOG_WARN(printedText);
//     } else if (logLevel == "error") {
//         SPDLOG_ERROR(printedText);
//     } else {
//         SPDLOG_ERROR("Unrecognised log level: {}", logLevel);
//     }
// }

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

    batchTimerThread = std::thread(&Planner::batchTimerCheck, this);
    dequeueScheduledRequestsThread =
      std::thread(&Planner::dequeueScheduledRequests, this);
}

Planner::~Planner()
{
    // Stop the batch timer thread
    stopThreadTimer = true;
    if (batchTimerThread.joinable()) {
        batchTimerThread.join();
    }
    if (dequeueScheduledRequestsThread.joinable()) {
        dequeueScheduledRequestsThread.join();
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
}

void Planner::flushExecutors()
{
    // Flush Planner State
    // state.funcLatencyStats.clear();
    // state.chainFuncLatencyStats.clear();
    // state.appChainedInflights.clear();
    // Flush Aware Scheduler State
    auto batchScheduler = faabric::batch_scheduler::getBatchScheduler();
    auto stateAwareScheduler =
      std::dynamic_pointer_cast<batch_scheduler::StateAwareScheduler>(
        batchScheduler);
    // If preload the parallelism desision, we change the parallelism here
    if (stateAwareScheduler) {
        stateAwareScheduler->flushStateInfo();
    }

    auto availableHosts = getAvailableHosts();

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

    state.inFlightChains.clear();
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
        state.hostMap.emplace(
          std::make_pair<std::string, std::shared_ptr<Host>>(
            (std::string)hostIn.ip(), std::make_shared<Host>(hostIn)));
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

    return true;
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

void Planner::setMessageResult(std::shared_ptr<faabric::Message> msg,
                               bool locked)
{}

void Planner::setMessageResultWitoutLock(std::shared_ptr<faabric::Message> msg)
{
    int appId = msg->appid();
    int msgId = msg->id();

    faabric::util::FullLock lock(plannerMx);

    SPDLOG_TRACE("Planner setting message result (id: {}) for {}:{}:{}",
                 msg->id(),
                 msg->appid(),
                 msg->groupid(),
                 msg->groupidx());

    // Release the slot only once
    assert(state.hostMap.contains(msg->executedhost()));

    // Set the result
    state.appResults[appId][msgId] = msg;

    if (!state.inFlightReqs.contains(appId)) {
        // We don't want to error if any client uses `setMessageResult`
        // liberally. This means that it may happen that when we set a message
        // result a second time, the app is already not in-flight
        SPDLOG_DEBUG("Setting result for non-existant (or finished) app: {}",
                     appId);
    } else {
        auto req = state.inFlightReqs.at(appId).first;
        auto decision = state.inFlightReqs.at(appId).second;

        // Work out the message position in the BER
        auto it = std::find_if(
          req->messages().begin(), req->messages().end(), [&](auto innerMsg) {
              return innerMsg.id() == msg->id();
          });
        if (it == req->messages().end()) {
            // Ditto as before. We want to allow setting the message result
            // more than once without breaking
            SPDLOG_DEBUG(
              "Setting result for non-existant (or finished) message: {}",
              appId);
        } else {
            SPDLOG_TRACE("Removing message {} from app {}", msg->id(), appId);

            // Remove message from in-flight requests
            req->mutable_messages()->erase(it);

            // Remove message from decision
            decision->removeMessage(msg->id());

            // Record the Metrics
            std::string userFunc = msg->user() + "_" + msg->function();
            int parallelismId = msg->parallelismid();
            std::string userFuncPar =
              userFunc + "_" + std::to_string(parallelismId);

            // Remove from in-flight chained requests
            int chainedId = msg->chainedid();
            state.inFlightChains[chainedId]--;
            if (state.inFlightChains[chainedId] < 0) {
                SPDLOG_ERROR("In-flight chains count is negative: {}",
                             state.inFlightChains[chainedId]);
                throw std::runtime_error("Chained Id removes negative count");
            }
            if (state.inFlightChains[chainedId] == 0) {
                state.inFlightChains.erase(chainedId);
            }

            // Remove pair altogether if no more messages left
            if (req->messages_size() == 0) {
                state.inFlightReqs.erase(appId);
            }
        }
    }
    // Finally, dispatch an async message to all hosts that are waiting once
    // all planner accounting is updated
    if (state.appResultWaiters.find(msgId) != state.appResultWaiters.end()) {
        for (const auto& host : state.appResultWaiters[msgId]) {
            SPDLOG_DEBUG("Sending result to waiting host: {}", host);
            faabric::scheduler::getFunctionCallClient(host)->setMessageResult(
              msg);
        }
    }
}

void Planner::setMessageResultBatch(
  std::shared_ptr<faabric::BatchExecuteRequest> batchMsg)
{
    for (int msgIdx = 0; msgIdx < batchMsg->messages_size(); msgIdx++) {
        auto msg = batchMsg->messages(msgIdx);
        setMessageResultWitoutLock(std::make_shared<faabric::Message>(msg));
    }
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
        if (state.inFlightReqs.contains(appId)) {
            berStatus->set_finished(false);
            return berStatus;
        }

        for (auto msgResultPair : state.appResults.at(appId)) {
            *berStatus->add_messageresults() = *(msgResultPair.second);
        }

        // Set the finished condition
        berStatus->set_finished(!state.inFlightReqs.contains(appId));

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

void Planner::enqueueCallBatch(std::shared_ptr<BatchExecuteRequest> req,
                               bool isChained)
{
    int appId = req->appid();

    faabric::util::FullLock lock(plannerMx);

    // Record planner enqueue time
    auto currentTime = faabric::util::getGlobalClock().epochMicros();
    for (int i = 0; i < req->messages_size(); i++) {
        req->mutable_messages(i)->set_plannerqueuetime(currentTime);
        int chainedId = req->messages(i).chainedid();
        if (isChained) {
            if (!state.inFlightChains.contains(chainedId)) {
                SPDLOG_ERROR("ChainedId {} is not running", chainedId);
                throw std::runtime_error("Chained call id cannot be found");
            }
            state.inFlightChains[chainedId]++;
        } else {
            if (state.inFlightChains.contains(chainedId)) {
                SPDLOG_ERROR("ChainedId {} already exists", chainedId);
                throw std::runtime_error(
                  "ChainedId already exists and running");
            }
            state.inFlightChains[chainedId] = 1;
        }
    }

    if (isChained) {
        // If the message is chained, we need to record it to the state inflight
        // and chained count at first. Otherwise, the caller function might
        // finish before this function is recorded. In this case, the set batch
        // result might mark this chained functions are finished.
        auto oldReq = state.inFlightReqs.at(appId).first;
        auto oldDec = state.inFlightReqs.at(appId).second;

        for (size_t i = 0; i < req->messages_size(); i++) {
            *oldReq->add_messages() = req->messages(i);
            oldDec->addMessage("enqueue_not_set", req->messages(i));

            faabric::Message tempMessage = req->messages(i);
            // int chainedid = tempMessage.chainedid();
            // state.appChainedInflights[appId][chainedid]++;
        }
    }
    return batchExecuteReqQueue.enqueue(req);
}

void Planner::batchTimerCheck()
{
    while (!stopThreadTimer) {
        // If empty, this thread will be blocked.
        std::shared_ptr<BatchExecuteRequest> req =
          batchExecuteReqQueue.dequeue();
        callBatchWithoutLock(req);
    }
}

void Planner::callBatchWithoutLock(std::shared_ptr<BatchExecuteRequest> req)
{
    int appId = req->appid();

    faabric::util::FullLock lock(plannerMx);

    // Record planner dequeue time
    auto currentTime = faabric::util::getGlobalClock().epochMicros();
    for (int i = 0; i < req->messages_size(); i++) {
        req->mutable_messages(i)->set_plannerpoptime(currentTime);
    }

    auto batchScheduler = faabric::batch_scheduler::getBatchScheduler();
    auto decisionType =
      batchScheduler->getDecisionType(state.inFlightReqs, req);

    std::shared_ptr<batch_scheduler::StateAwareScheduler> stateAwareScheduler =
      std::dynamic_pointer_cast<batch_scheduler::StateAwareScheduler>(
        batchScheduler);

    if (!stateAwareScheduler) {
        throw std::runtime_error(
          "enqueue call batch is only supported by state aware scheduler");
    }

    // In stream processing, only the NEW and SCALE_CHANGE are supported.
    if (decisionType == faabric::batch_scheduler::DecisionType::DIST_CHANGE) {
        throw std::runtime_error(
          "Dist change is not supported in the stream mode");
    }

    auto hostMapCopy = convertToBatchSchedHostMap(state.hostMap);

    std::shared_ptr<batch_scheduler::SchedulingDecision> decision = nullptr;

    decision = stateAwareScheduler->scheduleWithoutLock(
      hostMapCopy, state.inFlightReqs, req);

    assert(decision != nullptr);
    switch (decisionType) {
        case faabric::batch_scheduler::DecisionType::NEW: {

            // 1. For a new decision, we just add it to the in-flight map
            state.inFlightReqs[appId] = std::make_pair(req, decision);
            // 2 For a new decision, we record its metrics
            // Metrics include chained metrics and function metrics (BOTH
            // message level)
            for (size_t i = 0; i < req->messages_size(); i++) {
                faabric::Message tempMessage = req->messages(i);
                int tempParallelisimId = tempMessage.parallelismid();
                // int msgId = tempMessage.id();
                // int chainedId = tempMessage.chainedid();
                std::string userFunc =
                  tempMessage.user() + "_" + tempMessage.function();
                std::string userFuncPar =
                  userFunc + "_" + std::to_string(tempParallelisimId);
            }
            break;
        }
        case faabric::batch_scheduler::DecisionType::SCALE_CHANGE: {
            // 1. TODO - Up date the enqueue_not_set deceision

            // 2. Record the metrics for each function.
            for (size_t i = 0; i < req->messages_size(); i++) {
                faabric::Message tempMessage = req->messages(i);
                std::string userFunc =
                  tempMessage.user() + "_" + tempMessage.function();
                int tempParallelisimId = tempMessage.parallelismid();
                // int msgId = tempMessage.id();
                std::string userFuncPar =
                  userFunc + "_" + std::to_string(tempParallelisimId);
            }
            break;
        }
        default: {
            SPDLOG_ERROR("Unrecognised decision type: {} (app: {})",
                         decisionType,
                         req->appid());
            throw std::runtime_error("Unrecognised decision type");
        }
    }
    // Sanity-checks before actually dispatching functions for execution
    assert(req->messages_size() == decision->hosts.size());
    assert(req->appid() == decision->appId);

    dispatchSchedulingDecision(req, decision);
}

std::shared_ptr<faabric::batch_scheduler::SchedulingDecision>
Planner::callBatch(std::shared_ptr<BatchExecuteRequest> req)
{
    return nullptr;
}

void Planner::dispatchSchedulingDecision(
  std::shared_ptr<faabric::BatchExecuteRequest> req,
  std::shared_ptr<faabric::batch_scheduler::SchedulingDecision> decision)
{
    std::map<std::string, std::shared_ptr<faabric::BatchExecuteRequest>>
      hostRequests;

    assert(req->messages_size() == decision->hosts.size());
    bool isSingleHost = decision->isSingleHost();

    auto currentTime = faabric::util::getGlobalClock().epochMicros();
    // First we build all the BatchExecuteRequests for all the different hosts.
    // We need to keep a map as the hosts may not be contiguous in the decision
    // (i.e. we may have (hostA, hostB, hostA)
    for (int i = 0; i < req->messages_size(); i++) {
        auto mutable_msg = req->mutable_messages(i);
        mutable_msg->set_plannerdispatchtime(currentTime);
        auto msg = req->messages().at(i);

        // Initialise the BER if it is not there
        std::string thisHost = decision->hosts.at(i);
        if (hostRequests.find(thisHost) == hostRequests.end()) {
            hostRequests[thisHost] = faabric::util::batchExecFactory();
            hostRequests[thisHost]->set_appid(decision->appId);
            hostRequests[thisHost]->set_groupid(decision->groupId);
            hostRequests[thisHost]->set_user(msg.user());
            hostRequests[thisHost]->set_function(msg.function());
            hostRequests[thisHost]->set_snapshotkey(req->snapshotkey());
            hostRequests[thisHost]->set_type(req->type());
            hostRequests[thisHost]->set_subtype(req->subtype());
            hostRequests[thisHost]->set_contextdata(req->contextdata());
            hostRequests[thisHost]->set_singlehost(isSingleHost);
            // Propagate the single host hint
            hostRequests[thisHost]->set_singlehosthint(req->singlehosthint());
        }

        *hostRequests[thisHost]->add_messages() = msg;
    }

    bool isThreads = req->type() == faabric::BatchExecuteRequest::THREADS;
    if (!isSingleHost && req->singlehosthint()) {
        SPDLOG_ERROR(
          "User provided single-host hint in BER, but decision is not!");
    }

    for (const auto& [hostIp, hostReq] : hostRequests) {
        SPDLOG_DEBUG("Dispatching {} messages to host {} for execution",
                     hostReq->messages_size(),
                     hostIp);
        assert(faabric::util::isBatchExecRequestValid(hostReq));

        // In a THREADS request, before sending an execution request we need to
        // push the main (caller) thread snapshot to all non-main hosts
        // FIXME: ideally, we would do this from the caller thread, once we
        // know the scheduling decision and all other threads would be awaiting
        // for the snapshot
        if (isThreads && !isSingleHost) {
            auto snapshotKey =
              faabric::util::getMainThreadSnapshotKey(hostReq->messages(0));
            try {
                auto snap = snapshotRegistry.getSnapshot(snapshotKey);

                // TODO(thread-opt): push only diffs
                if (hostIp != req->messages(0).mainhost()) {
                    faabric::snapshot::getSnapshotClient(hostIp)->pushSnapshot(
                      snapshotKey, snap);
                }
            } catch (std::runtime_error& e) {
                // Catch errors, but don't let them crash the planner. Let the
                // worker crash instead
                SPDLOG_ERROR("Snapshot {} not regsitered in planner!",
                             snapshotKey);
            }
        }

        enqueueScheduledRequests(hostIp, hostReq);
    }

    SPDLOG_DEBUG("Finished dispatching {} messages for execution",
                 req->messages_size());
}

void Planner::enqueueScheduledRequests(std::string host,
                                       std::shared_ptr<BatchExecuteRequest> req)
{
    faabric::util::FullLock lock(state.scheduledRequestsMapMx);
    state.scheduledRequestsMap[host].push_back(req);
}

void Planner::dequeueScheduledRequests()
{
    while (!stopThreadTimer) {
        // If empty, this thread will be blocked.
        faabric::util::FullLock lock(state.scheduledRequestsMapMx);
        // Create a local filterd copy of the scheduledRequestsMap
        std::map<std::string, std::list<std::shared_ptr<BatchExecuteRequest>>>
          reqsCallMap;
        for (auto& [hostIp, reqsList] : state.scheduledRequestsMap) {
            if (!reqsList.empty()) {
                reqsCallMap[hostIp] = reqsList;
                reqsList.clear();
            }
        }
        lock.unlock();

        std::vector<std::thread> threads;
        for (auto& [hostIp, reqs] : reqsCallMap) {
            auto reqsCopy = std::move(reqs);

            threads.emplace_back(
              [hostIp, reqsCopy = std::move(reqsCopy)]() mutable {
                  faabric::scheduler::getFunctionCallClient(hostIp)
                    ->executeFunctionsBatch(reqsCopy);
              });
        }
        // Join all threads to ensure they complete before next iteration
        for (auto& t : threads) {
            if (t.joinable()) {
                t.join();
            }
        }
        // Sleep for a while to batch the scheduled requests
        std::this_thread::sleep_for(std::chrono::milliseconds(dispatchPeriod));
    }
}

int Planner::getInFlightChainsSize()
{
    return state.inFlightChains.size();
}

std::map<std::string, FunctionMetrics> Planner::collectMetrics()
{
    // metrics states contains two types information: topology and function
    // Topology info is stored with source function name: UserFunction
    // Function info is stored with function name with parallelism: UserFuncPar
    std::map<std::string, FunctionMetrics> metricsStats;

    std::map<std::string, std::map<std::string, int>> tempMetrics;
    // Retrive Metrics from State Server.
    for (auto [ithHostName, ithHostObject] : state.hostMap) {
        state::FunctionStateClient cli(ithHostName);
        auto hostMetrics = cli.getMetrics();
        for (auto& [ithUserFuncPar, ithStateMetrics] : hostMetrics) {
            // Accumulate metrics
            tempMetrics[ithUserFuncPar][LOCK_BLOCK_TIME] +=
              ithStateMetrics[LOCK_BLOCK_TIME];
            tempMetrics[ithUserFuncPar][LOCK_HOLD_TIME] +=
              ithStateMetrics[LOCK_HOLD_TIME];
        }
    }

    // Print the metrics.
    for (auto [ithFunc, ithMetrics] : metricsStats) {
        SPDLOG_DEBUG("Metrics for {}: throughput: {}, processLatency: {}, "
                     "averageWaitingTime: {}, lockCongestionTime: {}, "
                     "lockHoldTime: {}",
                     ithFunc,
                     ithMetrics.throughput,
                     ithMetrics.processLatency,
                     ithMetrics.averageWaitingTime,
                     ithMetrics.lockCongestionTime,
                     ithMetrics.lockHoldTime);
    }
    return metricsStats;
}

bool Planner::updateFuncParallelism(const std::string& userFunction,
                                    int changedParallelism)
{
    SPDLOG_DEBUG("Planner received request to changed parallelism for {} by {}",
                 userFunction,
                 changedParallelism);
    faabric::util::FullLock lock(plannerMx);
    auto batchScheduler = faabric::batch_scheduler::getBatchScheduler();
    std::shared_ptr<batch_scheduler::StateAwareScheduler> stateAwareScheduler =
      std::dynamic_pointer_cast<batch_scheduler::StateAwareScheduler>(
        batchScheduler);
    if (!stateAwareScheduler) {
        SPDLOG_ERROR("State-aware scheduler is not enabled");
        return false;
    }
    auto hostMapCopy = convertToBatchSchedHostMap(state.hostMap);
    stateAwareScheduler->increaseFunctionParallelism(
      changedParallelism, userFunction, hostMapCopy);
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

bool Planner::resetParameter(const std::string& key, const int32_t value, bool plannerParameter)
{   
    // Reset the parameter of planner
    if (plannerParameter) {
        if (key == "dispatch_period") {
            SPDLOG_INFO("Planner reset dispatchPeriod to {}", value);
            dispatchPeriod = value;
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
    if (state.appResults.empty()) {
        SPDLOG_INFO("No results to output");
        return;
    }

    try {
        rapidjson::Document document;
        document.SetObject();
        rapidjson::Document::AllocatorType& allocator = document.GetAllocator();

        for (const auto& [appId, messages] : state.appResults) {
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
