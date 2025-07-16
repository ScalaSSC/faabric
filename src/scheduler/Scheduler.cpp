#include <faabric/batch-scheduler/BatchScheduler.h>
#include <faabric/executor/ExecutorFactory.h>
#include <faabric/planner/PlannerClient.h>
#include <faabric/planner/planner.pb.h>
#include <faabric/proto/faabric.pb.h>
#include <faabric/scheduler/FunctionCallClient.h>
#include <faabric/scheduler/Scheduler.h>
#include <faabric/snapshot/SnapshotClient.h>
#include <faabric/snapshot/SnapshotRegistry.h>
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
#include <faabric/util/snapshot.h>
#include <faabric/util/string_tools.h>
#include <faabric/util/testing.h>

#include <sstream>
#include <unordered_set>

using namespace faabric::util;
using namespace faabric::snapshot;

constexpr int DEFAULT_SLOT_NUM = 100;

const std::string WORKER_ENQUEUE_TIME_KEY = "worker_queue_time_key";
const std::string WORKER_ENQUEUE_SIZE_KEY = "worker_queue_size_key";

namespace faabric::scheduler {

static faabric::batch_scheduler::HostMap convertHostMap(
  const std::map<std::string, std::string>& registeredHostsMap)
{
    faabric::batch_scheduler::HostMap hostMap;
    for (const auto& [ip, hostId] : registeredHostsMap) {
        hostMap[ip] = std::make_shared<faabric::batch_scheduler::HostState>(
          ip, DEFAULT_SLOT_NUM, 0);
    }
    return hostMap;
}

Scheduler& getScheduler()
{
    static Scheduler sch;
    return sch;
}

Scheduler::Scheduler()
  : thisHost(faabric::util::getSystemConfig().endpointHost)
  , conf(faabric::util::getSystemConfig())
  , reg(faabric::snapshot::getSnapshotRegistry())
  , broker(faabric::transport::getPointToPointBroker())
//   , instancesLoadState(maxSamples)
{
    executeBatchsize = conf.batchSize;
    // Start the reaper thread
    reaperThread.start(conf.reaperIntervalSeconds);
    batchTimerThread = std::thread(&Scheduler::batchTimerCheck, this);
    dispatchChainedMsgsThread =
      std::thread(&Scheduler::dispatchChainedMsgs, this);
}

Scheduler::~Scheduler()
{
    if (!_isShutdown) {
        SPDLOG_ERROR("Destructing scheduler without shutting down first");
    }
    // Stop the batch timer thread
    stopBatchTimer = true;
    if (batchTimerThread.joinable()) {
        batchTimerThread.join();
    }
    stopThreadTimer = true;
    if (dispatchChainedMsgsThread.joinable()) {
        dispatchChainedMsgsThread.join();
    }
}

void Scheduler::addHostToGlobalSet(
  const std::string& hostIp,
  std::shared_ptr<faabric::HostResources> overwriteResources)
{
    // Build register host request. Setting the overwrite flag means that we
    // will overwrite whatever records the planner has on this host. We only
    // set it when calling this method for a different host (e.g. in the tests)
    // or when passing an overwrited host-resources (e.g. when calling
    // setThisHostResources)
    auto req = std::make_shared<faabric::planner::RegisterHostRequest>();
    req->mutable_host()->set_ip(hostIp);
    req->set_overwrite(false);
    if (overwriteResources != nullptr) {
        req->mutable_host()->set_slots(overwriteResources->slots());
        req->mutable_host()->set_usedslots(overwriteResources->usedslots());
        req->set_overwrite(true);
    } else if (hostIp == thisHost) {
        req->mutable_host()->set_slots(faabric::util::getUsableCores());
        req->mutable_host()->set_usedslots(0);
        if (conf.overWriteSlots != 0) {
            req->mutable_host()->set_slots(conf.overWriteSlots);
        }
    }

    auto registerHostResult =
      faabric::planner::getPlannerClient().registerHost(req);
    int plannerTimeout = std::get<0>(registerHostResult);

    auto hostSync = std::get<1>(registerHostResult);
    if (!hostSync) {
        SPDLOG_ERROR("Host {} not registered in planner", hostIp);
        throw std::runtime_error("Host not registered in planner");
    }
    auto& hostMap = std::get<2>(registerHostResult);
    updateHosts(hostMap);

    // Once the host is registered, set-up a periodic thread to send a heart-
    // beat to the planner. Note that this method may be called multiple times
    // during the tests, so we only set the scheduler's variable if we are
    // actually registering this host. Also, only start the keep-alive thread
    // if not in test mode
    if (hostIp == thisHost && !faabric::util::isTestMode()) {
        keepAliveThread.setRequest(req);
        keepAliveThread.start(plannerTimeout / 2);
    }
}

void Scheduler::addHostToGlobalSet()
{
    addHostToGlobalSet(thisHost);
}

void Scheduler::removeHostFromGlobalSet(const std::string& hostIp)
{
    auto req = std::make_shared<faabric::planner::RemoveHostRequest>();
    bool isThisHost =
      hostIp == thisHost && keepAliveThread.thisHostReq != nullptr;
    if (isThisHost) {
        *req->mutable_host() = *keepAliveThread.thisHostReq->mutable_host();
    } else {
        req->mutable_host()->set_ip(hostIp);
    }

    faabric::planner::getPlannerClient().removeHost(req);

    // Clear the keep alive thread
    if (isThisHost) {
        keepAliveThread.stop();
    }
}

void Scheduler::resetThreadLocalCache()
{
    SPDLOG_DEBUG("Resetting scheduler thread-local cache");
}

void Scheduler::reset()
{
    SPDLOG_DEBUG("Resetting scheduler");
    resetThreadLocalCache();

    // Stop the reaper thread
    reaperThread.stop();

    stopBatchTimer = true;
    if (batchTimerThread.joinable()) {
        batchTimerThread.join();
    }

    stopThreadTimer = true;
    if (dispatchChainedMsgsThread.joinable()) {
        dispatchChainedMsgsThread.join();
    }

    // Shut down, then clear executors
    for (auto& ep : executors) {
        for (auto& e : ep.second) {
            e->shutdown();
        }
    }
    executors.clear();

    // Clear the point to point broker
    broker.clear();

    // Clear the clients
    clearFunctionCallClients();
    clearSnapshotClients();
    faabric::planner::getPlannerClient().clearCache();

    faabric::util::FullLock lock(mx);
    faabric::util::FullLock msgMapLock(scheduledMsgsMapMx);

    // Ensure host is set correctly
    thisHost = faabric::util::getSystemConfig().endpointHost;

    // Reset scheduler state
    threadResultMessages.clear();

    // Records
    recordedMessages.clear();

    // Restart reaper thread
    reaperThread.start(conf.reaperIntervalSeconds);

    waitingQueues.clear();
    partitionedWaitingQueues.clear();
    {
        faabric::util::FullLock chainedCallMsgslock(chainedCallMsgsMx);
        chainedCallMsgs.clear();
    }
    {
        faabric::util::FullLock setResultMsgslock(setResultMsgsMx);
        setResultMsgs.clear();
    }

    scheduledMsgsMap.clear();

    // This function is called when planner flush executors. In this case,
    // planner didn't flush the hostmap, the scheduler also should not flush it.
    // registeredHostsMap.clear();
    // hostMap.clear();

    stopBatchTimer = false;
    batchTimerThread = std::thread(&Scheduler::batchTimerCheck, this);

    stopThreadTimer = false;
    dispatchChainedMsgsThread =
      std::thread(&Scheduler::dispatchChainedMsgs, this);

    decentralScheduler.resetScheduler();
}

void Scheduler::shutdown()
{
    reset();

    reaperThread.stop();

    removeHostFromGlobalSet(thisHost);

    _isShutdown = true;
}

void SchedulerReaperThread::doWork()
{
    getScheduler().reapStaleExecutors();
}

int Scheduler::reapStaleExecutors()
{
    faabric::util::FullLock lock(mx);

    if (executors.empty()) {
        SPDLOG_DEBUG("No executors to check for reaping");
        return 0;
    }

    std::vector<std::string> keysToRemove;

    int nReaped = 0;
    for (auto& execPair : executors) {
        std::string key = execPair.first;
        std::vector<std::shared_ptr<faabric::executor::Executor>>& execs =
          execPair.second;
        std::vector<std::shared_ptr<faabric::executor::Executor>> toRemove;

        if (execs.empty()) {
            continue;
        }

        SPDLOG_TRACE(
          "Checking {} executors for {} for reaping", execs.size(), key);

        faabric::Message& firstMsg = execs.back()->getBoundMessage();
        std::string user = firstMsg.user();
        std::string function = firstMsg.function();
        std::string mainHost = firstMsg.mainhost();

        for (auto exec : execs) {
            long millisSinceLastExec = exec->getMillisSinceLastExec();
            if (millisSinceLastExec < conf.boundTimeout) {
                // This executor has had an execution too recently
                SPDLOG_TRACE("Not reaping {}, last exec {}ms ago (limit {}ms)",
                             exec->id,
                             millisSinceLastExec,
                             conf.boundTimeout);
                continue;
            }

            // Check if executor is currently executing
            if (exec->isExecuting()) {
                SPDLOG_TRACE("Not reaping {}, currently executing", exec->id);
                continue;
            }

            SPDLOG_TRACE("Reaping {}, last exec {}ms ago (limit {}ms)",
                         exec->id,
                         millisSinceLastExec,
                         conf.boundTimeout);

            toRemove.emplace_back(exec);
            nReaped++;
        }

        // Remove those that need to be removed
        for (auto exec : toRemove) {
            // Shut down the executor
            exec->shutdown();

            // Remove and erase
            auto removed = std::remove(execs.begin(), execs.end(), exec);
            execs.erase(removed, execs.end());
        }
    }

    // Remove and erase
    for (auto& key : keysToRemove) {
        SPDLOG_TRACE("Removing scheduler record for {}, no more executors",
                     key);
        executors.erase(key);
    }

    return nReaped;
}

long Scheduler::getFunctionExecutorCount(const faabric::Message& msg)
{
    faabric::util::SharedLock lock(mx);
    const std::string funcStr = faabric::util::funcParToString(msg, false);
    return executors[funcStr].size();
}

bool Scheduler::registerApp(std::unique_ptr<batch_scheduler::Application> app)
{
    SPDLOG_INFO("Scheduler registers application {}", app->getName());
    faabric::util::FullLock lock(mx);
    decentralScheduler.registerApp(std::move(app));
    return true;
}

// Enqueue the request messages from remote into local unprocess queue for
// further processing.
void Scheduler::enqueueMessageBatch(std::unique_ptr<faabric::MessageBatch> msgs)
{
    // If the scheduler is updating state information, we may need to transfer
    // them to other nodes. So, just enqueue them temporarily.
    if (isUpdateState) {
        SPDLOG_DEBUG("Enqueueing messages while updating state");
        unschedMsgs.enqueue(std::move(msgs));
        return;
    }

    // This function is called by planner and other workers. Deadlocks happens
    // if lock(mx) is required. Our BatchQueue is thread-safe.

    int nMessages = msgs->messages_size();
    auto current = faabric::util::getGlobalClock().epochMicros();
    auto currentMillis = faabric::util::getGlobalClock().epochMillis();
    auto endPoint = faabric::util::getSystemConfig().endpointHost;

    // Statistics the message enqueue count
    std::map<std::string, int> instancesCounter;

    for (int i = 0; i < nMessages; i++) {
        faabric::Message& msg = msgs->mutable_messages()->at(i);
        std::string waitingQueueName = msg.user() + "_" + msg.function() + "_" +
                                       std::to_string(msg.parallelismid());
        instancesCounter[waitingQueueName]++;
        (*msg.mutable_metricrecorder())[WORKER_ENQUEUE_TIME_KEY] = current;
        msg.set_starttimestamp(currentMillis);
        msg.set_executedhost(endPoint);

        // Add messages to the waiting queue
        // SPDLOG_DEBUG("Enqueueing messages for {}", waitingQueueName);
        int messageType = msg.messagetype();
        if (messageType == 2) {
            size_t hash = msg.hash();
            auto [iterator, inserted] = partitionedWaitingQueues.emplace(
              waitingQueueName,
              std::make_unique<faabric::util::PartitionedStateMessageQueue>(
                waitingQueueName, maxReplicas, executeBatchsize));
            int waitMsgs = iterator->second->getMessagesCount();
            (*msg.mutable_metricrecorder())[WORKER_ENQUEUE_SIZE_KEY] = waitMsgs;
            iterator->second->addMessage(
              hash, std::make_unique<faabric::Message>(std::move(msg)));
        } else {
            auto [iterator, inserted] =
              waitingQueues.emplace(waitingQueueName,
                                    std::make_unique<faabric::util::BatchQueue>(
                                      waitingQueueName, executeBatchsize));
            int waitMsgs = iterator->second->getMessagesCount();
            (*msg.mutable_metricrecorder())[WORKER_ENQUEUE_SIZE_KEY] = waitMsgs;
            iterator->second->addMessage(
              std::make_unique<faabric::Message>(std::move(msg)));
        }
    }

    // Update the instances runtime stats
    std::string invokeHost = msgs->invokehost();
    for (const auto& [instancesName, count] : instancesCounter) {
        runtimeStats.instanceAdd(instancesName, invokeHost, count);
    }

    SPDLOG_DEBUG("Enqueued {} messages completed", nMessages);
}

void Scheduler::executeBatchForQueue(const std::string& userFuncPar,
                                     util::BatchQueueBase& waitingQueue,
                                     faabric::util::FullLock& lock)
{
    auto firstMsg = waitingQueue.queueFront();
    std::string user = firstMsg->user();
    std::string func = firstMsg->function();
    std::string funcStr = faabric::util::funcParToString(*firstMsg, false);

    // Check if the executor is available.
    if (!executorAvailable(funcStr)) {
        return;
    }
    if (isUpdateState) {
        return;
    }

    // Generate new BatchExecuteRequest (A request contains multiple requests)
    auto newReq = faabric::util::batchExecFactory();
    newReq->set_user(user);
    newReq->set_function(func);

    // Execute Batch Requests until the queue is empty or no available executor.
    while (waitingQueue.getMessagesCount() != 0) {
        // The state lock is obtained before the requets are dequeued.
        auto stateLock =
          std::make_unique<faabric::util::SharedLock>(stateUpdateMx);

        // Double Check
        if (waitingQueue.getMessagesCount() == 0) {
            stateLock->unlock();
            break;
        }
        SPDLOG_DEBUG("statelock is acquired for {}", userFuncPar);
        auto msgVec = waitingQueue.getMessages();
        // int msgVecSize = msgVec.size();
        for (auto& msgPointer : msgVec) {
            auto* message = newReq->add_messages();
            *message = std::move(*msgPointer);
            long workerQueueTime =
              (*message->mutable_metricrecorder())[WORKER_ENQUEUE_TIME_KEY];
            message->mutable_metricrecorder()->erase(WORKER_ENQUEUE_TIME_KEY);
            int workerQueueWaitingTime =
              faabric::util::getGlobalClock().epochMicros() - workerQueueTime;
            // int workerQueueSize =
            //   (*message->mutable_metricrecorder())[WORKER_ENQUEUE_SIZE_KEY];
            message->mutable_metricrecorder()->erase(WORKER_ENQUEUE_SIZE_KEY);
            message->set_workerqueuewaittime(workerQueueWaitingTime);
            // Record the message waiting time in the queue while waiting for an
            // available executor. If the msgVecSize is smaller than batchsize,
            // it means this Batch is dispatched when window expired. It should
            // not be recorded.
            // if (msgVecSize >= executeBatchsize) {
            //     instancesLoadState.addWaitTime(
            //       userFuncPar, workerQueueWaitingTime, workerQueueSize);
            // }
        }
        // Claim new Executor, we can bound the first msg here, since claim
        // only needs the user and function of Message.
        faabric::Message& localMsg = newReq->mutable_messages()->at(0);
        auto timeFlag1 = faabric::util::getGlobalClock().epochMicros();
        std::shared_ptr<faabric::executor::Executor> e =
          claimExecutor(localMsg, lock);
        auto timeFlag2 = faabric::util::getGlobalClock().epochMicros();
        int elapsed = static_cast<int>(timeFlag2 - timeFlag1);
        for (int i = 0; i < newReq->messages_size(); i++) {
            newReq->mutable_messages()->at(i).set_executorpreparetime(elapsed);
        }
        SPDLOG_DEBUG("Claimed executor {} for {} with message size {}",
                     e->id,
                     userFuncPar,
                     newReq->messages_size());
        // Execute the BatchRequest
        e->executeBatchTasks(newReq, std::move(stateLock));
        // Quit if no executor is available. Otherwise, execute the next batch.
        if (!executorAvailable(funcStr)) {
            break;
        }
        if (isUpdateState) {
            return;
        }
        // Reset the newReq
        newReq = faabric::util::batchExecFactory();
        newReq->set_user(user);
        newReq->set_function(func);
    }

    if (waitingQueue.getMessagesCount() == 0) {
        waitingQueue.resetLastTime();
    }
}

void Scheduler::enqueueChainedCalls(
  std::vector<std::unique_ptr<faabric::Message>> msgs)
{
    SPDLOG_DEBUG("Enqueueing chained calls for {} messages", msgs.size());
    faabric::util::FullLock lock(chainedCallMsgsMx);

    auto currentTime = faabric::util::getGlobalClock().epochMicros();
    for (auto& msg : msgs) {
        if (msg) {
            msg->set_plannerqueuetime(currentTime);
            chainedCallMsgs.emplace_back(std::move(msg));
        }
    }
    msgs.clear();
    SPDLOG_DEBUG("Enqueueing chained calls finished");
}

void Scheduler::enqueueSetResults(
  std::shared_ptr<faabric::BatchExecuteRequest> req)
{
    SPDLOG_DEBUG("Enqueueing set results for {} messages",
                 req->messages_size());
    faabric::util::FullLock lock(setResultMsgsMx);

    for (int i = 0; i < req->messages_size(); i++) {
        faabric::Message& msg = req->mutable_messages()->at(i);
        setResultMsgs.emplace_back(std::make_unique<faabric::Message>(msg));
    }
    SPDLOG_DEBUG("Enqueueing set results finished");
}

void Scheduler::batchTimerCheck()
{
    while (!stopBatchTimer) {
        std::this_thread::sleep_for(
          std::chrono::milliseconds(conf.batchCheckInterval));

        // SPDLOG_DEBUG("batchTimerCheck: trying to acquire mx lock");
        faabric::util::FullLock lock(mx);
        // SPDLOG_DEBUG("batchTimerCheck: acquired mx lock");

        if (stopBatchTimer) {
            break;
        }

        // If we have some unScheduled messages, schedule them.
        while (!unschedMsgs.empty()) {
            SPDLOG_DEBUG("Processing unscheduled messages");
            auto msgs = unschedMsgs.dequeue()->messages();
            std::vector<std::unique_ptr<faabric::Message>> msgsVec;
            msgsVec.reserve(msgs.size());
            for (auto& msg : msgs) {
                msgsVec.push_back(std::make_unique<faabric::Message>(msg));
            }
            auto hosts =
              decentralScheduler.scheduleMessagesBatch(hostMap, msgsVec);
            enqueueSchedMsgs(hosts, std::move(msgsVec));
        }

        // long totalWaitingMessages = 0;
        // for (auto const& [userFuncPar, waitingBatch] : waitingQueues) {
        //     totalWaitingMessages += waitingBatch->getMessagesCount();
        // }
        // SPDLOG_DEBUG(
        //   "batchTimerCheck: Checking waitingQueues. Total messages: {}",
        //   totalWaitingMessages);

        for (auto& [userFuncPar, waitingBatch] : waitingQueues) {
            if (waitingBatch->getMessagesCount() == 0) {
                continue;
            }
            if (waitingBatch->getMessagesCount() >= executeBatchsize ||
                waitingBatch->getTimeInterval() >= conf.batchInterval) {
                executeBatchForQueue(userFuncPar, *waitingBatch, lock);
            }
        }

        // NEW DEBUGGING CODE
        // ==================================
        // long totalPartitionedMessages = 0;
        // for (auto const& [userFuncPar, waitingBatch] :
        //      partitionedWaitingQueues) {
        //     totalPartitionedMessages += waitingBatch->getMessagesCount();
        // }
        // SPDLOG_DEBUG("batchTimerCheck: Checking partitionedWaitingQueues. "
        //              "Total messages: {}",
        //              totalPartitionedMessages);
        // ==================================

        // if Repartitioned, parititioned state functions are in the
        // partitionedWaitingQueues.
        for (auto& [userFuncPar, waitingBatch] : partitionedWaitingQueues) {
            if (waitingBatch->getMessagesCount() == 0) {
                continue;
            }
            if (waitingBatch->getMessagesCount() >= executeBatchsize ||
                waitingBatch->getTimeInterval() >= conf.batchInterval) {
                executeBatchForQueue(userFuncPar, *waitingBatch, lock);
            }
        }

        // SPDLOG_DEBUG("batchTimerCheck: send message results back");

        auto currentMillis = faabric::util::getGlobalClock().epochMillis();

        if (currentMillis - lastPlannerCallCheck < plannerCallInterval) {
            continue;
        }
        lastPlannerCallCheck = currentMillis;
        auto& plannerCli = faabric::planner::getPlannerClient();

        faabric::util::FullLock setResultMsgsLock(setResultMsgsMx);
        if (!setResultMsgs.empty()) {
            auto req = faabric::util::batchExecFactory("FAASM", "Func", 0);
            for (auto& msg : setResultMsgs) {
                auto* message = req->add_messages();
                *message = std::move(*msg);
            }
            SPDLOG_DEBUG("Set result batch size: {}", req->messages_size());
            plannerCli.setMessageResultBatch(req);
            setResultMsgs.clear();
        }
        setResultMsgsLock.unlock();

        // SPDLOG_DEBUG("batchTimerCheck: finished");
    }
}

void Scheduler::enqueueSchedMsgs(
  std::vector<std::string> hosts,
  std::vector<std::unique_ptr<faabric::Message>> msgs)
{
    auto currentTime = faabric::util::getGlobalClock().epochMicros();
    faabric::util::FullLock lock(scheduledMsgsMapMx);
    for (int i = 0; i < msgs.size(); i++) {
        auto msg = std::move(msgs[i]);
        auto host = hosts[i];
        msg->set_plannerpoptime(currentTime);
        scheduledMsgsMap[host].push_back(std::move(msg));
    }
}

void Scheduler::dispatchChainedMsgs()
{
    while (!stopThreadTimer) {
        // Sleep for a while to batch the scheduled requests
        std::this_thread::sleep_for(std::chrono::milliseconds(dispatchPeriod));
        // Lock only for copying and clearing `scheduledMsgsMap`

        // SPDLOG_DEBUG("dispatchChainedMsgs: trying to acquire mx lock");
        faabric::util::FullLock mxLock(mx);
        // SPDLOG_DEBUG("dispatchChainedMsgs: acquired mx lock");

        if (stopThreadTimer) {
            break;
        }

        // if scheduleMode is 2 (centralized), we need to transfer the chained
        // calls to planner
        if (scheduleMode == 2) {
            auto& plannerCli = faabric::planner::getPlannerClient();

            faabric::util::FullLock chainedCallLock(chainedCallMsgsMx);
            if (chainedCallMsgs.empty()) {
                chainedCallLock.unlock();
                continue;
            }
            auto req = faabric::util::batchExecFactory("FAASM", "Func", 0);
            for (auto& msg : chainedCallMsgs) {
                auto* message = req->add_messages();
                *message = std::move(*msg);
            }
            SPDLOG_DEBUG("Chaining call batch size: {}", req->messages_size());
            plannerCli.enqueueFunctions(req);
            chainedCallMsgs.clear();
            SPDLOG_DEBUG("Chaining call batch completed");
            continue;
        }

        // Otherwise, decentralized scheduler is used

        // Schedule the chained calls
        // MAP<instanceName, <host, count>>
        std::map<std::string, std::map<std::string, int>> chainedCallsCounter;

        std::vector<std::unique_ptr<faabric::Message>> localChainedCallMsgs;
        {
            faabric::util::FullLock chainedCallLock(chainedCallMsgsMx);
            if (!chainedCallMsgs.empty()) {
                localChainedCallMsgs = std::move(chainedCallMsgs);
                chainedCallMsgs.clear(); // just in case
            }
        }
        if (!localChainedCallMsgs.empty()) {
            auto hosts = decentralScheduler.scheduleMessagesBatch(
              hostMap, localChainedCallMsgs);
            // Statistics the chained calls
            for (int i = 0; i < localChainedCallMsgs.size(); i++) {
                auto msg = localChainedCallMsgs[i].get();
                std::string userFuncPar = faabric::util::getUserFuncPar(*msg);
                chainedCallsCounter[userFuncPar][hosts[i]]++;
            }
            enqueueSchedMsgs(hosts, std::move(localChainedCallMsgs));
        }

        for (auto& [instancesName, hostCounter] : chainedCallsCounter) {
            for (auto& [host, count] : hostCounter) {
                runtimeStats.instanceGenerate(instancesName, host, count);
            }
        }

        faabric::util::FullLock lock(scheduledMsgsMapMx);
        if (scheduledMsgsMap.empty()) {
            lock.unlock();
            continue;
        }

        // Create a local filtered copy of scheduledRequestsMap
        auto currentTime = faabric::util::getGlobalClock().epochMicros();
        std::map<std::string, std::list<std::unique_ptr<faabric::Message>>>
          msgsCallMap;
        for (auto& [hostIp, msgsList] : scheduledMsgsMap) {
            if (msgsList.empty()) {
                continue;
            }
            for (auto& msg : msgsList) {
                msg->set_plannerdispatchtime(currentTime);
            }
            msgsCallMap[hostIp] = std::move(msgsList); // Move ownership
        }
        scheduledMsgsMap.clear();

        // Parallel execution of function calls for each host
        std::vector<std::thread> threads;
        for (auto& [hostIp, msgs] : msgsCallMap) {
            // SPDLOG_DEBUG the hosts and messages
            SPDLOG_DEBUG("Dispatching messages to host {} with message size {}",
                         hostIp,
                         msgs.size());
            // If locally, we put the messages into a batch directly
            if (hostIp == thisHost) {
                auto batchMsgs = std::make_unique<faabric::MessageBatch>();
                batchMsgs->set_invokehost(thisHost);
                SPDLOG_DEBUG("Batch execute {} locally with Batch size: {}",
                             thisHost,
                             msgs.size());
                for (auto& msg : msgs) {
                    batchMsgs->add_messages()->CopyFrom(*msg);
                }
                threads.emplace_back(
                  [this, batch = std::move(batchMsgs)]() mutable {
                      enqueueMessageBatch(std::move(batch));
                  });
                continue;
            }
            // Otherwise, we send the messages to the remote host
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

void Scheduler::resetParameter(std::string key, int32_t value)
{
    faabric::util::FullLock lock(mx);
    if (key == "max_executors") {
        maxExecutors = value;
        SPDLOG_INFO("Reset maxExecutors parameter to : {}", maxExecutors);
    } else if (key == "planner_call_interval") {
        plannerCallInterval = value;
        SPDLOG_INFO("Reset plannerCallInterval parameter to : {}",
                    plannerCallInterval);
    } else if (key == "max_replicas") {
        maxReplicas = value;
        SPDLOG_INFO("Reset maxReplicas parameter to : {}", maxReplicas);
    } else if (key == "batch_size") {
        executeBatchsize = value;
        // change the batch size of all waiting queues
        for (auto& [userFuncPar, waitingBatch] : waitingQueues) {
            waitingBatch->resetBatchSize(executeBatchsize);
        }
        for (auto& [userFuncPar, waitingBatch] : partitionedWaitingQueues) {
            waitingBatch->resetBatchSize(executeBatchsize);
        }
        SPDLOG_INFO("Reset executeBatchsize parameter to : {}",
                    executeBatchsize);
    } else if (key == "schedule_mode") {
        decentralScheduler.setScheduleMode(value);
        scheduleMode = value;
        SPDLOG_INFO("Reset schedule_mode parameter to : {}", value);
    } else {
        throw std::runtime_error(
          fmt::format("Unrecognized parameter key: {}", key));
    }
}

void Scheduler::clearRecordedMessages()
{
    faabric::util::FullLock lock(mx);
    recordedMessages.clear();
}

std::vector<faabric::Message> Scheduler::getRecordedMessages()
{
    faabric::util::SharedLock lock(mx);
    return recordedMessages;
}

bool Scheduler::executorAvailable(const std::string& funcStr)
{
    auto& thisExecutors = executors[funcStr];
    SPDLOG_TRACE(
      "Checking if executor is available for {}, current executor size {}",
      funcStr,
      thisExecutors.size());
    // If we can reuse warm executors, we can return true.
    for (auto& e : thisExecutors) {
        if (e->availableClaim()) {
            SPDLOG_TRACE("Available executor {} for {}", e->id, funcStr);
            return true;
        }
    }
    // We don't have to check the maxExecutors here, since in updateStatesInfo,
    // max replicas are limited. Total number of executors won't exceed the
    // maxExecutors. If current current replicas is less than the max size,
    // return true.
    if (thisExecutors.size() < maxReplicas) {
        return true;
    } else {
        SPDLOG_DEBUG("No available executor for {}: max replicas for function "
                     "reached ({} >= {})",
                     funcStr,
                     thisExecutors.size(),
                     maxReplicas);
    }

    return false;
}

std::shared_ptr<faabric::executor::Executor> Scheduler::claimExecutor(
  faabric::Message& msg,
  faabric::util::FullLock& schedulerLock)
{
    std::string funcStr = faabric::util::funcParToString(msg, false);

    std::vector<std::shared_ptr<faabric::executor::Executor>>& thisExecutors =
      executors[funcStr];

    auto factory = faabric::executor::getExecutorFactory();

    std::shared_ptr<faabric::executor::Executor> claimed = nullptr;
    for (auto& e : thisExecutors) {
        if (e->tryClaim()) {
            claimed = e;
            // Reset the just claimed warm executor to guarantee TLS is
            // refreshed
            claimed->reset(msg);
            SPDLOG_DEBUG(
              "Reusing warm executor {} for {}", claimed->id, funcStr);
            break;
        }
    }

    // We have no warm executors available, so scale up
    if (claimed == nullptr) {
        SPDLOG_DEBUG("Scaling {} from {} -> {}",
                     funcStr,
                     thisExecutors.size(),
                     thisExecutors.size() + 1);

        // Spinning up a new executor can be lengthy, allow other things
        // to run in parallel
        schedulerLock.unlock();
        auto executor = factory->createExecutor(msg);
        schedulerLock.lock();
        thisExecutors.push_back(std::move(executor));
        claimed = thisExecutors.back();

        // Claim it
        claimed->tryClaim();
    }
    assert(claimed != nullptr);
    return claimed;
}

// std::string Scheduler::getThisHost()
// {
//     faabric::util::SharedLock lock(mx);
//     return thisHost;
// }

void Scheduler::setThreadResultLocally(uint32_t appId,
                                       uint32_t msgId,
                                       int32_t returnValue,
                                       faabric::transport::Message& message)
{
    // Keep the message
    faabric::util::FullLock lock(mx);
    threadResultMessages.insert(std::make_pair(msgId, std::move(message)));
}

// TODO(scheduler-cleanup): move method elsewhere
std::vector<std::pair<uint32_t, int32_t>> Scheduler::awaitThreadResults(
  std::shared_ptr<faabric::BatchExecuteRequest> req,
  int timeoutMs)
{
    std::vector<std::pair<uint32_t, int32_t>> results;
    results.reserve(req->messages_size());
    for (int i = 0; i < req->messages_size(); i++) {
        uint32_t messageId = req->messages().at(i).id();

        auto msgResult = faabric::planner::getPlannerClient().getMessageResult(
          req->appid(), messageId, timeoutMs);
        results.emplace_back(messageId, msgResult.returnvalue());
    }

    return results;
}

size_t Scheduler::getCachedMessageCount()
{
    return threadResultMessages.size();
}

void Scheduler::setThisHostResources(faabric::HostResources& res)
{
    addHostToGlobalSet(thisHost, std::make_shared<faabric::HostResources>(res));
    conf.overrideCpuCount = res.slots();
}

// --------------------------------------------
// EXECUTION GRAPH
// --------------------------------------------

#define CHAINED_SET_PREFIX "chained_"
std::string getChainedKey(unsigned int msgId)
{
    return std::string(CHAINED_SET_PREFIX) + std::to_string(msgId);
}

// ----------------------------------------
// MIGRATION
// ----------------------------------------

std::shared_ptr<faabric::PendingMigration>
Scheduler::checkForMigrationOpportunities(faabric::Message& msg,
                                          int overwriteNewGroupId)
{
    int appId = msg.appid();
    int groupId = msg.groupid();
    int groupIdx = msg.groupidx();
    SPDLOG_DEBUG("Message {}:{}:{} checking for migration opportunities",
                 appId,
                 groupId,
                 groupIdx);

    // TODO: maybe we could move this into a broker-specific function?
    int newGroupId = 0;
    if (groupIdx == 0) {
        // To check for migration opportunities, we request a scheduling
        // decision for the same batch execute request, but setting the
        // migration flag
        auto req =
          faabric::util::batchExecFactory(msg.user(), msg.function(), 1);
        faabric::util::updateBatchExecAppId(req, msg.appid());
        faabric::util::updateBatchExecGroupId(req, msg.groupid());
        req->set_type(faabric::BatchExecuteRequest::MIGRATION);
        auto decision = planner::getPlannerClient().callFunctions(req);

        // Update the group ID if we want to migrate
        if (decision == DO_NOT_MIGRATE_DECISION) {
            newGroupId = groupId;
        } else {
            newGroupId = decision.groupId;
        }

        // Send the new group id to all the members of the group
        auto groupIdxs = broker.getIdxsRegisteredForGroup(groupId);
        groupIdxs.erase(0);
        for (const auto& recvIdx : groupIdxs) {
            broker.sendMessage(
              groupId, 0, recvIdx, BYTES_CONST(&newGroupId), sizeof(int));
        }
    } else if (overwriteNewGroupId == 0) {
        std::vector<uint8_t> bytes = broker.recvMessage(groupId, 0, groupIdx);
        newGroupId = faabric::util::bytesToInt(bytes);
    } else {
        // In some settings, like tests, we already know the new group id, so
        // we can set it here (and in fact, we need to do so when faking two
        // hosts)
        newGroupId = overwriteNewGroupId;
    }

    bool appMustMigrate = newGroupId != groupId;
    if (!appMustMigrate) {
        return nullptr;
    }

    msg.set_groupid(newGroupId);
    broker.waitForMappingsOnThisHost(newGroupId);
    std::string newHost = broker.getHostForReceiver(newGroupId, groupIdx);

    auto migration = std::make_shared<faabric::PendingMigration>();
    migration->set_appid(appId);
    migration->set_groupid(newGroupId);
    migration->set_groupidx(groupIdx);
    migration->set_srchost(thisHost);
    migration->set_dsthost(newHost);

    return migration;
}

// ----------------------------------
// Status Collection
// ----------------------------------
int Scheduler::getMonitoredInfoTest()
{
    return 0;
}

void Scheduler::updateHosts(const std::vector<std::string>& hosts)
{
    SPDLOG_DEBUG("updateHosts: Updating hosts with {} entries", hosts.size());
    SPDLOG_DEBUG("updateHosts: trying to acquire mx lock");
    faabric::util::FullLock lock(mx);
    SPDLOG_DEBUG("updateHosts: acquired mx lock");
    if (hosts.empty()) {
        SPDLOG_ERROR("No hosts provided to updateHosts");
        throw std::runtime_error("No hosts provided to updateHosts");
        return;
    }
    registeredHostsMap.clear();
    for (const auto& host : hosts) {
        registeredHostsMap.emplace(host, host);
    }

    std::ostringstream oss;
    oss << "Registered hosts:\n";
    for (const auto& [key, value] : registeredHostsMap) {
        oss << key << "=" << value << ";\n";
    }
    SPDLOG_INFO("updateHosts: {}", oss.str());

    hostMap = convertHostMap(registeredHostsMap);
}

void Scheduler::storeMigrateState(
  std::multimap<std::string, std::string>&& migrateState)
{
    faabric::util::FullLock lock(tempMigrateStateMapMx);
    tempMigrateStateMap.merge(migrateState);
}

void Scheduler::updateStatesInfo(
  const std::map<std::string, faabric::batch_scheduler::ScheduledOperator>&
    scheuduledOperatorMap,
  const std::map<std::string, faabric::batch_scheduler::FunctionStateInfo>&
    statesInfo)
{
    SPDLOG_DEBUG("updateStatesInfo: Update states info starts");
    isUpdateState = true;
    // Update the states info in decentralized scheduler
    // To update states, we must ensure that no executors are running.
    // Otherwise, it might cannot update the states correctly.
    faabric::util::FullLock lock(mx);
    SPDLOG_DEBUG("updateStatesInfo: scheduler lock acquired");
    faabric::util::FullLock stateLock(stateUpdateMx);
    SPDLOG_DEBUG("updateStatesInfo: state lock acquired");

    // reset max replicas based on number of instances assigned loccally.
    int localInstanceCount = 0;
    for (const auto& [operatorName, operatorInfo] : scheuduledOperatorMap) {
        if (operatorInfo.node.type == faabric::batch_scheduler::STATELESS) {
            // For distributed scheduler (default) and centralized scheduler,
            // stateless operator are distributed in RB across all hosts.
            if (scheduleMode == 1 || scheduleMode == 2) {
                localInstanceCount++;
            } else if (operatorInfo.weightDist.count(thisHost) > 0) {
                localInstanceCount++;
            }
        } else {
            for (const auto& [_, ip] : operatorInfo.parallelismDist) {
                if (ip == thisHost) {
                    localInstanceCount++;
                }
            }
        }
    }
    if (localInstanceCount == 0) {
        localInstanceCount = maxExecutors;
    }
    maxReplicas = maxExecutors / localInstanceCount;
    SPDLOG_INFO("updateStatesInfo: localInstanceCount is {}, maxReplicas is "
                "{}, maxExecutors is {}",
                localInstanceCount,
                maxReplicas,
                maxExecutors);
    if (maxReplicas < 1) {
        SPDLOG_ERROR("maxReplicas is less than 1, too many instances");
        throw std::runtime_error("maxReplicas is less than 1");
    }

    decentralScheduler.setScheuduledOperatorMap(scheuduledOperatorMap);

    // Update the states info in decentralized scheduler
    decentralScheduler.syncStatesInfo(statesInfo);

    // We migrate the old state and create the new state according to the
    // planner's new scheduling decision.
    auto& stateServer = faabric::state::getGlobalState();

    stateServer.backupAll();
    auto& hashRings = decentralScheduler.getStateHashRing();
    auto migrationStatesMap =
      stateServer.schedulePreStates(hashRings, statesInfo);

    // Migrate the states.
    SPDLOG_INFO("updateStatesInfo: transferring states to other hosts");
    std::vector<std::thread> threads;
    for (const auto& [ip, migrStates] : migrationStatesMap) {
        auto req = std::make_shared<faabric::StateMigrationRequest>();
        for (const auto& [userFuncPar, serializedState] : migrStates) {
            auto* migrateState = req->add_migratestates();
            migrateState->set_userfuncpar(userFuncPar);
            migrateState->set_serializedstate(serializedState);
        }
        threads.emplace_back(
          [ip, req]() { getFunctionCallClient(ip)->migrateStates(req); });
    }
    for (auto& t : threads) {
        if (t.joinable()) {
            t.join();
        }
    }

    SPDLOG_INFO(
      "updateStatesInfo: states update complete, reallocate states now");

    // Intialize new allocated state
    for (const auto& [stateKey, stateInfo] : statesInfo) {
        for (const auto& [id, ip] : stateInfo.stateHost) {
            if (ip != thisHost) {
                continue;
            }
            auto [user, func] = faabric::util::splitUserFunc(stateKey);
            bool isPartitionable =
              stateInfo.partitionBy != "" && stateInfo.partitionBy != "None";

            // Create the state locally if the state is on this host
            stateServer.createFS(user, func, id, isPartitionable);
        }
    }

    SPDLOG_INFO(
      "updateStatesInfo: create states complete, waiting for other hosts");

    // Hurdle - waiting for other hosts to finish the migration
    faabric::planner::getPlannerClient().migrationComplete();

    SPDLOG_INFO("updateStatesInfo: migration complete, update the states now");

    // Update the states from tempMigrateStateMap to state
    {
        faabric::util::FullLock migrateStateLock(tempMigrateStateMapMx);
        std::ostringstream oss;
        oss << "tempMigrateStateMap contents:\n";
        for (const auto& [key, value] : tempMigrateStateMap) {
            oss << "Key: " << key << " -> Value Size: " << value.size() << "\n";
        }
        SPDLOG_INFO("{}", oss.str());
        stateServer.loadMigrateState(tempMigrateStateMap);

        tempMigrateStateMap.clear();
        stateServer.cleanBackup();
    }

    SPDLOG_INFO("updateStatesInfo: states reallocation complete, reschedule "
                "requests now");

    // Reschedule all the messages in unprocessed queue and scheduled queue.
    std::vector<std::unique_ptr<faabric::Message>> rescheduleMsgs;
    for (auto& [queueKey, queuePtr] : waitingQueues) {
        if (queuePtr->getMessagesCount() == 0) {
            continue;
        }
        auto messages = queuePtr->drainMessages();
        for (auto& msg : messages) {
            rescheduleMsgs.emplace_back(std::move(msg));
        }
    }
    for (auto& [queueKey, queuePtr] : partitionedWaitingQueues) {
        if (queuePtr->getMessagesCount() == 0) {
            continue;
        }
        auto messages = queuePtr->drainMessages();
        for (auto& msg : messages) {
            rescheduleMsgs.emplace_back(std::move(msg));
        }
    }
    // Reschedule the scheduled messages in scheduledMsgsMap
    for (auto& [hostIp, msgs] : scheduledMsgsMap) {
        for (auto& msg : msgs) {
            rescheduleMsgs.emplace_back(std::move(msg));
        }
        scheduledMsgsMap.erase(hostIp);
    }
    // Reschedule the messages
    if (!rescheduleMsgs.empty()) {
        auto hosts =
          decentralScheduler.scheduleMessagesBatch(hostMap, rescheduleMsgs);
        enqueueSchedMsgs(hosts, std::move(rescheduleMsgs));
    }
    isUpdateState = false;
    SPDLOG_INFO("updateStatesInfo: reschedule complete");
}

std::map<std::string, InstanceStatsResult> Scheduler::getRuntimeStats()
{
    return runtimeStats.getAllStats();
}

void Scheduler::updateStatelessDist(
  const std::map<std::string, std::map<std::string, int>>& sourceCountStats)
{
    // TODO - update the source.
    decentralScheduler.runtimeSourceUpdate(sourceCountStats);
}

void Scheduler::setLocalPersistentState(
  const std::map<std::string, std::string>& kvMap)
{
    SPDLOG_INFO("Setting local persistent state");
    faabric::state::getGlobalState().writePersistentStateBatch(kvMap);
    SPDLOG_INFO("Local persistent state set successfully");
}

}
