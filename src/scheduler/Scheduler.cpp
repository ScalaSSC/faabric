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
#include <faabric/util/testing.h>

#include <unordered_set>

using namespace faabric::util;
using namespace faabric::snapshot;

namespace faabric::scheduler {

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
{
    executeBatchsize = conf.batchSize;
    // Start the reaper thread
    reaperThread.start(conf.reaperIntervalSeconds);
    batchTimerThread = std::thread(&Scheduler::batchTimerCheck, this);
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

    int plannerTimeout = faabric::planner::getPlannerClient().registerHost(req);

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

void Scheduler::executeBatch(std::shared_ptr<faabric::BatchExecuteRequest> req)
{
    faabric::util::FullLock lock(mx);

    bool isThreads = req->type() == faabric::BatchExecuteRequest::THREADS;
    auto funcStr = faabric::util::funcParToString(req->messages(0), false);
    int nMessages = req->messages_size();

    // Records for tests - copy messages before execution to avoid races
    if (faabric::util::isTestMode()) {
        for (int i = 0; i < nMessages; i++) {
            recordedMessages.emplace_back(req->messages().at(i));
        }
    }

    // For threads we only need one executor, for anything else we want
    // one Executor per function in flight.
    if (isThreads) {
        // Threads use the existing executor. We assume there's only
        // one running at a time.
        std::vector<std::shared_ptr<faabric::executor::Executor>>&
          thisExecutors = executors[funcStr];

        std::shared_ptr<faabric::executor::Executor> e = nullptr;
        if (thisExecutors.empty()) {
            // Create executor if not exists
            e = claimExecutor(*req->mutable_messages(0), lock);
        } else if (thisExecutors.size() == 1) {
            // Use existing executor if exists
            e = thisExecutors.back();
        } else {
            SPDLOG_ERROR("Found {} executors for threaded function {}",
                         thisExecutors.size(),
                         funcStr);
            throw std::runtime_error(
              "Expected only one executor for threaded function");
        }

        assert(e != nullptr);

        // Execute the tasks
        std::vector<int> thisHostIdxs(req->messages_size());
        std::iota(thisHostIdxs.begin(), thisHostIdxs.end(), 0);
        e->executeTasks(thisHostIdxs, req);
    } else {
        // Non-threads require one executor per task
        for (int i = 0; i < nMessages; i++) {
            faabric::Message& localMsg = req->mutable_messages()->at(i);

            std::shared_ptr<faabric::executor::Executor> e =
              claimExecutor(localMsg, lock);
            e->executeTasks({ i }, req);
        }
    }
}

void Scheduler::enqueueMessageBatch(std::shared_ptr<faabric::MessageBatch> msgs)
{
    faabric::util::FullLock lock(mx);

    auto current = faabric::util::getGlobalClock().epochMicros();
    auto currentMillis = faabric::util::getGlobalClock().epochMillis();
    auto endPoint = faabric::util::getSystemConfig().endpointHost;
    for (int i = 0; i < msgs->messages_size(); i++) {
        faabric::Message& msg = msgs->mutable_messages()->at(i);
        std::string waitingQueueName = msg.user() + "_" + msg.function() + "_" +
                                       std::to_string(msg.parallelismid());
        msg.set_workerqueuetime(current);
        msg.set_starttimestamp(currentMillis);
        msg.set_executedhost(endPoint);

        size_t hash = msg.hash();
        int messageType = msg.messagetype();
        if (isRepartition && messageType == 2) {
            if (!partitionedWaitingQueues.contains(waitingQueueName)) {
                faabric::util::PartitionedStateMessageQueue tempWaitingQueue(
                  maxReplicas, executeBatchsize);
                partitionedWaitingQueues.emplace(waitingQueueName,
                                                 tempWaitingQueue);
            }
            partitionedWaitingQueues.at(waitingQueueName)
              .addMessage(hash,
                          std::make_shared<faabric::Message>(std::move(msg)));
        } else {
            auto [iterator, inserted] =
              waitingQueues.emplace(waitingQueueName, waitingQueueName);
            iterator->second.insertMsg(
              std::make_shared<faabric::Message>(std::move(msg)));
        }
    }
}

void Scheduler::executeBatchLazy(
  std::shared_ptr<faabric::BatchExecuteRequest> req)
{
    faabric::util::FullLock lock(mx);

    bool isThreads = req->type() == faabric::BatchExecuteRequest::THREADS;
    int nMessages = req->messages_size();

    // Records for tests - copy messages before execution to avoid races
    if (faabric::util::isTestMode()) {
        for (int i = 0; i < nMessages; i++) {
            recordedMessages.emplace_back(req->messages().at(i));
        }
    }

    // We don't support THREADS now, Since, I don't know how it works :).
    if (isThreads) {
        SPDLOG_ERROR("THREADS is not supported in callBatchFunctions");
        throw std::runtime_error(
          "THREADS is not supported in callBatchFunctions");
    }

    // A request only contains the same function with same parallelism(first
    // tier partition)
    auto firstMsg =
      std::make_shared<faabric::Message>(*req->mutable_messages(0));
    std::string userFuncPar = firstMsg->user() + "_" + firstMsg->function() +
                              "_" + std::to_string(firstMsg->parallelismid());
    for (size_t i = 0; i < nMessages; i++) {
        std::string waitingQueueName = userFuncPar;
        std::shared_ptr<faabric::Message> tempMsg =
          std::make_shared<faabric::Message>(*req->mutable_messages(i));
        // Set the queue start time here.
        tempMsg->set_workerqueuetime(
          faabric::util::getGlobalClock().epochMicros());

        // TODO - Refine the hashing key, after mod by first tier, second tier
        // keys are shown in some pattern.
        // Statistic the Hashing key distribution.
        size_t hash = tempMsg->hash();
        int messageType = tempMsg->messagetype();
        if (isRepartition && messageType == 2) {
            if (!partitionedWaitingQueues.contains(waitingQueueName)) {
                faabric::util::PartitionedStateMessageQueue tempWaitingQueue(
                  maxReplicas, executeBatchsize);
                partitionedWaitingQueues.emplace(waitingQueueName,
                                                 tempWaitingQueue);
            }
            partitionedWaitingQueues.at(waitingQueueName)
              .addMessage(hash, std::move(tempMsg));
        } else {
            auto [iterator, inserted] =
              waitingQueues.emplace(waitingQueueName, waitingQueueName);
            iterator->second.insertMsg(std::move(tempMsg));
        }
    }
}

void Scheduler::executeBatchForQueue(const std::string& userFuncPar,
                                     BatchQueue& waitingBatch,
                                     faabric::util::FullLock& lock)
{
    auto fisrtMsg = waitingBatch.batchQueue.front();
    std::string funcStr = faabric::util::funcParToString(*fisrtMsg, false);
    // Check if the executor is available.
    if (!executorAvailable(funcStr)) {
        return;
    }
    // Generate new BatchExecuteRequest
    auto newReq = faabric::util::batchExecFactory();
    newReq->set_user(fisrtMsg->user());
    newReq->set_function(fisrtMsg->function());
    while (!waitingBatch.batchQueue.empty()) {
        auto* message = newReq->add_messages();
        *message = std::move(*waitingBatch.batchQueue.front());
        message->set_workerpoptime(
          faabric::util::getGlobalClock().epochMicros());
        waitingBatch.batchQueue.pop();
        // If the batch size is reached, execute it.
        if (newReq->messages_size() >= executeBatchsize ||
            waitingBatch.batchQueue.empty()) {
            // Claim new Executor, we can bound the first msg here, since claim
            // only needs the user and function of Message.
            faabric::Message& localMsg = newReq->mutable_messages()->at(0);
            // Record the time for claim executor.
            auto timeFlag1 = faabric::util::getGlobalClock().epochMicros();
            std::shared_ptr<faabric::executor::Executor> e =
              claimExecutor(localMsg, lock);
            auto timeFlag2 = faabric::util::getGlobalClock().epochMicros();
            int elapsed = static_cast<int>(timeFlag2 - timeFlag1);
            for (int i = 0; i < newReq->messages_size(); i++) {
                newReq->mutable_messages()->at(i).set_executorpreparetime(
                  elapsed);
            }

            SPDLOG_DEBUG("Claimed executor {} for {} with message size {}",
                         e->id,
                         userFuncPar,
                         newReq->messages_size());
            // Execute the tasks for Paritioned State
            if (localMsg.messagetype() == 2) {
                std::string lockHint =
                  std::to_string(waitingBatch.lockVersion) + "/" +
                  std::to_string(waitingBatch.rangeStart) + "/" +
                  std::to_string(waitingBatch.rangeEnd);
                newReq->set_lockhint(lockHint);
            }
            e->executeBatchTasks(newReq);
            // Check if the executor is available.
            if (!executorAvailable(funcStr)) {
                break;
            }
            // Reset the newReq
            newReq = faabric::util::batchExecFactory();
            newReq->set_user(fisrtMsg->user());
            newReq->set_function(fisrtMsg->function());
        }
    }
    // Reset the lastTime. It will be updated when inserted next time.
    // We reset here just in case.
    if (waitingBatch.batchQueue.empty()) {
        waitingBatch.resetlastTime();
    }
}

void Scheduler::executeBatchForPartitionQueue(
  const std::string& userFuncPar,
  faabric::util::PartitionedStateMessageQueue& waitingBatch,
  faabric::util::FullLock& lock)
{
    auto fisrtMsg = waitingBatch.front();
    std::string funcStr = faabric::util::funcParToString(*fisrtMsg, false);
    // Check if the executor is available.
    if (!executorAvailable(funcStr)) {
        return;
    }
    // Generate new BatchExecuteRequest
    auto newReq = faabric::util::batchExecFactory();
    newReq->set_user(fisrtMsg->user());
    newReq->set_function(fisrtMsg->function());
    while (waitingBatch.getMessagesCount() != 0) {
        auto msgVec = waitingBatch.getMessages();
        for (auto& msgPointer : msgVec) {
            auto* message = newReq->add_messages();
            *message = std::move(*msgPointer);
            message->set_workerpoptime(
              faabric::util::getGlobalClock().epochMicros());
        }
        // Claim new Executor, we can bound the first msg here, since claim
        // only needs the user and function of Message.
        faabric::Message& localMsg = newReq->mutable_messages()->at(0);
        // Record the time for claim executor.
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
        // Execute the tasks for Paritioned State
        e->executeBatchTasks(newReq);
        // Check if the executor is available.
        if (!executorAvailable(funcStr)) {
            break;
        }
        // Reset the newReq
        newReq = faabric::util::batchExecFactory();
        newReq->set_user(fisrtMsg->user());
        newReq->set_function(fisrtMsg->function());
    }
    // Reset the lastTime. It will be updated when inserted next time.
    // We reset here just in case.
    if (waitingBatch.getMessagesCount() == 0) {
        waitingBatch.resetlastTime();
    }
}

void Scheduler::batchTimerCheck()
{
    while (!stopBatchTimer) {
        std::this_thread::sleep_for(
          std::chrono::milliseconds(conf.batchCheckInterval));

        faabric::util::FullLock lock(mx);
        for (auto& [userFuncPar, waitingBatch] : waitingQueues) {
            if (waitingBatch.batchQueue.size() == 0) {
                continue;
            }
            if (waitingBatch.batchQueue.size() >= executeBatchsize ||
                waitingBatch.getTimeInterval() >= conf.batchInterval) {
                executeBatchForQueue(userFuncPar, waitingBatch, lock);
            }
        }

        if (!isRepartition) {
            continue;
        }

        // if Repartitioned, parititioned state functions are in the
        // partitionedWaitingQueues.
        for (auto& [userFuncPar, waitingBatch] : partitionedWaitingQueues) {
            if (waitingBatch.getMessagesCount() == 0) {
                continue;
            }
            if (waitingBatch.getMessagesCount() >= executeBatchsize ||
                waitingBatch.getTimeInterval() >= conf.batchInterval) {
                executeBatchForPartitionQueue(userFuncPar, waitingBatch, lock);
            }
        }
    }
}

void Scheduler::resetParameter(std::string key, int32_t value)
{
    faabric::util::FullLock lock(mx);
    if (key == "is_repartition") {
        isRepartition = value == 1;
        SPDLOG_INFO("Reset isRepartition parameter to : {}", isRepartition);
    } else if (key == "max_executors") {
        maxExecutors = value;
        SPDLOG_INFO("Reset maxExecutors parameter to : {}", maxExecutors);
    } else {
        throw std::runtime_error(
          fmt::format("Unrecognized parameter key: {}", key));
    }
}

void Scheduler::resetBatchsize(int32_t newSize)
{
    faabric::util::FullLock lock(mx);
    executeBatchsize = newSize;
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

void Scheduler::resetMaxReplicas(int32_t newMaxReplicas)
{
    faabric::util::FullLock lock(mx);
    maxReplicas = newMaxReplicas;
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
    // If current current replicas is less than the max size.
    if (thisExecutors.size() < maxReplicas) {
        // If we have enough slots, we can return true.
        int totalExecutors = 0;
        for (const auto& pair : executors) {
            totalExecutors += pair.second.size();
        }
        if (totalExecutors < maxExecutors) {
            return true;
        }
    }
    SPDLOG_TRACE("NO Available executor for {}", funcStr);
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

    // auto timeFlag1 = faabric::util::getGlobalClock().epochMicros();
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
    // auto timeFlag2 = faabric::util::getGlobalClock().epochMicros();
    // int elapsed = static_cast<int>(timeFlag2 - timeFlag1);
    // SPDLOG_DEBUG("Claimed executor {} for {} in {}us",
    //             claimed->id,
    //             funcStr,
    //             elapsed);
    assert(claimed != nullptr);
    return claimed;
}

std::string Scheduler::getThisHost()
{
    faabric::util::SharedLock lock(mx);
    return thisHost;
}

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
}
