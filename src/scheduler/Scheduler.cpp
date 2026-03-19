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

#include <fstream>
#include <sstream>
#include <unordered_set>

using namespace faabric::util;
using namespace faabric::snapshot;

constexpr int DEFAULT_SLOT_NUM = 100;

const std::string WORKER_ENQUEUE_TIME_KEY = "worker_queue_time_key";
const std::string WORKER_ENQUEUE_SIZE_KEY = "worker_queue_size_key";

struct VmCpuData
{
    long long idleTime;
    long long totalTime;
};

static VmCpuData readVmCpuData()
{
    std::ifstream file("/proc/stat");
    std::string line;
    std::getline(file, line);

    long long user, nice, system, idle, iowait, irq, softirq, steal, guest,
      guest_nice;
    sscanf(line.c_str(),
           "cpu %lld %lld %lld %lld %lld %lld %lld %lld %lld %lld",
           &user,
           &nice,
           &system,
           &idle,
           &iowait,
           &irq,
           &softirq,
           &steal,
           &guest,
           &guest_nice);

    long long idleTime = idle + iowait;
    long long nonIdleTime = user + nice + system + irq + softirq + steal;
    long long totalTime = idleTime + nonIdleTime;

    return { idleTime, totalTime };
}

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
  , cpuRecordStart(std::chrono::steady_clock::now())
//   , instancesLoadState(maxSamples)
{
    executeBatchsize = conf.batchSize;
    // Start the reaper thread
    reaperThread.start(conf.reaperIntervalSeconds);
    batchTimerThread = std::thread(&Scheduler::batchTimerCheck, this);
    setResultThread = std::thread(&Scheduler::setMessageResults, this);
    stopCpuMonitor = false;
    cpuMonitorThread = std::thread(&Scheduler::cpuMonitorLoop, this);

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
    if (setResultThread.joinable()) {
        setResultThread.join();
    }
    stopCpuMonitor = true;
    if (cpuMonitorThread.joinable()) {
        cpuMonitorThread.join();
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
    if (setResultThread.joinable()) {
        setResultThread.join();
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

    runningExecutors.store(0);

    faabric::util::FullLock cpuLock(cpuRecordMx);
    cpuScheduleTime = 0;
    cpuRecordStart = std::chrono::steady_clock::now();
    while (!cpuRecordHistory.empty()) {
        cpuRecordHistory.pop();
    }
    runningThreads.clear();
    threadClockStartMap.clear();
    cpuLock.unlock();

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
    // partitionedWaitingQueues.clear();
    {
        faabric::util::FullLock chainedCallMsgslock(chainedCallMsgsMx);
        chainedCallMsgs.clear();
    }
    {
        faabric::util::FullLock setResultMsgslock(setResultMsgsMx);
        setResultMsgs.clear();
    }

    scheduledMsgsMap.clear();
    maxReplicasMap.clear();

    // This function is called when planner flush executors. In this case,
    // planner didn't flush the hostmap, the scheduler also should not flush it.
    // registeredHostsMap.clear();
    // hostMap.clear();
    activeHosts = hostMap;

    stopBatchTimer = false;
    batchTimerThread = std::thread(&Scheduler::batchTimerCheck, this);
    setResultThread = std::thread(&Scheduler::setMessageResults, this);

    stopThreadTimer = false;
    dispatchChainedMsgsThread =
      std::thread(&Scheduler::dispatchChainedMsgs, this);

    faabric::util::FullLock rflock(reconfigMx);
    decentralScheduler.resetScheduler();

    currentMigrationVersion = 0;
    receivedMigrationSources.clear();
    migrationHistory.clear();

    runtimeStats.reset();
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
    faabric::util::FullLock rflock(reconfigMx);

    decentralScheduler.registerApp(std::move(app));
    return true;
}

// Enqueue the request messages from remote into local unprocess queue for
// further processing.
void Scheduler::enqueueMessageBatch(std::unique_ptr<faabric::MessageBatch> msgs)
{
    // 1. Handle null input
    if (!msgs) {
        SPDLOG_WARN("Scheduler received a null message batch");
        return;
    }

    if (msgs->messages_size() == 0) {
        SPDLOG_DEBUG("Scheduler received an empty message batch");
        return;
    }

    // 2. Prepare the destination list and get the invokeHost
    std::list<std::unique_ptr<faabric::Message>> msgList;
    std::string invokeHost = msgs->invokehost();

    // 3. Efficiently move messages from the batch to the list
    // We use mutable_messages() to get non-const access to the repeated field.
    for (auto& msg : *msgs->mutable_messages()) {
        auto msgPtr = std::make_unique<faabric::Message>();
        msgPtr->Swap(&msg);
        msgList.push_back(std::move(msgPtr));
    }

    msgs->mutable_messages()->Clear();
    enqueueMessageBatch(std::move(msgList), invokeHost);
}

std::unique_ptr<faabric::MessageBatch> convertListToBatch(
  std::list<std::unique_ptr<faabric::Message>>& msgList,
  const std::string& invokeHost)
{
    auto batch = std::make_unique<faabric::MessageBatch>();
    batch->set_invokehost(invokeHost);

    for (auto& msgPtr : msgList) {
        batch->mutable_messages()->AddAllocated(msgPtr.release());
    }

    msgList.clear();

    return batch;
}

void Scheduler::enqueueMessageBatch(
  std::list<std::unique_ptr<faabric::Message>> msgs,
  std::string invokeHost)
{
    // If the scheduler is updating state information, we may need to transfer
    // them to other nodes. So, just enqueue them in chained call temporarily.
    if (isUpdateState) {
        SPDLOG_DEBUG("Enqueueing messages while updating state");
        auto msgsBatch = convertListToBatch(msgs, invokeHost);
        faabric::util::FullLock lock(chainedCallMsgsMx);
        for (int i = 0; i < msgsBatch->messages_size(); i++) {
            auto* msgPtr = msgsBatch->mutable_messages(i);
            chainedCallMsgs.push_back(
              std::make_unique<faabric::Message>(std::move(*msgPtr)));
        }
        return;
    }

    SPDLOG_DEBUG("Enqueueing message batch with size {} from host {}",
                 msgs.size(),
                 invokeHost);

    // This function is called by planner and other workers. Deadlocks happens
    // if lock(mx) is required. Our BatchQueue is thread-safe.

    [[maybe_unused]] int nMessages = msgs.size();
    auto current = faabric::util::getGlobalClock().epochMicros();
    auto currentMillis = faabric::util::getGlobalClock().epochMillis();
    auto endPoint = faabric::util::getSystemConfig().endpointHost;

    // Statistics the message enqueue count
    std::map<std::string, int> instancesCounter;

    while (!msgs.empty()) {
        std::unique_ptr<faabric::Message> msgPtr = std::move(msgs.front());
        msgs.pop_front();
        faabric::Message& msg = *msgPtr;
        std::string userFuncPar = msg.user() + "_" + msg.function() + "_" +
                                  std::to_string(msg.parallelismid());
        instancesCounter[userFuncPar]++;
        (*msg.mutable_metricrecorder())[WORKER_ENQUEUE_TIME_KEY] = current;
        msg.set_starttimestamp(currentMillis);
        msg.set_dispatchreceivetime(current);
        msg.set_executedhost(endPoint);

        auto [iterator, inserted] =
          waitingQueues.emplace(userFuncPar,
                                std::make_unique<faabric::util::BatchQueue>(
                                  userFuncPar, executeBatchsize));
        int waitMsgs = iterator->second->getMessagesCount();
        (*msg.mutable_metricrecorder())[WORKER_ENQUEUE_SIZE_KEY] = waitMsgs;
        runtimeStats.instanceWorkerQueueNum(userFuncPar, waitMsgs);
        iterator->second->addMessage(
          std::make_unique<faabric::Message>(std::move(msg)));
    }

    // Update the instances runtime stats
    for (const auto& [instancesName, count] : instancesCounter) {
        runtimeStats.instanceAdd(instancesName, invokeHost, count);
    }

    SPDLOG_DEBUG("Enqueued {} messages completed", nMessages);
}

void Scheduler::executeBatchForQueue(const std::string& userFuncPar,
                                     util::BatchQueueBase& waitingQueue)
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

        auto now = faabric::util::getGlobalClock().epochMicros();
        for (auto& src : msgVec) {
            // Before executing, we need to double check if the message is
            // scheduled to execute here.
            std::string host =
              decentralScheduler.scheduleMessage(activeHosts, *src);
            if (host != thisHost) {
                faabric::util::FullLock lock(chainedCallMsgsMx);
                chainedCallMsgs.push_back(std::move(src));
                continue;
            }
            // If the message is scheduled to execute here, we execute it.
            auto* message = newReq->add_messages();
            message->Swap(src.get());

            auto* metrics = message->mutable_metricrecorder();
            int workerQueueTime = now - (*metrics)[WORKER_ENQUEUE_TIME_KEY];
            message->set_workerqueuewaittime(workerQueueTime);

            runtimeStats.instanceWorkerQueueTime(userFuncPar, workerQueueTime);

            metrics->erase(WORKER_ENQUEUE_TIME_KEY);
            metrics->erase(WORKER_ENQUEUE_SIZE_KEY);
        }
        if (newReq->messages_size() != 0) {
            // Claim new Executor, we can bound the first msg here, since claim
            // only needs the user and function of Message.
            faabric::Message& localMsg = newReq->mutable_messages()->at(0);
            auto timeFlag1 = faabric::util::getGlobalClock().epochMicros();
            std::shared_ptr<faabric::executor::Executor> e =
              claimExecutor(localMsg);
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
            // Execute the BatchRequest
            auto threadClockId =
              e->executeBatchTasks(newReq, std::move(stateLock));
            if (!runningThreads.contains(threadClockId)) {
                faabric::util::FullLock cpuLock(cpuRecordMx);
                runningThreads.emplace(threadClockId);
            }
        }
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

    // TODO - DEBUG CODE: TO BE DELETE
    // runtimeStats.logAverageQueuingTimes();
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

void Scheduler::setMessageResults()
{
    while (!stopBatchTimer) {
        std::this_thread::sleep_for(
          std::chrono::milliseconds(plannerCallInterval));

        if (stopBatchTimer) {
            break;
        }

        faabric::util::FullLock setResultMsgsLock(setResultMsgsMx);
        std::vector<std::unique_ptr<faabric::Message>> tempSetResultMsgs;
        if (!setResultMsgs.empty()) {
            tempSetResultMsgs = std::move(setResultMsgs);
            setResultMsgs.clear();
        }
        setResultMsgsLock.unlock();

        if (!tempSetResultMsgs.empty()) {
            auto& plannerCli = faabric::planner::getPlannerClient();

            auto req = faabric::util::batchExecFactory("FAASM", "Func", 0);
            for (auto& msg : tempSetResultMsgs) {
                auto* message = req->add_messages();
                *message = std::move(*msg);
            }
            SPDLOG_DEBUG("Set result batch size: {}", req->messages_size());
            plannerCli.setMessageResultBatch(req);
            tempSetResultMsgs.clear();
        }
    }
}

void Scheduler::enqueueSetResults(
  std::shared_ptr<faabric::BatchExecuteRequest> req)
{
    SPDLOG_DEBUG("Enqueueing set results for {} messages",
                 req->messages_size());
    faabric::util::FullLock lock(setResultMsgsMx);

    for (int i = 0; i < req->messages_size(); i++) {
        faabric::Message& msg = req->mutable_messages()->at(i);
        int workerExecuteTime =
          msg.workerexecuteend() - msg.workerexecutestart();
        std::string userFuncPar = faabric::util::getUserFuncPar(msg);
        runtimeStats.instanceWorkerExecTime(userFuncPar, workerExecuteTime);
        setResultMsgs.emplace_back(std::make_unique<faabric::Message>(msg));
    }
    SPDLOG_DEBUG("Enqueueing set results finished");
}

void Scheduler::batchTimerCheck()
{
    while (!stopBatchTimer) {
        std::this_thread::sleep_for(
          std::chrono::milliseconds(batchCheckPeriod));

        faabric::util::FullLock lock(mx);

        if (stopBatchTimer) {
            break;
        }

        // // If we have some unScheduled messages, schedule them.
        // while (!migratedMsgs.empty()) {
        //     SPDLOG_DEBUG("Processing unscheduled messages");
        //     auto msgs = migratedMsgs.dequeue()->messages();
        //     std::vector<std::unique_ptr<faabric::Message>> msgsVec;
        //     msgsVec.reserve(msgs.size());
        //     for (auto& msg : msgs) {
        //         msgsVec.push_back(std::make_unique<faabric::Message>(msg));
        //     }
        //     auto hosts =
        //       decentralScheduler.scheduleMessagesBatch(hostMap, msgsVec);
        //     enqueueSchedMsgs(hosts, std::move(msgsVec));
        // }

        // Queue for stateless and stateful operators.
        for (auto& [userFuncPar, waitingBatch] : waitingQueues) {
            if (waitingBatch->getMessagesCount() == 0) {
                continue;
            }
            if (waitingBatch->getMessagesCount() >= executeBatchsize ||
                waitingBatch->getTimeInterval() >= batchInterval) {
                executeBatchForQueue(userFuncPar, *waitingBatch);
            }
        }
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

        if (stopThreadTimer) {
            break;
        }

        /***
         * CPU usage recording
         ***/
        faabric::util::FullLock cpuLock(cpuRecordMx);
        auto nowWall = std::chrono::steady_clock::now();
        if (nowWall - cpuRecordStart >= cpuRecordWindow) {
            const auto wallNs =
              std::chrono::duration_cast<std::chrono::nanoseconds>(
                nowWall - cpuRecordStart)
                .count();
            long long totalDeltaNs = 0;

            for (const auto& [clk, startNs] : threadClockStartMap) {
                const int64_t endNs = faabric::util::getCpuTimeNano(clk);
                if (endNs >= 0 && startNs >= 0 && endNs >= startNs) {
                    const int64_t deltaNs = endNs - startNs;
                    totalDeltaNs += deltaNs;
                }
            }

            if (wallNs > 0 && totalDeltaNs > 0) {
                const double cpuExecutePct =
                  100.0 * (double)totalDeltaNs / (double)wallNs;
                const int64_t cpuScheduleNs =
                  cpuScheduleTime.exchange(0, std::memory_order_acq_rel);
                const double cpuSchedulePct =
                  100.0 * (double)cpuScheduleNs / (double)wallNs;

                SPDLOG_DEBUG(
                  "CPU execute percentage: {:.2f}%, CPU schedule percentage: "
                  "{:.2f}%",
                  cpuExecutePct,
                  cpuSchedulePct);

                cpuRecordHistory.emplace(
                  std::make_tuple(cpuExecutePct, cpuSchedulePct));
                while (cpuRecordHistory.size() > historyCap) {
                    cpuRecordHistory.pop();
                }
            }
            threadClockStartMap.clear();

            for (const clockid_t clk : runningThreads) {
                const int64_t nowNs = faabric::util::getCpuTimeNano(clk);
                if (nowNs >= 0) {
                    threadClockStartMap.emplace(clk, nowNs);
                }
            }
            cpuRecordStart = nowWall;
        }
        cpuLock.unlock();
        /***
         * End of CPU usage recording
         ***/

        // if scheduleMode is 7 (centralized faasflow), we need to transfer the
        // chained calls to planner
        if (scheduleMode == 7) {
            auto& plannerCli = faabric::planner::getPlannerClient();

            faabric::util::FullLock chainedCallLock(chainedCallMsgsMx);
            if (chainedCallMsgs.empty()) {
                chainedCallLock.unlock();
                continue;
            }
            auto req = faabric::util::batchExecFactory("FAASM", "Func", 0);
            auto currentTime = faabric::util::getGlobalClock().epochMicros();
            for (auto& msg : chainedCallMsgs) {
                msg->set_plannerdispatchtime(currentTime);
                auto* message = req->add_messages();
                *message = std::move(*msg);
            }
            SPDLOG_DEBUG("Chaining call batch size: {}", req->messages_size());
            plannerCli.enqueueFunctions(req);
            chainedCallMsgs.clear();
            SPDLOG_DEBUG("Chaining call batch completed");
            continue;
        }

        faabric::util::FullLock mxlock(mx);
        // Otherwise, decentralized scheduler is used, we schedule the chained
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
            // Schedule the chained calls
            auto start = faabric::util::getCpuTimeNano();
            auto hosts = decentralScheduler.scheduleMessagesBatch(
              activeHosts, localChainedCallMsgs);
            auto end = faabric::util::getCpuTimeNano();
            if (start > 0 && end >= start) {
                cpuScheduleTime.fetch_add(end - start,
                                          std::memory_order_relaxed);
            }

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
        lock.unlock();

        for (auto& [hostIp, msgs] : msgsCallMap) {
            // SPDLOG_DEBUG the hosts and messages
            SPDLOG_DEBUG("Dispatching messages to host {} with message size {}",
                         hostIp,
                         msgs.size());
            // If locally, we put the messages into a batch directly
            if (hostIp == thisHost) {
                enqueueMessageBatch(std::move(msgs), thisHost);
            } else {
                // Otherwise, we send the messages to the remote host
                faabric::scheduler::getFunctionCallClient(hostIp)
                  ->executeFunctionsBatch(std::move(msgs));
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
    } else if (key == "batch_size") {
        executeBatchsize = value;
        // change the batch size of all waiting queues
        for (auto& [userFuncPar, waitingBatch] : waitingQueues) {
            waitingBatch->resetBatchSize(executeBatchsize);
        }
        // for (auto& [userFuncPar, waitingBatch] : partitionedWaitingQueues) {
        //     waitingBatch->resetBatchSize(executeBatchsize);
        // }
        SPDLOG_INFO("Reset executeBatchsize parameter to : {}",
                    executeBatchsize);
    } else if (key == "schedule_mode") {
        decentralScheduler.setScheduleMode(value);
        scheduleMode = value;
        SPDLOG_INFO("Reset schedule_mode parameter to : {}", value);
    } else if (key == "dispatch_period") {
        dispatchPeriod = value;
        SPDLOG_INFO("Reset dispatchPeriod parameter to : {}", dispatchPeriod);
    } else if (key == "batch_check_period") {
        batchCheckPeriod = value;
        SPDLOG_INFO("Reset batchCheckPeriod parameter to : {}",
                    batchCheckPeriod);
    } else if (key == "parallel_dispatch") {
        if (value == 1) {
            parallelDispatch = true;
        } else {
            parallelDispatch = false;
        }
    } else if (key == "runtime_reconfig") {
        decentralScheduler.setRuntimeReconfig(value == 1);
    } else if (key == "alpha") {
        double newAlpha = value / 1000.0;
        SPDLOG_INFO("Alpha is set to {}", newAlpha);
        decentralScheduler.setAlpha(newAlpha);
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

    int maxReplicas;
    try {
        maxReplicas = util::getOrThrow(maxReplicasMap, funcStr);
    } catch (const std::runtime_error& e) {
        SPDLOG_WARN("Key {} not found in maxReplicasMap, defaulting to 1",
                    funcStr);
        maxReplicas = 1;
    }
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

void Scheduler::notifyExecutorStart()
{
    runningExecutors++;
}

void Scheduler::notifyExecutorFinished()
{
    int current = runningExecutors.load();

    while (current > 0) {
        if (runningExecutors.compare_exchange_weak(current, current - 1)) {
            break;
        }
    }
}

int Scheduler::getRunningExecutorsCount() const
{
    return runningExecutors.load();
}

std::shared_ptr<faabric::executor::Executor> Scheduler::claimExecutor(
  faabric::Message& msg)
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
        // schedulerLock.unlock();
        auto executor = factory->createExecutor(msg);
        // schedulerLock.lock();
        thisExecutors.push_back(std::move(executor));
        claimed = thisExecutors.back();

        // Claim it
        claimed->tryClaim();
    }
    assert(claimed != nullptr);
    return claimed;
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
    faabric::state::getGlobalState().updateHosts(hostMap);
}

void Scheduler::storeMigrateState(
  std::map<std::string, std::vector<uint8_t>>&& migrateState)
{
    faabric::util::FullLock lock(migratedStateMapMx);
    if (migrateState.empty()) {
        SPDLOG_DEBUG("Received empty migrate state, nothing to store");
        return;
    }
    migratedStateMap.merge(migrateState);
}

void Scheduler::updateActiveHosts(
  const std::map<std::string, faabric::batch_scheduler::ScheduledOperator>&
    scheduledOperatorMap)
{
    std::set<std::string> uniqueIps;
    for (const auto& [opName, op] : scheduledOperatorMap) {
        for (const auto& [ip, weight] : op.weightDist) {
            if (!ip.empty()) {
                uniqueIps.insert(ip);
            }
        }
    }

    if (!uniqueIps.empty()) {
        std::stringstream ss;
        ss << "Active IPs (" << uniqueIps.size() << "): [";
        for (auto it = uniqueIps.begin(); it != uniqueIps.end(); ++it) {
            ss << *it << (std::next(it) != uniqueIps.end() ? ", " : "");
        }

        ss << "]";
        SPDLOG_DEBUG(ss.str());
    } else {
        SPDLOG_DEBUG("No active IPs found.");
        activeHosts = hostMap;
    }

    activeHosts.clear();
    for (const auto& ip : uniqueIps) {
        auto it = hostMap.find(ip);
        if (it != hostMap.end()) {
            activeHosts[ip] = it->second;
        } else {
            SPDLOG_WARN("Operator scheduled on unknown host IP: {}", ip);
        }
    }
}

void Scheduler::updateStatesInfo(
  const std::map<std::string, faabric::batch_scheduler::ScheduledOperator>&
    scheduledOperatorMap,
  const std::map<std::string, faabric::batch_scheduler::FunctionStateInfo>&
    statesInfo,
  int migrationVersion,
  bool isInitialization,
  std::map<std::string, std::set<std::string>>& transDestinationMap,
  std::map<std::string, std::set<std::string>>& transSourceMap)
{
    SPDLOG_DEBUG("State Update: Start");

    auto startTime = faabric::util::getGlobalClock().epochMillis();

    isUpdateState = true;
    // Update the states info in decentralized scheduler
    // To update states, we must ensure that no executors are running (hold
    // state lock).
    faabric::util::FullLock lock(mx);
    SPDLOG_DEBUG("State Update: scheduler lock acquired");
    faabric::util::FullLock stateLock(stateUpdateMx);
    SPDLOG_DEBUG("State Update: state lock acquired");

    // 1. Update the local max replicas map based on the new scheduling
    // decision.
    calculateMaxReplicas(scheduledOperatorMap);

    // 2. Update the local scheduler and update the states info in decentralized
    // scheduler
    faabric::util::FullLock rflock(reconfigMx);

    SPDLOG_DEBUG("State Update: decentralized scheduler states update");
    updateActiveHosts(scheduledOperatorMap);
    decentralScheduler.resetScheduler();
    decentralScheduler.setScheuduledOperatorMap(scheduledOperatorMap);
    // update runtime summary and states info.
    decentralScheduler.syncStatesInfo(statesInfo);

    runtimeStats.versionUpdate(migrationVersion);

    // If it's initialization, we don't need to migrate. Just return after
    // updating the states info.
    if (isInitialization) {
        createLocalState(statesInfo);
        isUpdateState = false;
        SPDLOG_DEBUG(
          "State Update: initialization complete, no migration needed");
        return;
    }

    // Update the version, state destionation map and state source map.
    currentMigrationVersion = migrationVersion;
    std::set<std::string> migrationDestinations = transDestinationMap[thisHost];
    std::set<std::string> migrationSources = transSourceMap[thisHost];

    // 3. Prepare the migration request: state and in-flight messages

    SPDLOG_DEBUG("State Update: Migration data start");

    auto migrationStatesMap = packState(statesInfo);

    auto migrationMessagesMap = packMessage();

    // Transfer the state and message to the new host.

    transferData(migrationVersion,
                 migrationStatesMap,
                 std::move(migrationMessagesMap),
                 migrationDestinations);

    SPDLOG_DEBUG("State Update: Migration hurdle");

    // 4. Wait until migration is completed.
    size_t expectedCount = migrationSources.size();

    SPDLOG_DEBUG("Migration Version {}: Waiting for data from {} hosts",
                 migrationVersion,
                 expectedCount);

    if (expectedCount > 0) {
        std::unique_lock<std::mutex> lock(migrationMx);
        bool success = migrationCv.wait_for(
          lock,
          std::chrono::seconds(10),
          [this, migrationVersion, expectedCount] {
              return receivedMigrationSources[migrationVersion].size() >=
                     expectedCount;
          });
        if (!success) {
            SPDLOG_DEBUG("Migration timed out! Received {}/{} sources",
                         receivedMigrationSources[migrationVersion].size(),
                         expectedCount);
        } else {
            SPDLOG_DEBUG("Migration completed successfully for version {}",
                         migrationVersion);
        }
    } else {
        SPDLOG_DEBUG("No migration sources for this host, proceeding.");
    }

    // 5. Head to next step after migration. (sync state and messages)
    // Create state in state server
    SPDLOG_DEBUG("State Update: create states based on new states info");

    createLocalState(statesInfo);

    // Update the states from migratedStateMap to state
    faabric::util::FullLock migrateStateLock(migratedStateMapMx);
    auto& stateServer = faabric::state::getGlobalState();
    stateServer.loadMigrateState(migratedStateMap);

    SPDLOG_DEBUG(
      "State Update: states reallocation complete, reschedule requests now");

    isUpdateState = false;

    while (!migratedMsgs.empty()) {
        SPDLOG_DEBUG("Processing migrated messages");
        auto msgBatch = migratedMsgs.dequeue();
        enqueueMessageBatch(std::move(msgBatch));
    }

    // Clean up the migration sources
    receivedMigrationSources.erase(migrationVersion);

    auto endTime = faabric::util::getGlobalClock().epochMillis();
    int totalTimeMillis = endTime - startTime;
    SPDLOG_DEBUG("State Update: completed in {} ms", totalTimeMillis);

    migrationHistory[migrationVersion] = totalTimeMillis;
}

void Scheduler::calculateMaxReplicas(
  const std::map<std::string, faabric::batch_scheduler::ScheduledOperator>&
    scheduledOperatorMap)
{
    maxReplicasMap.clear();
    // reset max replicas based on resource requirements.
    std::map<std::string, int> tempMaxReplicasMap;
    for (const auto& [operatorName, operatorInfo] : scheduledOperatorMap) {
        if (operatorInfo.weightDist.count(thisHost) <= 0) {
            continue;
        }
        std::string userFunc = util::splitUserFunc(operatorName).first + "/" +
                               util::splitUserFunc(operatorName).second;
        // If operator is stateless
        if (operatorInfo.node.type == faabric::batch_scheduler::STATELESS) {
            int maxReplica =
              std::round(operatorInfo.weightDist.at(thisHost) * maxExecutors);
            tempMaxReplicasMap[userFunc + "/0"] = maxReplica;
        }
        // If operator is stateful
        else if (operatorInfo.node.type == faabric::batch_scheduler::STATEFUL) {
            double totalWeight = operatorInfo.weightDist.at(thisHost);
            int totalInstsances = 0;
            for (const auto& [parId, ip] : operatorInfo.parallelismDist) {
                if (ip == thisHost) {
                    totalInstsances++;
                }
            }
            double instanceWeight =
              totalWeight / static_cast<double>(totalInstsances);
            for (const auto& [parId, ip] : operatorInfo.parallelismDist) {
                if (ip == thisHost) {
                    int maxReplica = std::round(instanceWeight * maxExecutors);
                    tempMaxReplicasMap[userFunc + "/" + std::to_string(parId)] =
                      maxReplica;
                }
            }
        }
        // If operator is partitioned stateful
        else if (operatorInfo.node.type ==
                 faabric::batch_scheduler::PARTITIONED_STATEFUL) {
            for (const auto& [parId, ip] : operatorInfo.parallelismDist) {
                if (ip == thisHost) {
                    double weight = operatorInfo.weightDist.at(thisHost);
                    int maxReplica = std::round(weight * maxExecutors);
                    tempMaxReplicasMap[userFunc + "/" + std::to_string(parId)] =
                      maxReplica;
                }
            }
        }
    }
    // We then scale, make sure the total replicas are not larger than
    // maxExecutors.
    int totalReplicas = 0;
    for (const auto& [funcStr, maxReplica] : tempMaxReplicasMap) {
        totalReplicas += maxReplica;
    }
    double scaleFactor =
      static_cast<double>(maxExecutors) / static_cast<double>(totalReplicas);
    for (const auto& [funcStr, maxReplica] : tempMaxReplicasMap) {
        int scaledMaxReplica =
          std::ceil(static_cast<double>(maxReplica) * scaleFactor);
        if (scaledMaxReplica <= 0) {
            scaledMaxReplica = 1;
        }
        maxReplicasMap[funcStr] = scaledMaxReplica;
        SPDLOG_DEBUG("updateStatesInfo: {} max replicas set to {}",
                     funcStr,
                     scaledMaxReplica);
    }
}

void Scheduler::createLocalState(
  const std::map<std::string, faabric::batch_scheduler::FunctionStateInfo>&
    statesInfo)
{
    auto& stateServer = faabric::state::getGlobalState();
    stateServer.clearFS();
    for (const auto& [stateKey, stateInfo] : statesInfo) {
        for (const auto& [id, ip] : stateInfo.stateHost) {
            if (ip != thisHost) {
                continue;
            }
            auto [user, func] = faabric::util::splitUserFunc(stateKey);
            bool isPartitionable =
              stateInfo.partitionBy != "" && stateInfo.partitionBy != "None";
            stateServer.createFS(user, func, id, isPartitionable);
        }
    }
}

// MAP <HOST, function parallelism, serialized state>
std::map<std::string, std::map<std::string, std::vector<uint8_t>>>
Scheduler::packState(
  const std::map<std::string, faabric::batch_scheduler::FunctionStateInfo>&
    statesInfo)
{
    SPDLOG_DEBUG("Packing state for migration");
    // Migrate the state according to new scheduling decision.
    auto& stateServer = faabric::state::getGlobalState();

    auto& hashRings = decentralScheduler.getStateHashRing();
    auto migrationStatesMap = stateServer.redirectState(hashRings, statesInfo);

    return migrationStatesMap;
}

// MAP <HOST, message batch>
std::map<std::string, std::unique_ptr<faabric::MessageBatch>>
Scheduler::packMessage()
{
    SPDLOG_DEBUG("Packing messages for migration");
    std::map<std::string, std::unique_ptr<faabric::MessageBatch>>
      packedMessageMap;
    // Migrate the in-flight messages according to new scheduling decision.
    std::vector<std::unique_ptr<faabric::Message>> localMsgs;
    for (auto& [_, queuePtr] : waitingQueues) {
        auto messages = queuePtr->drainMessages();
        if (messages.empty()) {
            continue;
        }
        localMsgs.insert(localMsgs.end(),
                         std::make_move_iterator(messages.begin()),
                         std::make_move_iterator(messages.end()));
    }
    faabric::util::FullLock lock(scheduledMsgsMapMx);
    for (auto& [_, msgs] : scheduledMsgsMap) {
        for (auto& msg : msgs) {
            localMsgs.emplace_back(std::move(msg));
        }
    }
    scheduledMsgsMap.clear();
    lock.unlock();

    SPDLOG_DEBUG("Total local messages to migrate: {}", localMsgs.size());

    // Schedule the local messages according to the new scheduling decision.
    auto hosts =
      decentralScheduler.scheduleMessagesBatch(activeHosts, localMsgs);

    for (size_t i = 0; i < localMsgs.size(); ++i) {
        const std::string& destinationHost = hosts[i];
        auto& msg = localMsgs[i];

        if (packedMessageMap.find(destinationHost) == packedMessageMap.end()) {
            packedMessageMap[destinationHost] =
              std::make_unique<faabric::MessageBatch>();
        }
        auto* newMsg = packedMessageMap[destinationHost]->add_messages();
        *newMsg = std::move(*msg);
    }

    return packedMessageMap;
}

void Scheduler::transferData(int migrationVersion,
                             StateMigrationMap stateMap,
                             MessageMigrationMap msgMap,
                             std::set<std::string> migrationDestinations)
{
    SPDLOG_INFO("Transferring data for migration version {}, number of "
                "destination hosts: {}",
                migrationVersion,
                migrationDestinations.size());
    // Prepare Message
    // Map <Destination host, StateMigrationRequest>
    std::map<std::string, std::shared_ptr<faabric::StateMigrationRequest>>
      migrationRequestMap;

    for (const auto& destHost : migrationDestinations) {
        migrationRequestMap[destHost] =
          std::make_shared<faabric::StateMigrationRequest>();
        migrationRequestMap[destHost]->set_sourcehost(thisHost);
        migrationRequestMap[destHost]->set_migrationversion(migrationVersion);
    }

    for (auto& [destHost, funcData] : stateMap) {
        if (migrationRequestMap.find(destHost) == migrationRequestMap.end()) {
            SPDLOG_ERROR("Destination host {} not in migrationRequestMap",
                         destHost);
            throw std::runtime_error(fmt::format(
              "Destination host {} not in migrationRequestMap", destHost));
        }
        auto& requestPtr = migrationRequestMap[destHost];

        for (auto& [funcPar, state] : funcData) {
            auto* migrateState = requestPtr->add_migratestates();
            migrateState->set_userfuncpar(funcPar);
            migrateState->set_serializedstate(state.data(), state.size());
        }
    }

    for (auto& [destHost, batchPtr] : msgMap) {
        if (!batchPtr)
            continue;

        auto& request = migrationRequestMap[destHost];
        request->set_allocated_messagebatch(batchPtr.release());
    }

    // Transfer data to the new host
    for (auto& [destHost, requestPtr] : migrationRequestMap) {
        if (destHost == thisHost) {
            processMigrationData(*requestPtr);
        } else {
            faabric::scheduler::getFunctionCallClient(destHost)->migrateStates(
              requestPtr);
        }
    }
}

void Scheduler::processMigrationData(const faabric::StateMigrationRequest& req)
{
    std::unique_lock<std::mutex> lock(migrationMx);

    const std::string& source = req.sourcehost();

    std::map<std::string, std::vector<uint8_t>> immiStates;
    for (const auto& stateEntry : req.migratestates()) {
        const std::string& key = stateEntry.userfuncpar();
        const std::string& data = stateEntry.serializedstate();
        std::vector<uint8_t> stateData(data.begin(), data.end());
        immiStates[key] = std::move(stateData);
    }
    this->storeMigrateState(std::move(immiStates));

    if (req.messagebatch().messages_size() > 0) {
        migratedMsgs.enqueue(
          std::make_unique<faabric::MessageBatch>(req.messagebatch()));
    }

    int migrationVersion = req.migrationversion();
    receivedMigrationSources[migrationVersion].insert(source);

    migrationCv.notify_all();
}

std::map<std::string, InstanceStatsResult> Scheduler::getRuntimeStats()
{
    return runtimeStats.getAllStats();
}

void Scheduler::updateStatelessDist(
  const std::map<std::string, std::map<std::string, int>>& sourceCountStats)
{
    // TODO - update the source.
    SPDLOG_DEBUG("Updating stateless distribution");
    faabric::util::FullLock rflock(reconfigMx);
    decentralScheduler.runtimeDistTune(sourceCountStats);
}

void Scheduler::setLocalPersistentState(
  const std::map<std::string, std::string>& kvMap)
{
    SPDLOG_DEBUG("Setting local persistent state");
    faabric::state::getGlobalState().writePersistentStateBatch(kvMap);
    SPDLOG_DEBUG("Local persistent state set successfully");
}

std::string Scheduler::getLocalPersistentState(std::string key)
{
    SPDLOG_DEBUG("Getting local persistent state for key: {}", key);
    std::string value;
    value = faabric::state::getGlobalState().readPersistentState(key);

    return value;
}

void Scheduler::flushState()
{
    SPDLOG_INFO("Flushing state");
    faabric::state::getGlobalState().flushState();
}

std::queue<std::tuple<double, double>> Scheduler::getCpuRecordHistory()
{
    return cpuRecordHistory;
}

std::map<std::string, int> Scheduler::getMaxReplicasMap()
{
    return maxReplicasMap;
}

std::map<int, int> Scheduler::getMigrationHistory()
{
    SPDLOG_DEBUG("Retrieving migration history with {} records",
                 migrationHistory.size());
    if (migrationHistory.empty()) {
        SPDLOG_DEBUG("No migration history records found.");
    } else {
        std::stringstream ss;
        ss << "Migration History: {";
        for (auto it = migrationHistory.begin(); it != migrationHistory.end();
             ++it) {
            ss << it->first << ": " << it->second
               << (std::next(it) != migrationHistory.end() ? ", " : "");
        }
        ss << "}";
        SPDLOG_DEBUG(ss.str());
    }

    return migrationHistory;
}

std::map<time_t, int> Scheduler::getVersionTimestamps()
{
    return runtimeStats.getVersionTimestamps();
}

std::map<std::string, InstanceMetricsResult> Scheduler::getWorkerMetrics(
  bool isRuntime)
{
    return runtimeStats.getWorkerMetrics(isRuntime);
}

std::tuple<std::map<std::string, int>, int, double>
Scheduler::getStatsSnapshot()
{
    std::map<std::string, int> queueSizes;
    for (const auto& [userFuncPar, waitingBatch] : waitingQueues) {
        queueSizes[userFuncPar] = waitingBatch->getMessagesCount();
    }

    int runningExecutorsCount = getRunningExecutorsCount();
    double lastCpu = getLastVmCpu();

    return { queueSizes, runningExecutorsCount, lastCpu };
}

void Scheduler::cpuMonitorLoop()
{
    SPDLOG_INFO("Starting VM CPU monitor thread");

    VmCpuData prevData = readVmCpuData();

    while (!stopCpuMonitor) {
        std::this_thread::sleep_for(std::chrono::seconds(1));

        if (stopCpuMonitor) {
            break;
        }

        VmCpuData currData = readVmCpuData();

        long long totalDiff = currData.totalTime - prevData.totalTime;
        long long idleDiff = currData.idleTime - prevData.idleTime;

        double currentCpuUsage = 0.0;
        if (totalDiff > 0) {
            currentCpuUsage = 100.0 *
                              static_cast<double>(totalDiff - idleDiff) /
                              static_cast<double>(totalDiff);
        }

        prevData = currData;

        faabric::util::FullLock lock(vmCpuHistoryMx);
        vmCpuHistory.push_back(currentCpuUsage);

        if (vmCpuHistory.size() > 60) {
            vmCpuHistory.pop_front();
        }
    }

    SPDLOG_INFO("VM CPU monitor thread stopped");
}

double Scheduler::getLastVmCpu()
{
    faabric::util::SharedLock lock(vmCpuHistoryMx);

    if (vmCpuHistory.empty()) {
        return 0.0;
    }

    return vmCpuHistory.back();
}
}
