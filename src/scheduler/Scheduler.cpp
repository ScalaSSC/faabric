#include <faabric/batch-scheduler/BatchScheduler.h>
#include <faabric/executor/ExecutorFactory.h>
#include <faabric/planner/PlannerClient.h>
#include <faabric/planner/planner.pb.h>
#include <faabric/proto/faabric.pb.h>
#include <faabric/redis/Redis.h>
#include <faabric/scheduler/FunctionCallClient.h>
#include <faabric/scheduler/Scheduler.h>
#include <faabric/snapshot/SnapshotClient.h>
#include <faabric/snapshot/SnapshotRegistry.h>
#include <faabric/state/State.h>
#include <faabric/transport/PointToPointBroker.h>
#include <faabric/util/batch.h>
#include <faabric/util/bytes.h>
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

#include <algorithm>
#include <cmath>
#include <fstream>
#include <limits>
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

// How long an event-driven loop sleeps at most without being woken. Only a
// safety net against a missed wake-up: work is picked up when it arrives, not
// on this timer.
static constexpr auto SAFETY_WAKE = std::chrono::milliseconds(100);

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
//   , cpuRecordStart(std::chrono::steady_clock::now())
//   , instancesLoadState(maxSamples)
{
    executeBatchsize = conf.batchSize;
    // Start the reaper thread
    reaperThread.start(conf.reaperIntervalSeconds);
    // batchTimerThread = std::thread(&Scheduler::batchTimerCheck, this);
    setResultThread = std::thread(&Scheduler::setMessageResults, this);
    stopCpuMonitor = false;
    cpuMonitorThread = std::thread(&Scheduler::cpuMonitorLoop, this);

    dispatchChainedMsgsThread =
      std::thread(&Scheduler::dispatchChainedMsgs, this);

    fluxWorkerSlots =
      std::max(1, static_cast<int>(std::thread::hardware_concurrency()));
    decentralScheduler.setFluxLocalLoadProvider(
      [this](const std::string& userFuncPar) {
          return fluxLocalLoadOf(userFuncPar);
      });
    fluxRebalanceThread = std::thread(&Scheduler::fluxRebalanceLoop, this);

    // Initialize the dispatch worker pool
    int numDispatchers = 4;
    for (int i = 0; i < numDispatchers; ++i) {
        dispatchThreads.emplace_back(&Scheduler::dispatchWorkerLoop, this);
    }
}

Scheduler::~Scheduler()
{
    if (!_isShutdown) {
        SPDLOG_ERROR("Destructing scheduler without shutting down first");
    }
    // Stop the batch timer thread
    stopBatchTimer = true;
    resultsWake.notify();
    // if (batchTimerThread.joinable()) {
    //     batchTimerThread.join();
    // }
    if (setResultThread.joinable()) {
        setResultThread.join();
    }
    stopCpuMonitor = true;
    if (cpuMonitorThread.joinable()) {
        cpuMonitorThread.join();
    }
    stopThreadTimer = true;
    chainedWake.notify();
    if (dispatchChainedMsgsThread.joinable()) {
        dispatchChainedMsgsThread.join();
    }
    if (fluxRebalanceThread.joinable()) {
        fluxRebalanceThread.join();
    }

    // Safely shutdown dispatch threads
    {
        std::lock_guard<std::mutex> lock(dispatchMx);
        stopDispatcher = true;
    }
    dispatchCv.notify_all();
    // for (size_t i = 0; i < dispatchThreads.size(); ++i) {
    //     readyDispatchQueue.enqueue("");
    // }
    for (auto& t : dispatchThreads) {
        if (t.joinable())
            t.join();
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
    resultsWake.notify();
    // if (batchTimerThread.joinable()) {
    //     batchTimerThread.join();
    // }
    if (setResultThread.joinable()) {
        setResultThread.join();
    }

    stopThreadTimer = true;
    chainedWake.notify();
    if (dispatchChainedMsgsThread.joinable()) {
        dispatchChainedMsgsThread.join();
    }
    if (fluxRebalanceThread.joinable()) {
        fluxRebalanceThread.join();
    }

    // Shut down, then clear executors
    for (auto& ep : executors) {
        for (auto& e : ep.second) {
            e->shutdown();
        }
    }
    executors.clear();

    runningExecutors.store(0);

    // faabric::util::FullLock cpuLock(cpuRecordMx);
    // cpuScheduleTime = 0;
    // cpuRecordStart = std::chrono::steady_clock::now();
    // while (!cpuRecordHistory.empty()) {
    //     cpuRecordHistory.pop();
    // }
    // runningThreads.clear();
    // threadClockStartMap.clear();
    // cpuLock.unlock();

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
    decentralScheduler.setClusterWorkerStats({});

    scheduledMsgsMap.clear();
    maxReplicasMap.clear();
    {
        faabric::util::FullLock lock(fluxKnownUnitsMx);
        fluxKnownUnits.clear();
    }
    clearFluxMigrationState();
    {
        std::lock_guard<std::mutex> lock(fluxParidxRunMx);
        fluxParidxLoad.clear();
    }
    {
        // The queues are gone. A dispatcher still working on one finds it
        // missing and moves on.
        std::lock_guard<std::mutex> lock(dispatchMx);
        dispatchPending.clear();
    }
    {
        // The executors are shut down by now, so no batch holds any of these.
        faabric::util::FullLock lock(fluxUnitLocksMx);
        fluxUnitLocks.clear();
    }
    fluxMigrationCostUs = 0;
    fluxPreviousDepths.clear();
    fluxCounters.reset();

    // This function is called when planner flush executors. In this case,
    // planner didn't flush the hostmap, the scheduler also should not flush it.
    // registeredHostsMap.clear();
    // hostMap.clear();
    activeHosts = hostMap;

    stopBatchTimer = false;
    // batchTimerThread = std::thread(&Scheduler::batchTimerCheck, this);
    setResultThread = std::thread(&Scheduler::setMessageResults, this);

    stopThreadTimer = false;
    dispatchChainedMsgsThread =
      std::thread(&Scheduler::dispatchChainedMsgs, this);
    fluxRebalanceThread = std::thread(&Scheduler::fluxRebalanceLoop, this);

    faabric::util::FullLock rflock(reconfigMx);
    decentralScheduler.resetScheduler();

    currentMigrationVersion = 0;
    receivedMigrationSources.clear();

    runtimeStats.reset();

    faabric::state::getGlobalState().resetPersistentLockState();
    faabric::state::getGlobalState().persistentLock = false;
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
    auto it = executors.find(executorPoolKey(msg));
    return it == executors.end() ? 0 : it->second.size();
}

std::string Scheduler::executorPoolKey(const faabric::Message& msg) const
{
    if (scheduleMode == faabric::batch_scheduler::ModeFlux) {
        return faabric::util::funcToString(msg, false);
    }
    return faabric::util::funcParToString(msg, false);
}

void Scheduler::advanceFluxLoad(FluxParidxLoad& load, int64_t nowUs)
{
    if (load.windowStartUs == 0) {
        load.windowStartUs = nowUs;
    }
    if (load.lastChangeUs > 0 && nowUs > load.lastChangeUs) {
        load.runningIntegralUs +=
          static_cast<double>(load.running) * (nowUs - load.lastChangeUs);
    }
    load.lastChangeUs = nowUs;
}

namespace {
// user/func/par (executor and run-slot keys) to user_func_par (queue keys,
// and the keys of the stats). User and function names contain no '/'.
std::string fluxQueueKeyOf(const std::string& funcParStr)
{
    std::string key = funcParStr;
    std::replace(key.begin(), key.end(), '/', '_');
    return key;
}

std::string fluxFuncParStrOf(const std::string& userFuncPar)
{
    auto [user, func, par] = faabric::util::splitUserFuncPar(userFuncPar);
    return user + "/" + func + "/" + par;
}
}

bool Scheduler::reserveParidxRunFlux(const std::string& funcParStr)
{
    {
        std::lock_guard<std::mutex> lock(fluxParidxRunMx);
        auto& load = fluxParidxLoad[funcParStr];
        if (load.running < maxExecutors) {
            advanceFluxLoad(load,
                            faabric::util::getGlobalClock().epochMicros());
            load.running++;
            return true;
        }
    }
    // At the cap: this queue will not drain until one of its batches ends.
    logFluxExecutorCensus(funcParStr);
    return false;
}

void Scheduler::releaseParidxRunFlux(const std::string& funcParStr)
{
    std::lock_guard<std::mutex> lock(fluxParidxRunMx);
    auto it = fluxParidxLoad.find(funcParStr);
    if (it != fluxParidxLoad.end() && it->second.running > 0) {
        advanceFluxLoad(it->second,
                        faabric::util::getGlobalClock().epochMicros());
        it->second.running--;
    }
}

void Scheduler::finishParidxRunFlux(const std::string& funcParStr, int requests)
{
    std::lock_guard<std::mutex> lock(fluxParidxRunMx);
    auto it = fluxParidxLoad.find(funcParStr);
    if (it == fluxParidxLoad.end()) {
        return;
    }
    advanceFluxLoad(it->second, faabric::util::getGlobalClock().epochMicros());
    if (it->second.running > 0) {
        it->second.running--;
    }
    it->second.finished += std::max(1, requests);
}

void Scheduler::rollFluxParidxWindows()
{
    int64_t nowUs = faabric::util::getGlobalClock().epochMicros();
    std::lock_guard<std::mutex> lock(fluxParidxRunMx);
    for (auto& [funcParStr, load] : fluxParidxLoad) {
        advanceFluxLoad(load, nowUs);
        int64_t windowUs = nowUs - load.windowStartUs;
        if (windowUs <= 0) {
            continue;
        }
        load.ratePerSec = load.finished * 1e6 / windowUs;
        if (load.finished > 0) {
            // Little's law: batches in flight over requests per second is
            // the time each request holds a run slot.
            double avgRunning = load.runningIntegralUs / windowUs;
            load.slotTimeUs = avgRunning * 1e6 / load.ratePerSec;
        }
        load.windowStartUs = nowUs;
        load.runningIntegralUs = 0;
        load.finished = 0;
    }
}

std::map<std::string, Scheduler::FluxParidxLoad>
Scheduler::snapshotFluxParidxLoads()
{
    std::lock_guard<std::mutex> lock(fluxParidxRunMx);
    return fluxParidxLoad;
}

faabric::batch_scheduler::StateAwareScheduler::FluxLocalLoad
Scheduler::fluxLocalLoadOf(const std::string& userFuncPar)
{
    faabric::batch_scheduler::StateAwareScheduler::FluxLocalLoad local;
    {
        faabric::util::SharedLock lock(waitingQueuesMx);
        auto it = waitingQueues.find(userFuncPar);
        if (it != waitingQueues.end()) {
            local.queued = it->second->getMessagesCount();
        }
    }

    int running = 0;
    {
        std::lock_guard<std::mutex> lock(fluxParidxRunMx);
        auto it = fluxParidxLoad.find(fluxFuncParStrOf(userFuncPar));
        if (it != fluxParidxLoad.end()) {
            running = it->second.running;
            local.ratePerSec = it->second.ratePerSec;
            local.slotTimeUs = it->second.slotTimeUs;
        }
    }

    bool workerSlotFree =
      runningExecutors.load(std::memory_order_relaxed) < fluxWorkerSlots;
    local.freeRequests = workerSlotFree ? std::max(0, maxExecutors - running) *
                                            std::max(1, executeBatchsize)
                                        : 0;
    // A request sent elsewhere leaves with the next chained-call dispatch and
    // is only picked up by the other worker's next one.
    local.remoteCostUs = dispatchPeriod * 1000.0;
    return local;
}

void Scheduler::fillFluxWorkerStats(faabric::WorkerStats& out)
{
    {
        std::lock_guard<std::mutex> lock(fluxParidxRunMx);
        for (const auto& [funcParStr, load] : fluxParidxLoad) {
            std::string queueKey = fluxQueueKeyOf(funcParStr);
            (*out.mutable_paridxrunning())[queueKey] = load.running;
            if (load.slotTimeUs > 0) {
                (*out.mutable_paridxslottimeus())[queueKey] = load.slotTimeUs;
            }
        }
    }
    out.set_workerslots(fluxWorkerSlots);
    out.set_paridxcap(maxExecutors);

    auto* migration = out.mutable_fluxmigration();
    auto load = [](const std::atomic<int64_t>& counter) {
        return counter.load(std::memory_order_relaxed);
    };
    migration->set_handovers(load(fluxCounters.handovers));
    migration->set_failed(load(fluxCounters.failed));
    migration->set_outunits(load(fluxCounters.outUnits));
    migration->set_outstatereqs(load(fluxCounters.outStateReqs));
    migration->set_outstateless(load(fluxCounters.outStateless));
    migration->set_outstatebytes(load(fluxCounters.outStateBytes));
    migration->set_migrateus(load(fluxCounters.migrateUs));
    migration->set_inunits(load(fluxCounters.inUnits));
    migration->set_instatereqs(load(fluxCounters.inStateReqs));
    migration->set_instateless(load(fluxCounters.inStateless));
    migration->set_instatebytes(load(fluxCounters.inStateBytes));
    migration->set_fwdtombstone(load(fluxCounters.fwdTombstone));
    migration->set_fwdowner(load(fluxCounters.fwdOwner));
    migration->set_rerouted(load(fluxCounters.rerouted));
    migration->set_costus(fluxMigrationCostUs.load());

    // Units of state held here: every shard, and every paridx of a stateful
    // operator. Stateless operators are in the known set too, without state.
    std::vector<std::string> known;
    {
        faabric::util::SharedLock lock(fluxKnownUnitsMx);
        known.assign(fluxKnownUnits.begin(), fluxKnownUnits.end());
    }
    int held = 0;
    for (const auto& unitKey : known) {
        auto [userFuncPar, shardId] =
          faabric::batch_scheduler::StateAwareScheduler::splitFluxUnitKey(
            unitKey);
        if (shardId >= 0) {
            held++;
            continue;
        }
        auto [user, func, par] = faabric::util::splitUserFuncPar(userFuncPar);
        auto node = decentralScheduler.lookupNode(user + "_" + func);
        if (node != nullptr &&
            node->type != faabric::batch_scheduler::NodeType::STATELESS) {
            held++;
        }
    }
    migration->set_heldunits(held);
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
        lock.unlock();
        chainedWake.notify();
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

    // ModeFlux: requests for units we do not hold, by their owner.
    std::map<std::string, std::list<std::unique_ptr<faabric::Message>>>
      fluxForwards;

    while (!msgs.empty()) {
        std::unique_ptr<faabric::Message> msgPtr = std::move(msgs.front());
        msgs.pop_front();
        faabric::Message& msg = *msgPtr;
        std::string userFuncPar = msg.user() + "_" + msg.function() + "_" +
                                  std::to_string(msg.parallelismid());

        if (scheduleMode == faabric::batch_scheduler::ModeFlux) {
            std::string unitKey =
              faabric::batch_scheduler::StateAwareScheduler::fluxUnitKey(msg);
            // Before admitMessageFlux: a unit mid-migration or already gone
            // must not be recreated here.
            if (divertMessageFlux(msgPtr, unitKey, fluxForwards)) {
                continue;
            }
            std::string owner = admitMessageFlux(msg, userFuncPar, unitKey);
            if (!owner.empty()) {
                countForward(msg);
                fluxCounters.fwdOwner.fetch_add(1, std::memory_order_relaxed);
                fluxForwards[owner].push_back(std::move(msgPtr));
                continue;
            }
        }

        instancesCounter[userFuncPar]++;
        (*msg.mutable_metricrecorder())[WORKER_ENQUEUE_TIME_KEY] = current;
        msg.set_starttimestamp(currentMillis);
        msg.set_dispatchreceivetime(current);
        msg.set_executedhost(endPoint);

        faabric::util::BatchQueueBase* targetQueue = nullptr;

        {
            faabric::util::SharedLock readLock(waitingQueuesMx);
            auto it = waitingQueues.find(userFuncPar);
            if (it != waitingQueues.end()) {
                targetQueue = it->second.get();
            }
        }
        if (targetQueue == nullptr) {
            faabric::util::FullLock writeLock(waitingQueuesMx);
            // ModeFlux serves the oldest input first, wherever its work waits.
            bool orderByInput =
              scheduleMode == faabric::batch_scheduler::ModeFlux;
            auto [iterator, inserted] = waitingQueues.emplace(
              userFuncPar,
              std::make_unique<faabric::util::BatchQueue>(
                userFuncPar, executeBatchsize, orderByInput));
            targetQueue = iterator->second.get();
        }

        int waitMsgs = targetQueue->getMessagesCount();
        (*msg.mutable_metricrecorder())[WORKER_ENQUEUE_SIZE_KEY] = waitMsgs;

        // Thread Safe Queue Push
        targetQueue->addMessage(std::move(msgPtr));
    }

    // Update the instances runtime stats
    for (const auto& [instancesName, count] : instancesCounter) {
        runtimeStats.instanceAdd(instancesName, invokeHost, count);
    }

    // Something to dispatch in every queue that received requests.
    for (const auto& [userFuncPar, count] : instancesCounter) {
        markDispatchPending(userFuncPar);
    }

    for (auto& [host, forwardMsgs] : fluxForwards) {
        SPDLOG_DEBUG(
          "Flux forwards {} requests for units it does not hold to {}",
          forwardMsgs.size(),
          host);
        faabric::scheduler::getFunctionCallClient(host)->executeFunctionsBatch(
          std::move(forwardMsgs));
    }

    SPDLOG_DEBUG("Enqueued {} messages completed", nMessages);
}

void Scheduler::markDispatchPending(const std::string& userFuncPar)
{
    {
        std::lock_guard<std::mutex> lock(dispatchMx);
        dispatchPending.try_emplace(
          userFuncPar, faabric::util::getGlobalClock().epochMicros());
    }
    dispatchCv.notify_one();
}

void Scheduler::markAllDispatchPending()
{
    std::vector<std::string> nonEmpty;
    {
        faabric::util::SharedLock lock(waitingQueuesMx);
        for (const auto& [userFuncPar, queue] : waitingQueues) {
            if (queue->getMessagesCount() > 0) {
                nonEmpty.push_back(userFuncPar);
            }
        }
    }
    if (nonEmpty.empty()) {
        return;
    }
    {
        std::lock_guard<std::mutex> lock(dispatchMx);
        int64_t nowUs = faabric::util::getGlobalClock().epochMicros();
        for (const auto& userFuncPar : nonEmpty) {
            dispatchPending.try_emplace(userFuncPar, nowUs);
        }
    }
    dispatchCv.notify_all();
}

std::string Scheduler::pickDispatchQueue()
{
    using OrderKey = faabric::util::BatchQueue::OrderKey;
    bool byInput = scheduleMode == faabric::batch_scheduler::ModeFlux;

    std::string picked;
    OrderKey pickedHead{ INT64_MAX, INT64_MAX };
    int64_t pickedSince = INT64_MAX;
    faabric::util::SharedLock queuesLock(waitingQueuesMx);
    for (const auto& [userFuncPar, since] : dispatchPending) {
        if (dispatchActive.contains(userFuncPar)) {
            continue;
        }
        if (byInput) {
            // ModeFlux: the queue whose next request descends from the
            // oldest input goes first.
            auto it = waitingQueues.find(userFuncPar);
            OrderKey head = OrderKey{ INT64_MAX, INT64_MAX };
            if (it != waitingQueues.end()) {
                head = it->second->headKey().value_or(head);
            }
            if (picked.empty() || head < pickedHead) {
                picked = userFuncPar;
                pickedHead = head;
            }
        } else if (picked.empty() || since < pickedSince) {
            picked = userFuncPar;
            pickedSince = since;
        }
    }

    if (!picked.empty()) {
        dispatchPending.erase(picked);
        dispatchActive.insert(picked);
    }
    return picked;
}

void Scheduler::dispatchWorkerLoop()
{
    while (true) {
        std::string userFuncPar;
        {
            std::unique_lock<std::mutex> lock(dispatchMx);
            auto hasWork = [this]() {
                if (isUpdateState.load(std::memory_order_acquire)) {
                    return false;
                }
                for (const auto& [key, since] : dispatchPending) {
                    if (!dispatchActive.contains(key)) {
                        return true;
                    }
                }
                return false;
            };
            bool woken = dispatchCv.wait_for(
              lock, SAFETY_WAKE, [&] { return stopDispatcher || hasWork(); });
            if (stopDispatcher) {
                break;
            }
            if (!woken) {
                // Nothing for a while. In case an event was missed, look at
                // every queue once.
                lock.unlock();
                markAllDispatchPending();
                continue;
            }
            userFuncPar = pickDispatchQueue();
        }
        if (userFuncPar.empty()) {
            continue;
        }

        faabric::util::BatchQueue* queue = nullptr;
        {
            faabric::util::SharedLock lock(waitingQueuesMx);
            auto it = waitingQueues.find(userFuncPar);
            if (it != waitingQueues.end()) {
                queue = it->second.get();
            }
        }
        bool moreNow =
          queue != nullptr && executeBatchForQueue(userFuncPar, *queue);

        {
            std::lock_guard<std::mutex> lock(dispatchMx);
            dispatchActive.erase(userFuncPar);
            if (moreNow) {
                dispatchPending.try_emplace(
                  userFuncPar, faabric::util::getGlobalClock().epochMicros());
            }
        }
        // It may have been marked again while we worked on it, and only now
        // can another dispatcher take it.
        dispatchCv.notify_one();
    }
}

bool Scheduler::executeBatchForQueue(const std::string& userFuncPar,
                                     util::BatchQueueBase& waitingQueue)
{
    if (isUpdateState.load(std::memory_order_acquire)) {
        return false;
    }

    auto userFuncParTuple = util::splitUserFuncPar(userFuncPar);

    std::string user = std::get<0>(userFuncParTuple);
    std::string func = std::get<1>(userFuncParTuple);
    std::string par = std::get<2>(userFuncParTuple);

    std::string funcStr = user + "/" + func + "/" + par;

    auto loopDeadlineMs =
      faabric::util::getGlobalClock().epochMillis() + batchCheckPeriod;

    // ModeFlux: every request in a stateful queue already carries its unit --
    // its paridx, plus its shard if partitioned -- and must run against that
    // unit: it is the one whose lock the batch holds, so it is the one a
    // migration waits for. Re-scheduling would re-shuffle a non-partitioned
    // request onto some other paridx, so only ask who owns its unit now.
    bool fluxStatefulQueue = false;
    if (scheduleMode == faabric::batch_scheduler::ModeFlux) {
        auto node = decentralScheduler.lookupNode(user + "_" + func);
        fluxStatefulQueue =
          node != nullptr &&
          node->type != faabric::batch_scheduler::NodeType::STATELESS;
    }

    // Out of time with requests left: hand the queue back so the others get
    // a turn, and pick it up again straight after.
    bool outOfTime = false;
    while (waitingQueue.getMessagesCount() != 0) {

        if (faabric::util::getGlobalClock().epochMillis() >= loopDeadlineMs) {
            outOfTime = true;
            break;
        }

        // ModeFlux takes one of the paridx's run slots before it takes any
        // requests, so that concurrent dispatchers cannot overshoot the cap
        // between checking it and dispatching. The slot is handed back when
        // the batch finishes (notifyExecutorFinished), or right here if no
        // batch ends up running.
        struct ParidxRunSlot
        {
            Scheduler* scheduler;
            const std::string& funcParStr;
            bool held = false;
            ~ParidxRunSlot()
            {
                if (held) {
                    scheduler->releaseParidxRunFlux(funcParStr);
                }
            }
        } runSlot{ this, funcStr };
        if (scheduleMode == faabric::batch_scheduler::ModeFlux) {
            if (!reserveParidxRunFlux(funcStr)) {
                break;
            }
            runSlot.held = true;
        } else if (!executorAvailable(funcStr)) {
            break;
        }

        if (isUpdateState.load(std::memory_order_acquire)) {
            break;
        }

        // Held until the batch has finished executing. The other modes hold
        // stateUpdateMx, which their central state update takes exclusively.
        // ModeFlux never does; what can pull state from under a batch there
        // is a migration of a unit it addresses, so it holds those units'
        // locks instead, taken per request below.
        faabric::util::SharedLockSet stateLocks;
        if (scheduleMode != faabric::batch_scheduler::ModeFlux) {
            stateLocks.push_back(
              std::make_unique<faabric::util::SharedLock>(stateUpdateMx));
            if (isUpdateState.load(std::memory_order_acquire)) {
                break;
            }
        }

        // Double check
        if (waitingQueue.getMessagesCount() == 0) {
            break;
        }

        std::vector<std::unique_ptr<faabric::Message>> msgVec;
        try {
            msgVec = waitingQueue.getMessages();
        } catch (...) {
            break;
        }

        if (msgVec.empty()) {
            break;
        }

        auto newReq = faabric::util::batchExecFactory();
        newReq->set_user(user);
        newReq->set_function(func);

        auto now = faabric::util::getGlobalClock().epochMicros();

        std::map<std::string, std::unique_ptr<faabric::util::SharedLock>>
          unitLocks;
        for (auto& src : msgVec) {
            std::string unitKey;
            std::string host;
            if (fluxStatefulQueue) {
                unitKey =
                  faabric::batch_scheduler::StateAwareScheduler::fluxUnitKey(
                    *src);
                // A unit held here runs here. It was admitted against Redis,
                // the authority on ownership; the routing cache is only a
                // cache, and a stale entry pointing elsewhere would bounce
                // the request between two workers that each think the other
                // holds it. Only a request whose unit is not held here --
                // left behind by a migration -- is routed on.
                bool heldHere = false;
                {
                    faabric::util::SharedLock lock(fluxKnownUnitsMx);
                    heldHere = fluxKnownUnits.contains(unitKey);
                }
                host = heldHere ? thisHost
                                : decentralScheduler.unitOwnerFlux(
                                    unitKey, routableHosts());
            } else if (scheduleMode == faabric::batch_scheduler::ModeFlux) {
                // A stateless request is only dispatched once it has a run
                // slot here, so nowhere could start it sooner: sending it on
                // would only add a hop. Where stateless work runs is decided
                // when it is routed, and by rebalancing.
                host = thisHost;
            } else {
                host =
                  decentralScheduler.scheduleMessage(routableHosts(), *src);
            }
            if (host != thisHost) {
                if (fluxStatefulQueue) {
                    countForward(*src);
                    fluxCounters.rerouted.fetch_add(1,
                                                    std::memory_order_relaxed);
                }
                {
                    faabric::util::FullLock lock(chainedCallMsgsMx);
                    chainedCallMsgs.push_back(std::move(src));
                }
                chainedWake.notify();
                continue;
            }

            // A unit being migrated must not run: leave the request to the
            // migration instead of executing it against state that is leaving.
            if (fluxStatefulQueue &&
                !lockUnitForDispatchFlux(unitKey, unitLocks)) {
                returnUndispatchableFlux(std::move(src), unitKey);
                continue;
            }

            auto* message = newReq->add_messages();
            message->Swap(src.get());

            auto* metrics = message->mutable_metricrecorder();
            int workerQueueTime = now - (*metrics)[WORKER_ENQUEUE_TIME_KEY];
            message->set_workerqueuewaittime(workerQueueTime);
            metrics->erase(WORKER_ENQUEUE_TIME_KEY);
        }

        if (newReq->messages_size() != 0) {
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

            for (auto& [unitKey, unitLock] : unitLocks) {
                stateLocks.push_back(std::move(unitLock));
            }
            // The running batch owns the run slot from here.
            runSlot.held = false;
            e->executeBatchTasks(newReq, std::move(stateLocks));
            // if (!runningThreads.contains(threadClockId)) {
            //     faabric::util::FullLock cpuLock(cpuRecordMx);
            //     runningThreads.emplace(threadClockId);
            // }
        }
        // Otherwise nothing runs, and the locks go with this iteration.
    }

    try {
        if (waitingQueue.getMessagesCount() == 0) {
            waitingQueue.resetLastTime();
        }
    } catch (...) {
    }

    return outOfTime && waitingQueue.getMessagesCount() > 0;
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
    lock.unlock();
    chainedWake.notify();
    SPDLOG_DEBUG("Enqueueing chained calls finished");
}

void Scheduler::setMessageResults()
{
    while (!stopBatchTimer) {
        // Report results as soon as there are some; whatever finishes while
        // a report is in flight goes with the next one.
        resultsWake.wait(SAFETY_WAKE);

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

        // If the message doesn't have the worker enqueue size record, we set it
        // to 1 to avoid affecting the update of runtime stats.
        int waitMsgSize = 1;
        if (msg.metricrecorder().contains(WORKER_ENQUEUE_SIZE_KEY)) {
            waitMsgSize = static_cast<int>(
              msg.metricrecorder().at(WORKER_ENQUEUE_SIZE_KEY));
        }
        int queueWaitTime = msg.workerqueuewaittime();

        runtimeStats.updateInstanceExecutionMetrics(
          userFuncPar, queueWaitTime, waitMsgSize, workerExecuteTime);

        setResultMsgs.emplace_back(std::make_unique<faabric::Message>(msg));
    }
    lock.unlock();
    resultsWake.notify();
    SPDLOG_DEBUG("Enqueueing set results finished");
}

// void Scheduler::batchTimerCheck()
// {
//     while (!stopBatchTimer) {
//         std::this_thread::sleep_for(
//           std::chrono::milliseconds(batchCheckPeriod));

//         if (stopBatchTimer) {
//             break;
//         }

//         if (isUpdateState.load(std::memory_order_acquire)) {
//             continue;
//         }
//         faabric::util::SharedLock lock(mx);
//         if (isUpdateState.load(std::memory_order_acquire)) {
//             continue;
//         }

//         for (auto& [userFuncPar, waitingBatch] : waitingQueues) {
//             if (waitingBatch->getMessagesCount() == 0) {
//                 continue;
//             }

//             if (waitingBatch->getMessagesCount() >= executeBatchsize ||
//                 waitingBatch->getTimeInterval() >= batchInterval) {
//                 for (int i = 0; i < 2; ++i) {
//                     readyDispatchQueue.enqueue(userFuncPar);
//                 }
//                 waitingBatch->resetLastTime();
//             }
//         }
//     }
// }

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
        // Route chained calls as soon as there are some; whatever arrives
        // while a round is being routed and sent goes with the next one.
        chainedWake.wait(SAFETY_WAKE);

        if (stopThreadTimer) {
            break;
        }

        if (isUpdateState.load(std::memory_order_acquire)) {
            continue;
        }

        /***
         * CPU usage recording
         ***/
        // faabric::util::FullLock cpuLock(cpuRecordMx);
        // auto nowWall = std::chrono::steady_clock::now();
        // if (nowWall - cpuRecordStart >= cpuRecordWindow) {
        //     const auto wallNs =
        //       std::chrono::duration_cast<std::chrono::nanoseconds>(
        //         nowWall - cpuRecordStart)
        //         .count();
        //     long long totalDeltaNs = 0;

        //     for (const auto& [clk, startNs] : threadClockStartMap) {
        //         const int64_t endNs = faabric::util::getCpuTimeNano(clk);
        //         if (endNs >= 0 && startNs >= 0 && endNs >= startNs) {
        //             const int64_t deltaNs = endNs - startNs;
        //             totalDeltaNs += deltaNs;
        //         }
        //     }

        //     if (wallNs > 0 && totalDeltaNs > 0) {
        //         const double cpuExecutePct =
        //           100.0 * (double)totalDeltaNs / (double)wallNs;
        //         const int64_t cpuScheduleNs =
        //           cpuScheduleTime.exchange(0, std::memory_order_acq_rel);
        //         const double cpuSchedulePct =
        //           100.0 * (double)cpuScheduleNs / (double)wallNs;

        //         SPDLOG_DEBUG(
        //           "CPU execute percentage: {:.2f}%, CPU schedule percentage:
        //           "
        //           "{:.2f}%",
        //           cpuExecutePct,
        //           cpuSchedulePct);

        //         cpuRecordHistory.emplace(
        //           std::make_tuple(cpuExecutePct, cpuSchedulePct));
        //         while (cpuRecordHistory.size() > historyCap) {
        //             cpuRecordHistory.pop();
        //         }
        //     }
        //     threadClockStartMap.clear();

        //     for (const clockid_t clk : runningThreads) {
        //         const int64_t nowNs = faabric::util::getCpuTimeNano(clk);
        //         if (nowNs >= 0) {
        //             threadClockStartMap.emplace(clk, nowNs);
        //         }
        //     }
        //     cpuRecordStart = nowWall;
        // }
        // cpuLock.unlock();
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

        // faabric::util::FullLock mxlock(mx);
        // Otherwise, decentralized scheduler is used, we schedule the chained
        // MAP<UserFuncPar, <Host, Count>>
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
            // auto start = faabric::util::getCpuTimeNano();
            auto hosts = decentralScheduler.scheduleMessagesBatch(
              routableHosts(), localChainedCallMsgs);
            // auto end = faabric::util::getCpuTimeNano();
            // if (start > 0 && end >= start) {
            //     cpuScheduleTime.fetch_add(end - start,
            //                               std::memory_order_relaxed);
            // }

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
            msgsCallMap[hostIp] = std::move(msgsList);
        }
        scheduledMsgsMap.clear();
        lock.unlock();

        for (auto& [hostIp, msgs] : msgsCallMap) {
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
        for (auto& [userFuncPar, waitingBatch] : waitingQueues) {
            waitingBatch->resetBatchSize(executeBatchsize);
        }
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
    } else if (key == "flux_local_queue_limit" ||
               key == "flux_offload_margin") {
        // Stateless routing is by expected wait now; see Planner.
        SPDLOG_WARN("{} is no longer used by ModeFlux; ignored", key);
    } else if (key == "flux_partition_shards") {
        decentralScheduler.setFluxPartitionShards(value);
        SPDLOG_INFO("Reset flux_partition_shards parameter to : {}", value);
    } else if (key == "flux_rebalance_period") {
        fluxRebalancePeriod = value;
        SPDLOG_INFO("Reset flux_rebalance_period parameter to : {}", value);
    } else if (key == "flux_migrate_cooldown") {
        fluxMigrateCooldown = value;
        SPDLOG_INFO("Reset flux_migrate_cooldown parameter to : {}", value);
    } else if (key == "flux_migrate_requests") {
        fluxMigrateRequests = value;
        SPDLOG_INFO("Reset flux_migrate_requests parameter to : {}", value);
    } else if (key == "flux_worker_slots") {
        fluxWorkerSlots = value;
        SPDLOG_INFO("Reset flux_worker_slots parameter to : {}", value);
    } else if (key == "alpha") {
        double newAlpha = value / 1000.0;
        SPDLOG_INFO("Alpha is set to {}", newAlpha);
        decentralScheduler.setAlpha(newAlpha);
    } else if (key == "persistent_lock") {
        faabric::state::getGlobalState().resetPersistentLockState();
        faabric::state::getGlobalState().persistentLock = (value == 1);
        SPDLOG_INFO("Persistent lock: {}", value == 1 ? "ON" : "OFF");
    } else if (key == "access_state_remote") {
        faabric::state::getGlobalState().accessRemote = (value == 1);
        SPDLOG_INFO("Access state remote: {}", value == 1 ? "ON" : "OFF");
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
    // ModeFlux has no planner-derived replica budget: nothing pins operators
    // to workers, so there is no weight distribution to split one out of.
    // maxExecutors is applied per paridx instead -- funcStr is
    // user/func/parIdx -- so each paridx scales on demand up to the same
    // ceiling and no paridx can hold another one's queue shut by filling a
    // worker-wide budget. The executors themselves come from the operator's
    // shared pool, so the cap counts running batches, not pool size. The
    // reaper hands back whatever sits idle past BOUND_TIMEOUT.
    if (scheduleMode == faabric::batch_scheduler::ModeFlux) {
        std::lock_guard<std::mutex> lock(fluxParidxRunMx);
        auto it = fluxParidxLoad.find(funcStr);
        return it == fluxParidxLoad.end() || it->second.running < maxExecutors;
    }

    int currentExecutorsSize = 0;
    bool foundAvailable = false;

    {
        faabric::util::SharedLock lock(mx);
        // NB - find() not operator[]: the latter default-inserts, which is a
        // write to the map under a shared lock, racing every other reader and
        // the push_back in claimExecutor.
        auto it = executors.find(funcStr);
        if (it != executors.end()) {
            currentExecutorsSize = it->second.size();

            for (auto& e : it->second) {
                if (e->availableClaim()) {
                    foundAvailable = true;
                    break;
                }
            }
        }
    }

    if (foundAvailable) {
        return true;
    }

    int maxReplicas;
    try {
        maxReplicas = util::getOrThrow(maxReplicasMap, funcStr);
    } catch (const std::runtime_error& e) {
        maxReplicas = 1;
    }

    if (currentExecutorsSize < maxReplicas) {
        return true;
    } else {
        return false;
    }
}

void Scheduler::notifyExecutorStart()
{
    int current = runningExecutors.fetch_add(1, std::memory_order_relaxed) + 1;
    int64_t now = std::chrono::duration_cast<std::chrono::seconds>(
                    std::chrono::steady_clock::now().time_since_epoch())
                    .count();

    int64_t window = currentWindowSec.load(std::memory_order_acquire);

    if (now > window) {
        if (currentWindowSec.compare_exchange_strong(window, now)) {
            long long oldSum =
              currentSecondSum.exchange(0, std::memory_order_relaxed);
            long long oldCount =
              currentSecondCount.exchange(0, std::memory_order_relaxed);

            if (oldCount > 0) {
                lastSecondAverage.store(static_cast<double>(oldSum) / oldCount,
                                        std::memory_order_relaxed);
            } else {
                lastSecondAverage.store(current, std::memory_order_relaxed);
            }
        }
    }

    currentSecondSum.fetch_add(current, std::memory_order_relaxed);
    currentSecondCount.fetch_add(1, std::memory_order_relaxed);
}

void Scheduler::notifyExecutorFinished(const faabric::Message& msg)
{
    // The batch held one of its paridx's run slots since it was dispatched.
    if (scheduleMode == faabric::batch_scheduler::ModeFlux) {
        finishParidxRunFlux(faabric::util::funcParToString(msg, false),
                            msg.executebatchsize());
    }

    int current = runningExecutors.load(std::memory_order_relaxed);

    while (current > 0) {
        if (runningExecutors.compare_exchange_weak(
              current, current - 1, std::memory_order_relaxed)) {
            break;
        }
    }

    // Its run slot and executor are free again, so its queue may dispatch.
    // Only now: a dispatcher woken any earlier would find neither free, and
    // nothing would wake it again.
    markDispatchPending(faabric::util::getUserFuncPar(msg));
}

double Scheduler::getAverageExecutors() const
{
    int64_t now = std::chrono::duration_cast<std::chrono::seconds>(
                    std::chrono::steady_clock::now().time_since_epoch())
                    .count();
    int64_t window = currentWindowSec.load(std::memory_order_relaxed);

    if (now > window) {
        return runningExecutors.load(std::memory_order_relaxed);
    }
    return lastSecondAverage.load(std::memory_order_relaxed);
}

std::shared_ptr<faabric::executor::Executor> Scheduler::claimExecutor(
  faabric::Message& msg)
{
    std::string funcStr = executorPoolKey(msg);
    auto factory = faabric::executor::getExecutorFactory();
    std::shared_ptr<faabric::executor::Executor> claimed = nullptr;

    {
        faabric::util::FullLock lock(mx);
        auto& thisExecutors = executors[funcStr];

        for (auto& e : thisExecutors) {
            if (e->tryClaim()) {
                claimed = e;
                claimed->reset(msg);
                SPDLOG_DEBUG(
                  "Reusing warm executor {} for {}", claimed->id, funcStr);
                break;
            }
        }
    }

    if (claimed != nullptr) {
        return claimed;
    }

    SPDLOG_DEBUG("Scaling {} -> creating new executor", funcStr);
    auto executor = factory->createExecutor(msg);

    {
        faabric::util::FullLock lock(mx);
        auto& thisExecutors = executors[funcStr];

        thisExecutors.push_back(std::move(executor));
        claimed = thisExecutors.back();
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
    faabric::util::FullLock lock(mx);
    threadResultMessages.insert(std::make_pair(msgId, std::move(message)));
}

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

    // std::ostringstream oss;
    // oss << "Registered hosts:\n";
    // for (const auto& [key, value] : registeredHostsMap) {
    //     oss << key << "=" << value << ";\n";
    // }
    // SPDLOG_INFO("updateHosts: {}", oss.str());

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
        activeHosts.clear();
        for (const auto& ip : uniqueIps) {
            auto it = hostMap.find(ip);
            if (it != hostMap.end()) {
                activeHosts[ip] = it->second;
            }
        }
    } else {
        activeHosts = hostMap;
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
    SPDLOG_DEBUG("State Update: Starting state update for migration version {}",
                migrationVersion);
    isUpdateState.store(true, std::memory_order_release);
    faabric::util::FullLock stateLock(stateUpdateMx);
    SPDLOG_DEBUG("State Update: Acquired stateUpdateMx lock");
    faabric::util::FullLock lock(mx);
    faabric::util::FullLock rflock(reconfigMx);
    SPDLOG_DEBUG("State Update: Acquired mx and reconfigMx locks");
    // 1. Update the local max replicas map based on the new scheduling
    // decision.
    calculateMaxReplicas(scheduledOperatorMap);
    // 2. Update the local scheduler and update the states info in decentralized
    // scheduler
    updateActiveHosts(scheduledOperatorMap);
    decentralScheduler.resetScheduler();
    decentralScheduler.setScheuduledOperatorMap(scheduledOperatorMap);
    // update runtime summary and states info.
    decentralScheduler.syncStatesInfo(statesInfo);

    // If it's initialization, we don't need to migrate. Just return after
    // updating the states info.
    if (isInitialization) {
        createLocalState(statesInfo);
        isUpdateState = false;
        // Dispatching was paused for the update: resume it.
        markAllDispatchPending();
        chainedWake.notify();
        SPDLOG_DEBUG(
          "State Update: initialization complete, no migration needed");
        return;
    }

    currentMigrationVersion = migrationVersion;
    std::set<std::string> migrationDestinations = transDestinationMap[thisHost];
    std::set<std::string> migrationSources = transSourceMap[thisHost];

    // 3. Prepare the migration request: state and in-flight messages

    SPDLOG_DEBUG("State Update: Migration data start");

    auto migrationStatesMap = packState(statesInfo);
    auto migrationMessagesMap = packMessage();

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
        migrationCv.wait_for(
          lock,
          std::chrono::seconds(10),
          [this, migrationVersion, expectedCount] {
              return receivedMigrationSources[migrationVersion].size() >=
                     expectedCount;
          });
    }

    createLocalState(statesInfo);

    faabric::util::FullLock migrateStateLock(migratedStateMapMx);
    auto& stateServer = faabric::state::getGlobalState();
    stateServer.loadMigrateState(migratedStateMap);

    SPDLOG_DEBUG(
      "State Update: states reallocation complete, reschedule requests now");

    isUpdateState = false;
    // Dispatching was paused for the update: resume it.
    markAllDispatchPending();
    chainedWake.notify();

    while (!migratedMsgs.empty()) {
        auto msgBatch = migratedMsgs.dequeue();
        enqueueMessageBatch(std::move(msgBatch));
    }

    receivedMigrationSources.erase(migrationVersion);
}

void Scheduler::calculateMaxReplicas(
  const std::map<std::string, faabric::batch_scheduler::ScheduledOperator>&
    scheduledOperatorMap)
{
    maxReplicasMap.clear();

    // ModeFlux derives no per-function budgets. executorAvailable scales on
    // demand against maxExecutors instead, so leaving the map empty is the
    // intended state rather than a missing step.
    if (scheduleMode == faabric::batch_scheduler::ModeFlux) {
        return;
    }

    // Weight-derived budgets, renormalised below so they sum to maxExecutors.
    std::map<std::string, int> tempMaxReplicasMap;
    // Planner-supplied absolute budgets (Binpack mode 0), used verbatim.
    std::map<std::string, int> absoluteReplicasMap;

    // Spreads an absolute per-host budget over that host's instances of one
    // operator, remainder to the lowest parallelism ids, at least one each.
    auto splitAbsolute = [&](const std::string& userFunc,
                             const std::map<int, std::string>& parallelismDist,
                             int hostBudget) {
        int localInstances = 0;
        for (const auto& [parId, ip] : parallelismDist) {
            if (ip == thisHost) {
                localInstances++;
            }
        }
        if (localInstances == 0) {
            return;
        }
        int base = hostBudget / localInstances;
        int remainder = hostBudget % localInstances;
        int seen = 0;
        for (const auto& [parId, ip] : parallelismDist) {
            if (ip != thisHost) {
                continue;
            }
            int maxReplica = base + (seen < remainder ? 1 : 0);
            if (maxReplica < 1) {
                maxReplica = 1;
            }
            absoluteReplicasMap[userFunc + "/" + std::to_string(parId)] =
              maxReplica;
            seen++;
        }
    };

    for (const auto& [operatorName, operatorInfo] : scheduledOperatorMap) {
        if (operatorInfo.weightDist.count(thisHost) <= 0) {
            continue;
        }
        std::string userFunc = util::splitUserFunc(operatorName).first + "/" +
                               util::splitUserFunc(operatorName).second;

        auto execIt = operatorInfo.executorDist.find(thisHost);
        bool absolute = execIt != operatorInfo.executorDist.end();
        int hostBudget = absolute ? std::max(1, execIt->second) : 0;

        if (operatorInfo.node.type == faabric::batch_scheduler::STATELESS) {
            if (absolute) {
                absoluteReplicasMap[userFunc + "/0"] = hostBudget;
            } else {
                int maxReplica = std::round(
                  operatorInfo.weightDist.at(thisHost) * maxExecutors);
                tempMaxReplicasMap[userFunc + "/0"] = maxReplica;
            }
        } else if (operatorInfo.node.type ==
                   faabric::batch_scheduler::STATEFUL) {
            if (absolute) {
                splitAbsolute(
                  userFunc, operatorInfo.parallelismDist, hostBudget);
            } else {
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
                        int maxReplica =
                          std::round(instanceWeight * maxExecutors);
                        tempMaxReplicasMap[userFunc + "/" +
                                           std::to_string(parId)] = maxReplica;
                    }
                }
            }
        } else if (operatorInfo.node.type ==
                   faabric::batch_scheduler::PARTITIONED_STATEFUL) {
            if (absolute) {
                splitAbsolute(
                  userFunc, operatorInfo.parallelismDist, hostBudget);
            } else {
                for (const auto& [parId, ip] : operatorInfo.parallelismDist) {
                    if (ip == thisHost) {
                        double weight = operatorInfo.weightDist.at(thisHost);
                        int maxReplica = std::round(weight * maxExecutors);
                        tempMaxReplicasMap[userFunc + "/" +
                                           std::to_string(parId)] = maxReplica;
                    }
                }
            }
        }
    }

    int absoluteTotal = 0;
    for (const auto& [funcStr, maxReplica] : absoluteReplicasMap) {
        maxReplicasMap[funcStr] = maxReplica;
        absoluteTotal += maxReplica;
    }

    // Renormalise whatever is left on the weight-derived path so the host's
    // replicas add up to maxExecutors, minus what the absolute budgets already
    // claimed. Absolute budgets are deliberately not rescaled: the planner
    // sized them against measured throughput and may legitimately exceed
    // maxExecutors.
    int totalReplicas = 0;
    for (const auto& [funcStr, maxReplica] : tempMaxReplicasMap) {
        totalReplicas += maxReplica;
    }
    if (totalReplicas > 0) {
        int remainingBudget = std::max(1, maxExecutors - absoluteTotal);
        double scaleFactor = static_cast<double>(remainingBudget) /
                             static_cast<double>(totalReplicas);
        for (const auto& [funcStr, maxReplica] : tempMaxReplicasMap) {
            int scaledMaxReplica =
              std::ceil(static_cast<double>(maxReplica) * scaleFactor);
            if (scaledMaxReplica <= 0) {
                scaledMaxReplica = 1;
            }
            maxReplicasMap[funcStr] = scaledMaxReplica;
        }
    }

    if (absoluteTotal > maxExecutors) {
        SPDLOG_WARN("Planner executor budget for this host is {} but "
                    "maxExecutors is {}; honouring the planner",
                    absoluteTotal,
                    maxExecutors);
    }
}

void Scheduler::logFluxExecutorCensus(const std::string& blockedFunc)
{
    auto nowMs = faabric::util::getGlobalClock().epochMillis();
    auto last = lastFluxExecutorLogMs.load(std::memory_order_relaxed);
    if (nowMs - last < 1000) {
        return;
    }
    if (!lastFluxExecutorLogMs.compare_exchange_strong(last, nowMs)) {
        // Another thread is logging this second.
        return;
    }

    // Ordered, so the same operator sits in the same place from one line to
    // the next; `executors` itself is unordered. Under ModeFlux each pool
    // serves a whole operator.
    std::map<std::string, std::pair<int, int>> census; // op -> {free, total}
    int total = 0;
    {
        faabric::util::SharedLock lock(mx);
        for (const auto& [funcStr, execs] : executors) {
            int free = 0;
            for (const auto& e : execs) {
                if (e->availableClaim()) {
                    free++;
                }
            }
            census[funcStr] = { free, static_cast<int>(execs.size()) };
            total += static_cast<int>(execs.size());
        }
    }

    std::ostringstream oss;
    bool first = true;
    for (const auto& [funcStr, counts] : census) {
        if (!first) {
            oss << ", ";
        }
        first = false;
        oss << funcStr << " " << counts.first << "/" << counts.second;
    }

    SPDLOG_DEBUG("Flux {} at its cap of {} running batches on {}; worker holds "
                 "{} executors [free/total per operator]: {}",
                 blockedFunc,
                 maxExecutors,
                 thisHost,
                 total,
                 oss.str());
}

namespace {
using faabric::batch_scheduler::StateAwareScheduler;

// Ownership of a unit of state lives in Redis; its epoch sits next to it and
// only ever grows. See StateAwareScheduler::fluxOwnerKey.
std::string fluxOwnerKey(const std::string& unitKey)
{
    return StateAwareScheduler::fluxOwnerKey(unitKey);
}

std::string fluxEpochKey(const std::string& unitKey)
{
    return StateAwareScheduler::fluxEpochKey(unitKey);
}
}

std::string Scheduler::admitMessageFlux(const faabric::Message& msg,
                                        const std::string& userFuncPar,
                                        const std::string& unitKey)
{
    {
        faabric::util::SharedLock lock(fluxKnownUnitsMx);
        if (fluxKnownUnits.contains(unitKey)) {
            return "";
        }
    }

    // The DAG is the only description of an operator under ModeFlux, and
    // every worker holds one from registerApp.
    std::string userFunc = msg.user() + "_" + msg.function();
    auto node = decentralScheduler.lookupNode(userFunc);
    if (node == nullptr ||
        node->type == faabric::batch_scheduler::NodeType::STATELESS) {
        // Stateless, or not an operator we know. Remember that so the lookup
        // above short-circuits every later message for it.
        faabric::util::FullLock lock(fluxKnownUnitsMx);
        fluxKnownUnits.insert(unitKey);
        return "";
    }
    bool isPartitioned = StateAwareScheduler::fluxIsPartitioned(*node);

    // Redis decides who holds the unit. Routers claim every unit of an
    // operator before routing to any of it, so claiming here only ever wins
    // for a unit nobody has claimed -- and then the request is already here.
    std::string owner;
    try {
        owner =
          redis::Redis::getState().claimOrGet(fluxOwnerKey(unitKey), thisHost);
    } catch (const std::exception& e) {
        SPDLOG_ERROR(
          "Flux could not read the owner of {}: {}", unitKey, e.what());
        return "";
    }
    if (owner != thisHost) {
        SPDLOG_DEBUG("Flux {} does not hold {}, forwarding to its owner {}",
                     thisHost,
                     unitKey,
                     owner);
        return owner;
    }

    faabric::util::FullLock lock(fluxKnownUnitsMx);
    // Another thread may have admitted it while we were in Redis.
    if (fluxKnownUnits.contains(unitKey)) {
        return "";
    }
    try {
        // A partitioned paridx is split by shard across workers, so nobody
        // owns it as a whole and the per-paridx ownership check cannot apply.
        // Keep any copy already here either way: for a partitioned paridx it
        // may hold other shards' keys, some of which arrived by migration.
        faabric::state::getGlobalState().getOrCreateFS(msg.user(),
                                                       msg.function(),
                                                       msg.parallelismid(),
                                                       isPartitioned,
                                                       !isPartitioned);
    } catch (const std::exception& e) {
        SPDLOG_ERROR("Flux could not create state for {} on {}: {}",
                     unitKey,
                     thisHost,
                     e.what());
        return "";
    }
    fluxKnownUnits.insert(unitKey);
    SPDLOG_DEBUG("Flux admits {} on {}", unitKey, thisHost);
    return "";
}

// ----------------------------------
// ModeFlux rebalancing
// ----------------------------------

std::shared_ptr<std::shared_mutex> Scheduler::getFluxUnitLock(
  const std::string& unitKey)
{
    {
        faabric::util::SharedLock lock(fluxUnitLocksMx);
        auto it = fluxUnitLocks.find(unitKey);
        if (it != fluxUnitLocks.end()) {
            return it->second;
        }
    }

    // Entries are never erased: a running batch holds its lock through the
    // shared_ptr, and the map is bounded by the number of units anyway.
    faabric::util::FullLock lock(fluxUnitLocksMx);
    auto [it, inserted] =
      fluxUnitLocks.try_emplace(unitKey, std::make_shared<std::shared_mutex>());
    return it->second;
}

bool Scheduler::divertMessageFlux(
  std::unique_ptr<faabric::Message>& msg,
  const std::string& unitKey,
  std::map<std::string, std::list<std::unique_ptr<faabric::Message>>>& forwards)
{
    if (fluxRouteRecords.load(std::memory_order_acquire) == 0) {
        return false;
    }

    std::lock_guard<std::mutex> lock(fluxMigrationMx);
    if (fluxMigratingUnits.contains(unitKey)) {
        fluxPendingMsgs[unitKey].push_back(std::move(msg));
        return true;
    }

    auto movedIt = fluxMovedUnits.find(unitKey);
    if (movedIt != fluxMovedUnits.end()) {
        countForward(*msg);
        fluxCounters.fwdTombstone.fetch_add(1, std::memory_order_relaxed);
        forwards[movedIt->second.host()].push_back(std::move(msg));
        return true;
    }

    return false;
}

bool Scheduler::lockUnitForDispatchFlux(
  const std::string& unitKey,
  std::map<std::string, std::unique_ptr<faabric::util::SharedLock>>& locks)
{
    if (locks.contains(unitKey)) {
        return true;
    }

    // Lock first, then check: a migration marks the unit before it takes the
    // lock exclusively, so either we see the mark here, or we hold the lock
    // before the migration does and it waits for this batch to finish.
    auto unitLock = std::make_unique<faabric::util::SharedLock>(
      *getFluxUnitLock(unitKey), std::try_to_lock);
    if (!unitLock->owns_lock()) {
        return false;
    }
    if (fluxRouteRecords.load(std::memory_order_acquire) > 0) {
        std::lock_guard<std::mutex> lock(fluxMigrationMx);
        if (fluxMigratingUnits.contains(unitKey) ||
            fluxMovedUnits.contains(unitKey)) {
            return false;
        }
    }

    locks.emplace(unitKey, std::move(unitLock));
    return true;
}

void Scheduler::returnUndispatchableFlux(std::unique_ptr<faabric::Message> msg,
                                         const std::string& unitKey)
{
    std::string forwardTo;
    {
        std::lock_guard<std::mutex> lock(fluxMigrationMx);
        if (fluxMigratingUnits.contains(unitKey)) {
            // The migration forwards it with the unit, or puts it back.
            fluxPendingMsgs[unitKey].push_back(std::move(msg));
            return;
        }
        auto movedIt = fluxMovedUnits.find(unitKey);
        if (movedIt != fluxMovedUnits.end()) {
            forwardTo = movedIt->second.host();
        }
    }

    if (!forwardTo.empty()) {
        countForward(*msg);
        fluxCounters.fwdTombstone.fetch_add(1, std::memory_order_relaxed);
        std::list<std::unique_ptr<faabric::Message>> forward;
        forward.push_back(std::move(msg));
        faabric::scheduler::getFunctionCallClient(forwardTo)
          ->executeFunctionsBatch(std::move(forward));
        return;
    }

    // Neither: the lock was busy for a moment. Back in its queue, and try
    // again.
    std::string userFuncPar = faabric::util::getUserFuncPar(*msg);
    {
        faabric::util::SharedLock lock(waitingQueuesMx);
        auto it = waitingQueues.find(userFuncPar);
        if (it != waitingQueues.end()) {
            it->second->addMessage(std::move(msg));
        }
    }
    markDispatchPending(userFuncPar);
}

std::vector<faabric::ShardMove> Scheduler::getFluxShardMoves()
{
    std::lock_guard<std::mutex> lock(fluxMigrationMx);
    std::vector<faabric::ShardMove> moves;
    moves.reserve(fluxMovedUnits.size());
    for (const auto& [unitKey, move] : fluxMovedUnits) {
        moves.push_back(move);
    }
    return moves;
}

void Scheduler::clearFluxMigrationState()
{
    std::lock_guard<std::mutex> lock(fluxMigrationMx);
    fluxMigratingUnits.clear();
    fluxPendingMsgs.clear();
    fluxMovedUnits.clear();
    fluxUnitCooldownUntilMs.clear();
    fluxRouteRecords.store(0, std::memory_order_release);
}

void Scheduler::fluxRebalanceLoop()
{
    while (!stopThreadTimer) {
        int period = fluxRebalancePeriod > 0 ? fluxRebalancePeriod : 1000;
        // Sleep in short steps so a long period does not hold up shutdown.
        auto wakeAtMs = faabric::util::getGlobalClock().epochMillis() + period;
        while (!stopThreadTimer &&
               faabric::util::getGlobalClock().epochMillis() < wakeAtMs) {
            std::this_thread::sleep_for(std::chrono::milliseconds(50));
        }

        if (stopThreadTimer) {
            break;
        }

        // Close the measurement window whether or not we rebalance: the stats
        // report what it measured.
        rollFluxParidxWindows();

        if (scheduleMode != faabric::batch_scheduler::ModeFlux ||
            fluxRebalancePeriod <= 0) {
            continue;
        }

        try {
            rebalanceFlux();
        } catch (const std::exception& e) {
            SPDLOG_ERROR(
              "Flux rebalancing on {} failed: {}", thisHost, e.what());
        }
    }
}

void Scheduler::rebalanceFlux()
{
    auto stats = decentralScheduler.getClusterWorkerStats();
    if (stats.empty()) {
        return;
    }

    using OrderKey = faabric::util::BatchQueue::OrderKey;

    // The round plans for the time until the next one. A moved request loses
    // the migration cost before it can start on the other side, so only the
    // rest of the horizon is of use there; if the cost is the whole horizon,
    // nothing moved now would be done any sooner.
    double horizonUs =
      (fluxRebalancePeriod > 0 ? fluxRebalancePeriod : 1000) * 1000.0;
    double costUs = fluxMigrationCostUs.load();
    double usableUs = horizonUs - costUs;
    if (usableUs <= 0) {
        SPDLOG_DEBUG("Flux {} does not rebalance: migrating takes {:.0f}us, "
                     "longer than a round",
                     thisHost,
                     fluxMigrationCostUs.load());
        return;
    }

    int slots = std::max(1, fluxWorkerSlots);
    int batchSize = std::max(1, executeBatchsize);
    bool workerFull = runningExecutors.load(std::memory_order_relaxed) >= slots;
    auto loads = snapshotFluxParidxLoads();

    // 1. Paridx that are behind here: more is waiting than they will finish
    // before the next round. Only that excess is worth moving.
    //
    // What a paridx finishes is judged by what it did finish in the last
    // window -- measured, so it reflects the dispatch cadence and CPU
    // contention, which a model of free run slots does not. That only holds
    // for a paridx that had work waiting all along, though: one that was
    // idle a round ago finished only what little it was given. For those, a
    // fresh burst, assume it gets all of its run slots.
    struct FluxBacklog
    {
        std::string userFuncPar;
        std::string userFunc;
        // 0 stateless, 1 stateful (moves as a whole paridx), 2 partitioned
        // (moves by shard).
        int kind;
        int depth;
        double excess;
        double slotTimeUs;
        double ratePerSec;
        OrderKey first;
    };
    std::vector<std::pair<std::string, faabric::util::BatchQueue*>> queues;
    {
        // Queues are only ever erased by reset(), which stops this thread
        // first, so the pointers outlive the lock.
        faabric::util::SharedLock lock(waitingQueuesMx);
        for (const auto& [userFuncPar, queue] : waitingQueues) {
            queues.emplace_back(userFuncPar, queue.get());
        }
    }

    std::map<std::string, int> previousDepths = std::move(fluxPreviousDepths);
    fluxPreviousDepths.clear();

    std::vector<FluxBacklog> backlogs;
    for (const auto& [userFuncPar, queue] : queues) {
        int depth = queue->getMessagesCount();
        if (depth == 0) {
            continue;
        }
        fluxPreviousDepths[userFuncPar] = depth;

        FluxParidxLoad load;
        auto loadIt = loads.find(fluxFuncParStrOf(userFuncPar));
        if (loadIt != loads.end()) {
            load = loadIt->second;
        }
        double slotTimeUs =
          load.slotTimeUs > 0
            ? load.slotTimeUs
            : StateAwareScheduler::clusterSlotTimeUs(stats, userFuncPar);
        if (slotTimeUs <= 0) {
            // Nothing measured, here or anywhere: no basis for a plan.
            continue;
        }
        bool waitingAllAlong = previousDepths.contains(userFuncPar);
        // All its run slots, unless the worker has no executor slot to spare
        // for more than it already runs.
        int runSlots = workerFull ? std::max(1, load.running) : maxExecutors;
        double ratePerSec = waitingAllAlong && load.ratePerSec > 0
                              ? load.ratePerSec
                              : runSlots * 1e6 / slotTimeUs;
        double excess = depth - ratePerSec * horizonUs / 1e6;
        if (excess < 1) {
            continue;
        }
        auto first = queue->headKey();
        if (!first) {
            continue;
        }

        auto [user, func, parStr] =
          faabric::util::splitUserFuncPar(userFuncPar);
        std::string userFunc = user + "_" + func;
        auto node = decentralScheduler.lookupNode(userFunc);
        int kind = 0;
        if (node != nullptr &&
            node->type != faabric::batch_scheduler::NodeType::STATELESS) {
            kind = StateAwareScheduler::fluxIsPartitioned(*node) ? 2 : 1;
        }
        backlogs.push_back({ userFuncPar,
                             userFunc,
                             kind,
                             depth,
                             excess,
                             slotTimeUs,
                             ratePerSec,
                             *first });
    }
    if (backlogs.empty()) {
        return;
    }

    // 2. Workers that can take work, as of the last stats round, with the
    // executor time each has free before the next round. Budgets are drawn
    // down as work is assigned, so one idle worker does not take everything.
    std::set<std::string> routable;
    {
        faabric::util::SharedLock lock(mx);
        for (const auto& [ip, host] : routableHosts()) {
            routable.insert(ip);
        }
    }

    struct FluxTarget
    {
        std::string ip;
        int freeSlots;
        int cap;
        double workerBudgetUs;
        // Per queue key, filled on first use.
        std::map<std::string, double> paridxBudgetUs;
        std::vector<std::string> units;
        std::map<std::string, int> stateless;
    };
    std::vector<FluxTarget> targets;
    for (const auto& [ip, workerStats] : stats) {
        if (ip == thisHost || !routable.contains(ip)) {
            continue;
        }
        int targetSlots =
          workerStats.workerslots() > 0 ? workerStats.workerslots() : slots;
        int targetCap =
          workerStats.paridxcap() > 0 ? workerStats.paridxcap() : maxExecutors;
        int freeSlots =
          targetSlots - static_cast<int>(std::ceil(workerStats.executorsnum()));
        if (freeSlots <= 0) {
            continue;
        }
        targets.push_back(
          { ip, freeSlots, targetCap, freeSlots * usableUs, {}, {}, {} });
    }
    if (targets.empty()) {
        SPDLOG_DEBUG("Flux {} has {} paridx behind but no worker has a free "
                     "executor slot",
                     thisHost,
                     backlogs.size());
        return;
    }

    auto countIn = [](const auto& counts, const std::string& key) -> int {
        auto it = counts.find(key);
        return it == counts.end() ? 0 : static_cast<int>(it->second);
    };

    // What a target can do for a paridx before the next round: its free run
    // slots there (all of them if it does not run the paridx), bounded by its
    // free executor slots, less its own backlog of the paridx.
    auto paridxBudget = [&](FluxTarget& target,
                            const FluxBacklog& backlog) -> double& {
        auto it = target.paridxBudgetUs.find(backlog.userFuncPar);
        if (it != target.paridxBudgetUs.end()) {
            return it->second;
        }
        const auto& workerStats = stats.at(target.ip);
        int running = countIn(workerStats.paridxrunning(), backlog.userFuncPar);
        int queued =
          countIn(workerStats.instancequeuenum(), backlog.userFuncPar);
        int runSlots = std::min(target.cap - running, target.freeSlots);
        double budgetUs =
          std::max(0.0, runSlots * usableUs - queued * backlog.slotTimeUs);
        return target.paridxBudgetUs.emplace(backlog.userFuncPar, budgetUs)
          .first->second;
    };
    auto available = [&](FluxTarget& target, const FluxBacklog& backlog) {
        return std::min(target.workerBudgetUs, paridxBudget(target, backlog));
    };
    auto consume =
      [&](FluxTarget& target, const FluxBacklog& backlog, double executorUs) {
          target.workerBudgetUs -= executorUs;
          paridxBudget(target, backlog) -= executorUs;
      };

    // Tied to the operator: already running it, or holding its state.
    std::map<std::string, std::map<std::string, int>> unitsPerHost;
    auto affine = [&](const FluxTarget& target, const std::string& userFunc) {
        auto it = unitsPerHost.find(userFunc);
        if (it == unitsPerHost.end()) {
            it = unitsPerHost
                   .emplace(userFunc,
                            decentralScheduler.fluxUnitsPerHost(userFunc))
                   .first;
        }
        if (countIn(it->second, target.ip) > 0) {
            return true;
        }
        std::string prefix = userFunc + "_";
        for (const auto& [queueKey, depth] :
             stats.at(target.ip).instancequeuenum()) {
            if (queueKey.starts_with(prefix)) {
                return true;
            }
        }
        return false;
    };

    // The target with room for at least `needUs` that can take the most
    // batches of the paridx. Between targets offering as many batches, one
    // tied to the operator wins: resources first, operators together second.
    auto bestTarget = [&](const FluxBacklog& backlog,
                          double needUs) -> FluxTarget* {
        FluxTarget* best = nullptr;
        long bestBatches = -1;
        bool bestAffine = false;
        double bestAvailableUs = 0;
        for (auto& target : targets) {
            double availableUs = available(target, backlog);
            if (availableUs <= 0 || availableUs < needUs) {
                continue;
            }
            long batches =
              static_cast<long>(availableUs / (backlog.slotTimeUs * batchSize));
            bool isAffine = affine(target, backlog.userFunc);
            bool better = best == nullptr || batches > bestBatches ||
                          (batches == bestBatches && isAffine && !bestAffine) ||
                          (batches == bestBatches && isAffine == bestAffine &&
                           availableUs > bestAvailableUs);
            if (better) {
                best = &target;
                bestBatches = batches;
                bestAffine = isAffine;
                bestAvailableUs = availableUs;
            }
        }
        return best;
    };

    // 3. What could move, oldest input first. Stateless requests go one by
    // one; a unit of state goes whole with every request waiting for it, and
    // ranks by its oldest request.
    struct FluxItem
    {
        OrderKey first;
        size_t backlog;
        // Empty for stateless requests.
        std::string unitKey;
        int count;
    };
    int64_t nowMs = faabric::util::getGlobalClock().epochMillis();
    auto unitMovable = [&](const std::string& unitKey) {
        std::lock_guard<std::mutex> lock(fluxMigrationMx);
        if (fluxMigratingUnits.contains(unitKey) ||
            fluxMovedUnits.contains(unitKey)) {
            return false;
        }
        auto coolIt = fluxUnitCooldownUntilMs.find(unitKey);
        return coolIt == fluxUnitCooldownUntilMs.end() ||
               nowMs >= coolIt->second;
    };

    std::vector<FluxItem> items;
    for (size_t i = 0; i < backlogs.size(); i++) {
        const auto& backlog = backlogs[i];
        if (backlog.kind == 0) {
            items.push_back({ backlog.first,
                              i,
                              "",
                              static_cast<int>(std::ceil(backlog.excess)) });
            continue;
        }
        if (backlog.kind == 1) {
            if (unitMovable(backlog.userFuncPar)) {
                items.push_back(
                  { backlog.first, i, backlog.userFuncPar, backlog.depth });
            }
            continue;
        }

        // Partitioned: shed the shards holding the oldest requests, until
        // they carry the excess.
        std::map<int, faabric::util::BatchQueue::GroupSummary> shards;
        {
            faabric::util::SharedLock lock(waitingQueuesMx);
            auto it = waitingQueues.find(backlog.userFuncPar);
            if (it != waitingQueues.end()) {
                shards = it->second->summaryBy(
                  [](const faabric::Message& m) { return m.shardid(); });
            }
        }
        std::vector<std::pair<int, faabric::util::BatchQueue::GroupSummary>>
          ordered(shards.begin(), shards.end());
        std::sort(
          ordered.begin(), ordered.end(), [](const auto& a, const auto& b) {
              return a.second.first < b.second.first;
          });
        double carried = 0;
        for (const auto& [shard, summary] : ordered) {
            if (carried >= backlog.excess) {
                break;
            }
            std::string unitKey =
              StateAwareScheduler::fluxUnitKey(backlog.userFuncPar, shard);
            if (!unitMovable(unitKey)) {
                continue;
            }
            items.push_back({ summary.first, i, unitKey, summary.count });
            carried += summary.count;
        }
    }
    std::sort(items.begin(), items.end(), [](const auto& a, const auto& b) {
        return a.first < b.first;
    });

    // 4. Assign, within the round's request budget.
    int budgetRequests = fluxMigrateRequests;
    bool firstUnit = true;
    for (const auto& item : items) {
        if (budgetRequests <= 0) {
            break;
        }
        const auto& backlog = backlogs[item.backlog];

        if (item.unitKey.empty()) {
            // Stateless: as many as fit, spread over targets if need be.
            int remaining = std::min(item.count, budgetRequests);
            while (remaining > 0) {
                FluxTarget* target = bestTarget(backlog, backlog.slotTimeUs);
                if (target == nullptr) {
                    break;
                }
                int n = std::min(remaining,
                                 static_cast<int>(available(*target, backlog) /
                                                  backlog.slotTimeUs));
                if (n <= 0) {
                    break;
                }
                target->stateless[backlog.userFuncPar] += n;
                consume(*target, backlog, n * backlog.slotTimeUs);
                remaining -= n;
                budgetRequests -= n;
            }
            continue;
        }

        double needUs = item.count * backlog.slotTimeUs;
        FluxTarget* target = bestTarget(backlog, needUs);
        // A unit goes whole or not at all. One that fits nowhere waits for a
        // later round -- unless it is the round's first unit, or a unit
        // bigger than any target's room could never move.
        if (target == nullptr && firstUnit) {
            target = bestTarget(backlog, 0);
        }
        if (target == nullptr || (!firstUnit && item.count > budgetRequests)) {
            continue;
        }
        if (backlog.kind == 1) {
            // A stateful paridx keeps its cap wherever it runs, so moving it
            // only pays if the target drains it sooner than it drains here.
            double hereUs = backlog.ratePerSec > 0
                              ? backlog.depth * 1e6 / backlog.ratePerSec
                              : std::numeric_limits<double>::infinity();
            int runSlots =
              std::max(1, std::min(target->cap, target->freeSlots));
            double thereUs =
              costUs + backlog.depth * backlog.slotTimeUs / runSlots;
            if (thereUs >= hereUs) {
                continue;
            }
        }
        target->units.push_back(item.unitKey);
        consume(*target, backlog, needUs);
        budgetRequests -= item.count;
        firstUnit = false;
    }

    // 5. One hand-over per target. Each measures what migrating costs.
    int unitsMoved = 0;
    int statelessMoved = 0;
    int targetsUsed = 0;
    for (auto& target : targets) {
        if (target.units.empty() && target.stateless.empty()) {
            continue;
        }
        std::vector<std::pair<std::string, int>> stateless(
          target.stateless.begin(), target.stateless.end());
        int64_t startUs = faabric::util::getGlobalClock().epochMicros();
        auto [nUnits, nStateless] =
          migrateToWorkerFlux(target.ip, target.units, stateless);
        if (nUnits == 0 && nStateless == 0) {
            continue;
        }
        double tookUs = static_cast<double>(
          faabric::util::getGlobalClock().epochMicros() - startUs);
        fluxCounters.migrateUs.fetch_add(static_cast<int64_t>(tookUs),
                                         std::memory_order_relaxed);
        double previousCostUs = fluxMigrationCostUs.load();
        fluxMigrationCostUs =
          previousCostUs <= 0 ? tookUs : 0.7 * previousCostUs + 0.3 * tookUs;
        unitsMoved += nUnits;
        statelessMoved += nStateless;
        targetsUsed++;
    }

    if (unitsMoved > 0 || statelessMoved > 0) {
        SPDLOG_INFO("Flux {} rebalanced {} paridx behind: moved {} units of "
                    "state and {} stateless requests to {} workers "
                    "(migration cost now {:.0f}us)",
                    thisHost,
                    backlogs.size(),
                    unitsMoved,
                    statelessMoved,
                    targetsUsed,
                    fluxMigrationCostUs.load());
    }
}

std::pair<int, int> Scheduler::migrateToWorkerFlux(
  const std::string& target,
  const std::vector<std::string>& units,
  const std::vector<std::pair<std::string, int>>& stateless)
{
    struct FluxUnitMove
    {
        std::string unitKey;
        std::string userFuncPar;
        // -1 for a whole paridx.
        int shardId;
        std::string user;
        std::string func;
        int parIdx;
        int64_t epoch;
        std::unique_ptr<faabric::util::FullLock> lock;
    };

    // 1. Keep only the units Redis says we own, and read their epochs: the
    // move publishes epoch + 1, and the receiver needs it before the flip.
    redis::Redis& redis = redis::Redis::getState();
    std::vector<FluxUnitMove> moves;
    for (const auto& unitKey : units) {
        auto [userFuncPar, shardId] =
          StateAwareScheduler::splitFluxUnitKey(unitKey);
        auto [user, func, parStr] =
          faabric::util::splitUserFuncPar(userFuncPar);
        auto node = decentralScheduler.lookupNode(user + "_" + func);
        if (node == nullptr ||
            node->type == faabric::batch_scheduler::NodeType::STATELESS ||
            StateAwareScheduler::fluxIsPartitioned(*node) != (shardId >= 0)) {
            continue;
        }

        std::string owner =
          faabric::util::bytesToString(redis.get(fluxOwnerKey(unitKey)));
        if (owner != thisHost) {
            SPDLOG_WARN("Flux {} does not own {} (owner {}), not moving it",
                        thisHost,
                        unitKey,
                        owner);
            continue;
        }
        int64_t epoch = 0;
        auto epochBytes = redis.get(fluxEpochKey(unitKey));
        if (!epochBytes.empty()) {
            epoch = std::stoll(faabric::util::bytesToString(epochBytes));
        }

        FluxUnitMove move;
        move.unitKey = unitKey;
        move.userFuncPar = userFuncPar;
        move.shardId = shardId;
        move.user = user;
        move.func = func;
        move.parIdx = std::stoi(parStr);
        move.epoch = epoch;
        moves.push_back(std::move(move));
    }

    // 2. Freeze them all first: from here their requests are parked, not
    // queued, and no new batch touching them is dispatched.
    {
        std::lock_guard<std::mutex> lock(fluxMigrationMx);
        std::erase_if(moves, [this](const FluxUnitMove& m) {
            if (fluxMigratingUnits.contains(m.unitKey) ||
                fluxMovedUnits.contains(m.unitKey)) {
                return true;
            }
            fluxMigratingUnits.insert(m.unitKey);
            fluxRouteRecords.fetch_add(1, std::memory_order_release);
            return false;
        });
    }

    // 3. Wait out every batch already running against each unit. Each holds
    // its units' locks shared until it has finished, so a function that has
    // read-and-locked its state gets to write it back before we snapshot.
    // Dispatchers only ever try-lock, so holding one unit while waiting for
    // the next cannot deadlock.
    for (auto& move : moves) {
        move.lock = std::make_unique<faabric::util::FullLock>(
          *getFluxUnitLock(move.unitKey));
    }

    // 4. Pack every unit's state and waiting requests, then the stateless
    // requests.
    auto& state = faabric::state::getGlobalState();
    faabric::FluxShardMigrationRequest req;
    req.set_sourcehost(thisHost);
    auto* batch = req.mutable_messagebatch();
    batch->set_invokehost(thisHost);

    auto takeFromQueue =
      [this, batch](
        const std::string& userFuncPar,
        const std::function<std::vector<std::unique_ptr<faabric::Message>>(
          faabric::util::BatchQueue&)>& take) -> int {
        std::vector<std::unique_ptr<faabric::Message>> taken;
        {
            faabric::util::SharedLock lock(waitingQueuesMx);
            auto it = waitingQueues.find(userFuncPar);
            if (it == waitingQueues.end()) {
                return 0;
            }
            taken = take(*it->second);
        }
        for (auto& msg : taken) {
            batch->add_messages()->Swap(msg.get());
        }
        return static_cast<int>(taken.size());
    };

    int unitRequests = 0;
    size_t stateBytes = 0;
    // With access_state_remote the state already lives in Redis, and state
    // never materialised here has nothing to carry.
    bool carryState = !state.accessRemote;

    // Whole paridx: the state and the queue go as they are.
    for (auto& move : moves) {
        if (move.shardId >= 0) {
            continue;
        }
        auto* unitState = req.add_shards();
        unitState->set_user(move.user);
        unitState->set_function(move.func);
        unitState->set_parallelismid(move.parIdx);
        unitState->set_shardid(-1);
        unitState->set_ispartitioned(false);
        unitState->set_epoch(move.epoch + 1);
        if (carryState && state.hasFS(move.user, move.func, move.parIdx)) {
            auto bytes = state.snapshotFS(move.user, move.func, move.parIdx);
            unitState->set_hasstate(true);
            unitState->set_serializedstate(bytes.data(), bytes.size());
            stateBytes += bytes.size();
        }
        unitRequests +=
          takeFromQueue(move.userFuncPar, [](faabric::util::BatchQueue& q) {
              return q.drainMessages();
          });
    }

    // Shards: cut out of their paridx, one pass per paridx for the keys and
    // one for the requests.
    std::map<std::string, std::set<int>> shardsByParidx;
    std::map<std::string, const FluxUnitMove*> paridxOf;
    for (const auto& move : moves) {
        if (move.shardId >= 0) {
            shardsByParidx[move.userFuncPar].insert(move.shardId);
            paridxOf[move.userFuncPar] = &move;
        }
    }
    for (const auto& [userFuncPar, shards] : shardsByParidx) {
        const FluxUnitMove& any = *paridxOf.at(userFuncPar);
        std::string userFunc = any.user + "_" + any.func;

        std::map<int, std::vector<uint8_t>> snapshots;
        if (carryState && state.hasFS(any.user, any.func, any.parIdx)) {
            snapshots = state.snapshotShards(
              any.user,
              any.func,
              any.parIdx,
              [this, &userFunc](const std::string& key) {
                  return decentralScheduler.fluxShardOfKey(userFunc, key);
              },
              shards);
        }
        for (const auto& move : moves) {
            if (move.userFuncPar != userFuncPar || move.shardId < 0) {
                continue;
            }
            auto* unitState = req.add_shards();
            unitState->set_user(move.user);
            unitState->set_function(move.func);
            unitState->set_parallelismid(move.parIdx);
            unitState->set_shardid(move.shardId);
            unitState->set_ispartitioned(true);
            unitState->set_epoch(move.epoch + 1);
            auto snapIt = snapshots.find(move.shardId);
            if (snapIt != snapshots.end()) {
                unitState->set_hasstate(true);
                unitState->set_serializedstate(snapIt->second.data(),
                                               snapIt->second.size());
                stateBytes += snapIt->second.size();
            }
        }

        unitRequests +=
          takeFromQueue(userFuncPar, [&shards](faabric::util::BatchQueue& q) {
              return q.takeIf([&shards](const faabric::Message& m) {
                  return m.messagetype() == 2 && shards.contains(m.shardid());
              });
          });
    }

    int statelessRequests = 0;
    std::map<std::string, int> statelessTaken;
    for (const auto& [userFuncPar, count] : stateless) {
        int n =
          takeFromQueue(userFuncPar, [count](faabric::util::BatchQueue& q) {
              return q.takeMessages(count);
          });
        if (n > 0) {
            statelessTaken[userFuncPar] = n;
            statelessRequests += n;
        }
    }

    if (moves.empty() && statelessRequests == 0) {
        return { 0, 0 };
    }

    // Every request in the hand-over goes on to another worker once more.
    for (auto& msg : *batch->mutable_messages()) {
        countForward(msg);
    }

    // 5. Hand it all over. Until the flip below, Redis still names us for
    // every unit, so nothing new is routed to the target before the state
    // has arrived.
    bool installed = false;
    try {
        faabric::scheduler::getFunctionCallClient(target)->migrateShardFlux(
          req);
        installed = true;
    } catch (const std::exception& e) {
        SPDLOG_ERROR("Flux could not hand {} units and {} stateless requests "
                     "to {}: {}",
                     moves.size(),
                     statelessRequests,
                     target,
                     e.what());
    }

    int64_t nowMs = faabric::util::getGlobalClock().epochMillis();

    auto collectParked = [this, &moves]() {
        std::list<std::unique_ptr<faabric::Message>> parked;
        for (const auto& move : moves) {
            auto pendingIt = fluxPendingMsgs.find(move.unitKey);
            if (pendingIt == fluxPendingMsgs.end()) {
                continue;
            }
            for (auto& msg : pendingIt->second) {
                parked.push_back(std::move(msg));
            }
            fluxPendingMsgs.erase(pendingIt);
        }
        return parked;
    };

    if (!installed) {
        // Nothing changed hands. Unfreeze every unit and put every request
        // back in the queue it came from.
        std::list<std::unique_ptr<faabric::Message>> parked;
        {
            std::lock_guard<std::mutex> lock(fluxMigrationMx);
            for (const auto& move : moves) {
                fluxMigratingUnits.erase(move.unitKey);
                fluxRouteRecords.fetch_sub(1, std::memory_order_release);
                fluxUnitCooldownUntilMs[move.unitKey] =
                  nowMs + fluxMigrateCooldown;
            }
            parked = collectParked();
        }
        std::set<std::string> requeued;
        {
            // These were accounted for when they were first queued, so they
            // go straight back into their queues.
            faabric::util::SharedLock lock(waitingQueuesMx);
            for (auto& msg : *batch->mutable_messages()) {
                auto it =
                  waitingQueues.find(faabric::util::getUserFuncPar(msg));
                if (it == waitingQueues.end()) {
                    continue;
                }
                requeued.insert(it->first);
                // It never left.
                msg.set_forwardcount(msg.forwardcount() - 1);
                auto msgPtr = std::make_unique<faabric::Message>();
                msgPtr->Swap(&msg);
                it->second->addMessage(std::move(msgPtr));
            }
        }
        for (auto& move : moves) {
            move.lock.reset();
        }
        // The units are unfrozen and their requests are back: dispatch them.
        for (const auto& userFuncPar : requeued) {
            markDispatchPending(userFuncPar);
        }
        for (const auto& move : moves) {
            markDispatchPending(move.userFuncPar);
        }
        if (!parked.empty()) {
            enqueueMessageBatch(std::move(parked), thisHost);
        }
        fluxCounters.failed.fetch_add(1, std::memory_order_relaxed);
        return { 0, 0 };
    }

    // 6. Flip every owner at once. Only the owner moves a unit, so the checks
    // in the script can only fail if something outside Flux rewrote a key.
    std::vector<redis::Redis::OwnerTransfer> transfers;
    for (const auto& move : moves) {
        transfers.push_back({ fluxOwnerKey(move.unitKey),
                              fluxEpochKey(move.unitKey),
                              move.epoch });
    }
    bool flipped = false;
    try {
        flipped = redis.transferOwners(transfers, thisHost, target);
    } catch (const std::exception& e) {
        SPDLOG_ERROR(
          "Flux could not flip owners of {} units: {}", moves.size(), e.what());
    }
    if (!flipped && !moves.empty()) {
        // The target holds the state and the requests now, so it is the owner
        // in every way but the Redis record. Keep going rather than end up
        // with the units in two places.
        SPDLOG_ERROR("Flux owner flip of {} units ({} -> {}) was refused; "
                     "Redis no longer matches their location",
                     moves.size(),
                     thisHost,
                     target);
    }

    // 7. Leave a forwarding record for every unit, and hand over whatever
    // was parked while we were busy. From here their requests are forwarded.
    std::list<std::unique_ptr<faabric::Message>> parked;
    {
        std::lock_guard<std::mutex> lock(fluxMigrationMx);
        for (const auto& move : moves) {
            fluxMigratingUnits.erase(move.unitKey);
            faabric::ShardMove record;
            record.set_userfuncpar(move.userFuncPar);
            record.set_shardid(move.shardId);
            record.set_host(target);
            record.set_epoch(move.epoch + 1);
            // One record swapped for another: fluxRouteRecords is unchanged.
            fluxMovedUnits[move.unitKey] = std::move(record);
            fluxUnitCooldownUntilMs[move.unitKey] = nowMs + fluxMigrateCooldown;
        }
        parked = collectParked();
    }

    // Drop the state that left: a whole paridx goes, a shard's keys are cut
    // out of the paridx, which stays for the shards still here.
    for (const auto& move : moves) {
        decentralScheduler.updateStateHostFlux(
          move.unitKey, target, move.epoch + 1);
        if (move.shardId < 0 && carryState &&
            state.hasFS(move.user, move.func, move.parIdx)) {
            state.deleteFS(move.user, move.func, move.parIdx);
        }
    }
    for (const auto& [userFuncPar, shards] : shardsByParidx) {
        const FluxUnitMove& any = *paridxOf.at(userFuncPar);
        std::string userFunc = any.user + "_" + any.func;
        if (carryState && state.hasFS(any.user, any.func, any.parIdx)) {
            state.eraseShards(
              any.user,
              any.func,
              any.parIdx,
              [this, &userFunc](const std::string& key) {
                  return decentralScheduler.fluxShardOfKey(userFunc, key);
              },
              shards);
        }
    }
    {
        faabric::util::FullLock lock(fluxKnownUnitsMx);
        for (const auto& move : moves) {
            fluxKnownUnits.erase(move.unitKey);
        }
    }
    for (auto& move : moves) {
        move.lock.reset();
    }

    size_t nParked = parked.size();
    if (!parked.empty()) {
        for (auto& msg : parked) {
            countForward(*msg);
        }
        faabric::scheduler::getFunctionCallClient(target)
          ->executeFunctionsBatch(std::move(parked));
    }

    fluxCounters.handovers.fetch_add(1, std::memory_order_relaxed);
    fluxCounters.outUnits.fetch_add(static_cast<int64_t>(moves.size()),
                                    std::memory_order_relaxed);
    fluxCounters.outStateReqs.fetch_add(
      unitRequests + static_cast<int64_t>(nParked), std::memory_order_relaxed);
    fluxCounters.outStateless.fetch_add(statelessRequests,
                                        std::memory_order_relaxed);
    fluxCounters.outStateBytes.fetch_add(static_cast<int64_t>(stateBytes),
                                         std::memory_order_relaxed);

    for (const auto& [userFuncPar, n] : statelessTaken) {
        runtimeStats.instanceGenerate(userFuncPar, target, n);
    }

    SPDLOG_INFO("Flux moved {} units ({} state bytes, {} queued + {} parked "
                "requests) and {} stateless requests {} -> {}",
                moves.size(),
                stateBytes,
                unitRequests,
                nParked,
                statelessRequests,
                thisHost,
                target);
    return { static_cast<int>(moves.size()), statelessRequests };
}

void Scheduler::receiveShardFlux(faabric::FluxShardMigrationRequest& req)
{
    auto& state = faabric::state::getGlobalState();
    int64_t cooldownUntilMs =
      faabric::util::getGlobalClock().epochMillis() + fluxMigrateCooldown;

    for (const auto& unitState : req.shards()) {
        std::string userFuncPar = unitState.user() + "_" +
                                  unitState.function() + "_" +
                                  std::to_string(unitState.parallelismid());
        std::string unitKey =
          StateAwareScheduler::fluxUnitKey(userFuncPar, unitState.shardid());

        // Install even an empty unit: requests may already be on their way,
        // and admitMessageFlux will not take them because Redis still names
        // the sender until it flips the owner.
        if (!state.accessRemote) {
            const std::string& data = unitState.serializedstate();
            std::vector<uint8_t> bytes(data.begin(), data.end());
            if (unitState.shardid() < 0) {
                state.installMigratedFS(unitState.user(),
                                        unitState.function(),
                                        unitState.parallelismid(),
                                        unitState.ispartitioned(),
                                        bytes);
            } else {
                state.mergeMigratedShard(unitState.user(),
                                         unitState.function(),
                                         unitState.parallelismid(),
                                         bytes);
            }
        }
        {
            faabric::util::FullLock lock(fluxKnownUnitsMx);
            fluxKnownUnits.insert(unitKey);
        }
        {
            std::lock_guard<std::mutex> lock(fluxMigrationMx);
            // The unit may be coming back to a worker that once handed it
            // off.
            if (fluxMovedUnits.erase(unitKey) > 0) {
                fluxRouteRecords.fetch_sub(1, std::memory_order_release);
            }
            fluxUnitCooldownUntilMs[unitKey] = cooldownUntilMs;
        }
        decentralScheduler.updateStateHostFlux(
          unitKey, thisHost, unitState.epoch());
    }

    fluxCounters.inUnits.fetch_add(req.shards_size(),
                                   std::memory_order_relaxed);
    for (const auto& unitState : req.shards()) {
        fluxCounters.inStateBytes.fetch_add(
          static_cast<int64_t>(unitState.serializedstate().size()),
          std::memory_order_relaxed);
    }
    for (const auto& msg : req.messagebatch().messages()) {
        auto& counter = msg.messagetype() == 0 ? fluxCounters.inStateless
                                               : fluxCounters.inStateReqs;
        counter.fetch_add(1, std::memory_order_relaxed);
    }

    SPDLOG_INFO("Flux took over {} units and {} requests from {}",
                req.shards_size(),
                req.messagebatch().messages_size(),
                req.sourcehost());

    // Only after every unit is installed: the requests may address any of
    // them.
    if (req.messagebatch().messages_size() > 0) {
        auto batch = std::make_unique<faabric::MessageBatch>();
        batch->Swap(req.mutable_messagebatch());
        enqueueMessageBatch(std::move(batch));
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

    auto hosts =
      decentralScheduler.scheduleMessagesBatch(routableHosts(), localMsgs);

    for (size_t i = 0; i < localMsgs.size(); ++i) {
        const std::string& destinationHost = hosts[i];
        auto& msg = localMsgs[i];

        if (packedMessageMap.find(destinationHost) == packedMessageMap.end()) {
            packedMessageMap[destinationHost] =
              std::make_unique<faabric::MessageBatch>();
        }
        auto* newMsg = packedMessageMap[destinationHost]->add_messages();
        newMsg->Swap(msg.get());
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
}

std::string Scheduler::getLocalPersistentState(std::string key)
{
    SPDLOG_DEBUG("Getting local persistent state for key: {}", key);
    return faabric::state::getGlobalState().readPersistentState(key);
}

void Scheduler::flushState()
{
    SPDLOG_INFO("Flushing state");
    faabric::state::getGlobalState().flushState();
}

// std::queue<std::tuple<double, double>> Scheduler::getCpuRecordHistory()
// {
//     return cpuRecordHistory;
// }

std::map<std::string, int> Scheduler::getMaxReplicasMap()
{
    return maxReplicasMap;
}

std::map<std::string, InstanceMetricsResult> Scheduler::getWorkerMetrics(
  bool isRuntime)
{
    return runtimeStats.getWorkerMetrics(isRuntime);
}

std::tuple<std::map<std::string, int>, double, double>
Scheduler::getStatsSnapshot()
{
    std::map<std::string, int> queueSizes;
    for (const auto& [userFuncPar, waitingBatch] : waitingQueues) {
        queueSizes[userFuncPar] = waitingBatch->getMessagesCount();
    }
    double runningExecutorsCount = getAverageExecutors();
    double lastCpu = getLastVmCpu();

    return { queueSizes, runningExecutorsCount, lastCpu };
}

void Scheduler::setClusterWorkerStats(
  std::map<std::string, faabric::WorkerStats>&& stats)
{
    decentralScheduler.setClusterWorkerStats(std::move(stats));
}

void Scheduler::cpuMonitorLoop()
{
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