#pragma once

#include <faabric/batch-scheduler/DecentralizedScheduler.h>
#include <faabric/executor/Executor.h>
#include <faabric/planner/PlannerClient.h>
#include <faabric/proto/faabric.pb.h>
// #include <faabric/scheduler/InstancesLoadState.h>
#include <faabric/batch-scheduler/RuntimeSummary.h>
#include <faabric/scheduler/InstancesRuntimeStats.h>
#include <faabric/snapshot/SnapshotRegistry.h>
#include <faabric/transport/PointToPointBroker.h>
#include <faabric/util/PeriodicBackgroundThread.h>
#include <faabric/util/clock.h>
#include <faabric/util/queue.h>
#include <faabric/util/snapshot.h>

#include <shared_mutex>

#define DEFAULT_THREAD_RESULT_TIMEOUT_MS 1000

namespace faabric::scheduler {

class Scheduler;

Scheduler& getScheduler();

/**
 * Background thread that periodically checks to see if any executors have
 * become stale (i.e. not handled any requests in a given timeout). If any are
 * found, they are removed.
 */
class SchedulerReaperThread : public faabric::util::PeriodicBackgroundThread
{
  public:
    void doWork() override;
};

class Scheduler
{
  public:
    Scheduler();

    ~Scheduler();

    void enqueueMessageBatch(std::unique_ptr<faabric::MessageBatch> msgs);

    void enqueueMessageBatch(std::list<std::unique_ptr<faabric::Message>> msgs,
                             std::string invokeHost);

    void setMessageResults();

    // Check the waiting queue peroiodically.
    void batchTimerCheck();

    bool registerApp(std::unique_ptr<batch_scheduler::Application> app);

    void enqueueChainedCalls(
      std::vector<std::unique_ptr<faabric::Message>> msgs);

    void enqueueSetResults(std::shared_ptr<faabric::BatchExecuteRequest> req);

    void executeBatchForQueue(const std::string& userFuncPar,
                              util::BatchQueueBase& waitingQueue);

    void resetParameter(std::string key, int32_t value);

    void reset();

    void resetThreadLocalCache();

    void shutdown();

    bool isShutdown() { return _isShutdown; }

    void broadcastSnapshotDelete(const faabric::Message& msg,
                                 const std::string& snapshotKey);

    int reapStaleExecutors();

    long getFunctionExecutorCount(const faabric::Message& msg);

    void flushState();

    // ----------------------------------
    // Message results
    // ----------------------------------

    /**
     * Caches a message along with the thread result, to allow the thread result
     * to refer to data held in that message (i.e. snapshot diffs). The message
     * will be destroyed once the thread result is consumed.
     */
    void setThreadResultLocally(uint32_t appId,
                                uint32_t msgId,
                                int32_t returnValue,
                                faabric::transport::Message& message);

    std::vector<std::pair<uint32_t, int32_t>> awaitThreadResults(
      std::shared_ptr<faabric::BatchExecuteRequest> req,
      int timeoutMs = DEFAULT_THREAD_RESULT_TIMEOUT_MS);

    size_t getCachedMessageCount();

    // std::string getThisHost();

    void addHostToGlobalSet();

    void addHostToGlobalSet(
      const std::string& host,
      std::shared_ptr<faabric::HostResources> overwriteResources = nullptr);

    void removeHostFromGlobalSet(const std::string& host);

    void setThisHostResources(faabric::HostResources& res);

    // ----------------------------------
    // Testing
    // ----------------------------------
    std::vector<faabric::Message> getRecordedMessages();

    void clearRecordedMessages();

    // ----------------------------------
    // Function Migration
    // ----------------------------------
    std::shared_ptr<faabric::PendingMigration> checkForMigrationOpportunities(
      faabric::Message& msg,
      int overwriteNewGroupId = 0);

    // ----------------------------------
    // Status Collection
    // ----------------------------------
    int getMonitoredInfoTest();

    void updateHosts(const std::vector<std::string>& hosts);

    void updateStatesInfo(
      const std::map<std::string, faabric::batch_scheduler::ScheduledOperator>&
        scheuduledOperatorMap,
      const std::map<std::string, faabric::batch_scheduler::FunctionStateInfo>&
        statesInfo);

    void storeMigrateState(
      std::multimap<std::string, std::string>&& migrateState);

    // std::map<std::string, int> statsLocalLoad();

    // void updateWorkersLoad();

    std::map<std::string, InstanceStatsResult> getRuntimeStats();

    void updateStatelessDist(
      const std::map<std::string, std::map<std::string, int>>&
        sourceCountStats);

    std::string getLocalPersistentState(std::string key);

    void setLocalPersistentState(
      const std::map<std::string, std::string>& kvMap);

    std::queue<std::tuple<double, double>> getCpuRecordHistory();

    std::map<std::string, int> getMaxReplicasMap();

  private:
    std::string thisHost;

    faabric::util::SystemConfig& conf;

    std::shared_mutex mx;

    std::atomic<bool> _isShutdown = false;

    int scheduleMode = 0;

    // Maximum number of replicas per function
    // int maxReplicas = 8;

    std::map<std::string, int> maxReplicasMap;

    // Maximum number of concurrent executors in the worker
    int maxExecutors = 40;

    int executeBatchsize;

    // ---- Executors ----
    std::unordered_map<
      std::string,
      std::vector<std::shared_ptr<faabric::executor::Executor>>>
      executors;

    // ---- Threads ----
    faabric::snapshot::SnapshotRegistry& reg;

    std::unordered_map<uint32_t, faabric::transport::Message>
      threadResultMessages;

    // ---- Planner----
    faabric::planner::KeepAliveThread keepAliveThread;

    // ---- Actual scheduling ----
    SchedulerReaperThread reaperThread;

    bool executorAvailable(const std::string& funcStr);

    std::shared_ptr<faabric::executor::Executor> claimExecutor(
      faabric::Message& msg
      // ,faabric::util::FullLock& schedulerLock
    );

    // ---- Accounting and debugging ----
    std::vector<faabric::Message> recordedMessages;

    // ---- Point-to-point ----
    faabric::transport::PointToPointBroker& broker;

    // A queue stores the uninvoked requests: MAP<UserFuncPar, Queue>
    std::map<std::string, std::unique_ptr<faabric::util::BatchQueue>>
      waitingQueues;

    // MAP<UserFuncPar, Queue>
    // std::map<std::string,
    //          std::unique_ptr<faabric::util::PartitionedStateMessageQueue>>
    //   partitionedWaitingQueues;

    std::shared_mutex chainedCallMsgsMx;
    std::vector<std::unique_ptr<faabric::Message>> chainedCallMsgs;

    std::shared_mutex setResultMsgsMx;
    std::vector<std::unique_ptr<faabric::Message>> setResultMsgs;

    // ---- Batch Execution ----
    std::thread batchTimerThread;
    bool stopBatchTimer = false;

    std::thread setResultThread;
    int plannerCallInterval = 20; // ms

    int batchInterval = 20; // ms

    // ----- Scheduling Info -----
    int dispatchPeriod = 20;  // ms
    int batchCheckPeriod = 5; // ms

    // stateUpdateMx is used to prevent central scheduler update state when
    // the executor is running.
    std::shared_mutex stateUpdateMx;

    // scheduledMsgsMapMx is used for scheduledMsgsMap
    std::shared_mutex scheduledMsgsMapMx;
    std::map<std::string, std::list<std::unique_ptr<Message>>> scheduledMsgsMap;

    std::shared_mutex tempMigrateStateMapMx;
    std::multimap<std::string, std::string> tempMigrateStateMap;

    faabric::batch_scheduler::DecentralizedScheduler decentralScheduler;

    std::map<std::string, std::string> registeredHostsMap;

    faabric::batch_scheduler::HostMap hostMap;

    bool stopThreadTimer = false;

    std::thread dispatchChainedMsgsThread;

    bool isUpdateState = false;

    util::ThreadSafeQueue<std::unique_ptr<faabric::MessageBatch>> unschedMsgs;

    // Worker workload update timer
    // long lastWorkersUpdate = 0;
    // int workerUpdateInterval = 3000; // ms

    // long lastPlannerCallCheck = 0;

    // Statistics the loads of workers
    // size_t maxSamples = 10000;
    // InstancesLoadState instancesLoadState;

    InstancesRuntimeStats runtimeStats;

    void enqueueSchedMsgs(std::vector<std::string> hosts,
                          std::vector<std::unique_ptr<faabric::Message>> msgs);

    void dispatchChainedMsgs();

    bool parallelDispatch = true;
    std::shared_mutex reconfigMx;

    std::shared_mutex cpuRecordMx;
    std::atomic<std::int64_t> cpuScheduleTime{ 0 };
    std::chrono::steady_clock::time_point cpuRecordStart{};
    std::chrono::seconds cpuRecordWindow{ 10 };
    size_t historyCap = 60;
    std::queue<std::tuple<double, double>> cpuRecordHistory;
    // std::map<std::string, clockid_t> runningThreads;
    std::set<clockid_t> runningThreads;
    std::map<clockid_t, int64_t> threadClockStartMap;
};

}
