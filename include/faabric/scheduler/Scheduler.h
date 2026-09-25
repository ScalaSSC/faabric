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

#include <set>
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
        scheduledOperatorMap,
      const std::map<std::string, faabric::batch_scheduler::FunctionStateInfo>&
        statesInfo,
      int migrationVersion,
      bool isInitialization,
      std::map<std::string, std::set<std::string>>& transDestinationMap,
      std::map<std::string, std::set<std::string>>& transSourceMap);

    void storeMigrateState(
      std::map<std::string, std::vector<uint8_t>>&& migrateState);

    void processMigrationData(const faabric::StateMigrationRequest& req);

    // void updateWorkersLoad();

    std::map<std::string, InstanceStatsResult> getRuntimeStats();

    void updateStatelessDist(
      const std::map<std::string, std::map<std::string, int>>&
        sourceCountStats);

    std::string getLocalPersistentState(std::string key);

    void setLocalPersistentState(
      const std::map<std::string, std::string>& kvMap);

    // std::queue<std::tuple<double, double>> getCpuRecordHistory();

    std::map<std::string, int> getMaxReplicasMap();

    std::map<std::string, InstanceMetricsResult> getWorkerMetrics(
      bool isRuntime = false);

    std::tuple<std::map<std::string, int>, double, double> getStatsSnapshot();

    // Hands the planner's previous-round stats of all workers to
    // decentralScheduler.
    void setClusterWorkerStats(
      std::map<std::string, faabric::WorkerStats>&& stats);

    /**
     * ModeFlux: take over what another worker is handing to us. Installs
     * every shard's state, marks the shards known so ensureLocalStateFlux
     * leaves them alone, and enqueues the requests that travelled with them.
     * Called synchronously by the sender before it flips the owners in Redis.
     */
    void receiveShardFlux(faabric::FluxShardMigrationRequest& req);

    /**
     * ModeFlux: every shard this worker has handed off and not taken back,
     * reported in each stats round so the planner, and through it every
     * other worker, learns the new owners.
     */
    std::vector<faabric::ShardMove> getFluxShardMoves();

    void notifyExecutorFinished();

    void notifyExecutorStart();

    double getAverageExecutors() const;

    double getLastVmCpu();

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
    int maxExecutors = 10;

    int executeBatchsize;

    // ---- Executors ----
    std::unordered_map<
      std::string,
      std::vector<std::shared_ptr<faabric::executor::Executor>>>
      executors;

    std::atomic<int> runningExecutors{ 0 };
    std::atomic<int64_t> currentWindowSec{ 0 };
    std::atomic<long long> currentSecondSum{ 0 };
    std::atomic<long long> currentSecondCount{ 0 };
    std::atomic<double> lastSecondAverage{ 0.0 };

    faabric::util::ThreadSafeQueue<std::string> readyDispatchQueue;
    std::vector<std::thread> dispatchThreads;
    bool stopDispatcher = false;
    void dispatchWorkerLoop();

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
    std::shared_mutex waitingQueuesMx;
    std::map<std::string, std::unique_ptr<faabric::util::BatchQueue>>
      waitingQueues;

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

    // scheduledMsgsMapMx is used for scheduledMsgsMap which are the chained
    // call messages to be dispatched.
    std::shared_mutex scheduledMsgsMapMx;
    std::map<std::string, std::list<std::unique_ptr<Message>>> scheduledMsgsMap;

    // The migrated messages received from other hosts.
    util::ThreadSafeQueue<std::unique_ptr<faabric::MessageBatch>> migratedMsgs;

    // The migrated state received from other hosts.
    std::shared_mutex migratedStateMapMx;
    std::multimap<std::string, std::vector<uint8_t>> migratedStateMap;

    faabric::batch_scheduler::DecentralizedScheduler decentralScheduler;

    std::map<std::string, std::string> registeredHostsMap;

    faabric::batch_scheduler::HostMap hostMap;

    faabric::batch_scheduler::HostMap activeHosts;

    // ModeFlux: shards this worker has already settled -- either created, or
    // found not to need state at all. Keeps ensureLocalStateFlux to one set
    // lookup per message once an operator is running.
    std::shared_mutex fluxKnownShardsMx;
    std::set<std::string> fluxKnownShards;

    // Throttle for logFluxExecutorCensus.
    std::atomic<int64_t> lastFluxExecutorLogMs{ 0 };

    // ---- ModeFlux rebalancing ----
    // Guards the four maps below. Never held across a network call.
    std::mutex fluxMigrationMx;
    // Shards being handed off right now. Requests for them are parked in
    // fluxPendingMsgs rather than queued, and nothing dispatches them.
    std::set<std::string> fluxMigratingShards;
    std::map<std::string, std::vector<std::unique_ptr<faabric::Message>>>
      fluxPendingMsgs;
    // Shards this worker handed off, keyed by userFuncPar. A request that
    // still reaches us for one was routed on a stale owner and is forwarded.
    // Dropped if the shard ever comes back.
    std::map<std::string, faabric::ShardMove> fluxMovedShards;
    // Earliest time (epoch ms) a shard may move again, so a shard does not
    // bounce between workers on consecutive rounds of stale stats.
    std::map<std::string, int64_t> fluxShardCooldownUntilMs;
    // Size of fluxMigratingShards + fluxMovedShards, read lock-free by the
    // enqueue path to skip the mutex until the first migration.
    std::atomic<int> fluxRouteRecords{ 0 };

    // One lock per shard. A dispatched batch holds it shared until the batch
    // has finished executing; a migration holds it exclusively, which is how
    // it waits out every running access to the shard -- including one that
    // has read and locked the state and has yet to write it back.
    std::shared_mutex fluxShardLocksMx;
    std::map<std::string, std::shared_ptr<std::shared_mutex>> fluxShardLocks;
    std::shared_ptr<std::shared_mutex> getFluxShardLock(
      const std::string& userFuncPar);

    std::thread fluxRebalanceThread;
    // ms between rebalancing rounds; 0 disables rebalancing.
    int fluxRebalancePeriod = 1000;
    // ms a shard stays put after it moved.
    int fluxMigrateCooldown = 5000;
    // Most requests one worker hands off per round, over all targets,
    // stateless and stateful alike. A stateful shard goes whole or not at all,
    // so one whose backlog exceeds what is left is skipped -- unless it is the
    // first shard of the round, which may go over, or a shard bigger than
    // the budget could never move.
    int fluxMigrateRequests = 10000;
    // Executors a worker can run at once. Flux caps executors per shard, not
    // per worker, so free capacity is measured against this instead.
    int fluxWorkerSlots = 0;

    void fluxRebalanceLoop();

    /**
     * ModeFlux: one rebalancing round. Only acts when this worker is
     * saturated (every slot busy and requests waiting). Then, using the
     * cluster stats, it plans what goes where -- surplus stateless requests
     * and whole stateful shards, hottest queues first, each to the worker
     * with the most room left, within fluxMigrateRequests -- and hands each
     * target its share in a single migration.
     */
    void rebalanceFlux();

    /**
     * ModeFlux: hand `shards` (userFuncPar of stateful shards, with their
     * waiting requests) and up to the given count of requests from each
     * stateless queue in `stateless` to `target`, in one request.
     *
     * Blocks new dispatches of every shard, waits for running ones to finish,
     * ships the lot, flips all owners in Redis at once, then forwards
     * whatever arrived meanwhile. All or nothing: if the hand-over fails,
     * every shard stays and every request goes back where it was. Returns
     * {shards moved, stateless requests moved}.
     */
    std::pair<int, int> migrateToWorkerFlux(
      const std::string& target,
      const std::vector<std::string>& shards,
      const std::vector<std::pair<std::string, int>>& stateless);

    /**
     * ModeFlux enqueue hook. Parks a request for a shard being migrated, or
     * collects one for a shard we handed off into `forwards` (by new owner).
     * Returns true if the request was taken; false to queue it here.
     */
    bool divertMessageFlux(
      std::unique_ptr<faabric::Message>& msg,
      const std::string& userFuncPar,
      std::map<std::string, std::list<std::unique_ptr<faabric::Message>>>&
        forwards);

    void clearFluxMigrationState();

    /**
     * The hosts this worker may route to. ModeFlux pins no operator to a
     * worker and claims a shard's owner on demand, so every registered host
     * is a candidate -- and nothing ever narrows activeHosts under ModeFlux
     * anyway, since the planner sends no state-info broadcast. The other
     * modes deliberately route only to the workers the planner placed the
     * application on.
     */
    const faabric::batch_scheduler::HostMap& routableHosts() const
    {
        return scheduleMode == faabric::batch_scheduler::ModeFlux ? hostMap
                                                                  : activeHosts;
    }

    bool stopThreadTimer = false;

    std::thread dispatchChainedMsgsThread;

    std::atomic<bool> isUpdateState{ false };

    InstancesRuntimeStats runtimeStats;

    void enqueueSchedMsgs(std::vector<std::string> hosts,
                          std::vector<std::unique_ptr<faabric::Message>> msgs);

    void dispatchChainedMsgs();

    bool parallelDispatch = true;
    std::shared_mutex reconfigMx;

    // All related to cpu record for each executor.
    // std::shared_mutex cpuRecordMx;
    // std::atomic<std::int64_t> cpuScheduleTime{ 0 };
    // std::chrono::steady_clock::time_point cpuRecordStart{};
    // std::chrono::seconds cpuRecordWindow{ 10 };
    // size_t historyCap = 60;
    // std::queue<std::tuple<double, double>> cpuRecordHistory;
    // std::map<std::string, clockid_t> runningThreads;
    // std::set<clockid_t> runningThreads;
    // std::map<clockid_t, int64_t> threadClockStartMap;

    std::mutex migrationMx;
    int currentMigrationVersion = 0;
    // MAP<migration version, set of updated states>
    std::map<int, std::set<std::string>> receivedMigrationSources;
    std::condition_variable migrationCv;

    using StateMigrationMap =
      std::map<std::string, std::map<std::string, std::vector<uint8_t>>>;
    using MessageMigrationMap =
      std::map<std::string, std::unique_ptr<faabric::MessageBatch>>;

    void calculateMaxReplicas(
      const std::map<std::string, faabric::batch_scheduler::ScheduledOperator>&
        scheduledOperatorMap);

    /**
     * ModeFlux: bring `userFuncPar`'s function state into being on this
     * worker, the first time a message for it arrives here.
     *
     * ModeFlux pre-creates nothing -- the planner places no state and sends
     * no state-info broadcast -- so the arriving message is what creates the
     * shard it addresses. Safe because only the worker holding a shard can
     * migrate it away: a message for a shard we hold no record of means we
     * are its owner and nobody has touched it yet. (Once migration exists,
     * the third case -- a shard we handed off, whose router is stale -- is
     * told apart by the handoff record we kept, not by anything on the wire.)
     *
     * Everything needed is already to hand: the message carries the shard
     * index and the DAG carries the operator's kind. The FunctionState
     * constructor re-checks ownership against Redis, so a misrouted message
     * cannot create state on a host that does not own it.
     *
     * A no-op for stateless operators and after the shard is known.
     */
    void ensureLocalStateFlux(const faabric::Message& msg,
                              const std::string& userFuncPar);

    /**
     * ModeFlux only: one line naming every shard's executors as free/total,
     * plus the worker-wide total.
     *
     * Logged from the blocked path of executorAvailable, where one shard has
     * hit its own cap. The whole worker is listed rather than just that shard
     * because the cap is per shard and the worker has no ceiling of its own:
     * what you want to see is how much the rest of the worker is already
     * carrying. Throttled to one line a second -- the caller runs per queue
     * every batchCheckPeriod.
     */
    void logFluxExecutorCensus(const std::string& blockedFunc);

    void createLocalState(
      const std::map<std::string, faabric::batch_scheduler::FunctionStateInfo>&
        statesInfo);

    // State and Message Migration
    // MAP<HOST IP, <FUNCTION_PAR, Serialized Data>>
    StateMigrationMap packState(
      const std::map<std::string, faabric::batch_scheduler::FunctionStateInfo>&
        statesInfo);

    MessageMigrationMap packMessage();

    void transferData(int migrationVersion,
                      StateMigrationMap stateMap,
                      MessageMigrationMap msgMap,
                      std::set<std::string> migrationDestinations);

    void updateActiveHosts(
      const std::map<std::string, faabric::batch_scheduler::ScheduledOperator>&
        scheduledOperatorMap);

    // ==========================================
    // VM CPU Monitor
    // ==========================================
    std::thread cpuMonitorThread;
    std::atomic<bool> stopCpuMonitor{ false };
    std::shared_mutex vmCpuHistoryMx;
    std::deque<double> vmCpuHistory;

    void cpuMonitorLoop();
};

}
