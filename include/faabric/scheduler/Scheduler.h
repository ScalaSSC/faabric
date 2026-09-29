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

    // Dispatches batches from one queue for up to batchCheckPeriod. Returns
    // whether it stopped only because that time ran out, with requests still
    // waiting that could run now -- as opposed to running out of requests, or
    // of run slots or executors, which a later event will signal.
    bool executeBatchForQueue(const std::string& userFuncPar,
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
     * every unit's state -- replacing a whole paridx, merging a shard's keys
     * into the local copy of its paridx -- marks the units known so
     * admitMessageFlux leaves them alone, and enqueues the requests that
     * travelled with them. Called synchronously by the sender before it flips
     * the owners in Redis.
     */
    void receiveShardFlux(faabric::FluxShardMigrationRequest& req);

    /**
     * ModeFlux: every unit of state this worker has handed off and not taken
     * back,
     * reported in each stats round so the planner, and through it every
     * other worker, learns the new owners.
     */
    std::vector<faabric::ShardMove> getFluxShardMoves();

    // ModeFlux: adds this worker's per-paridx load and capacity to the stats
    // it reports every round.
    void fillFluxWorkerStats(faabric::WorkerStats& out);

    // `msg` is the first message of the batch that finished.
    void notifyExecutorFinished(const faabric::Message& msg);

    // An executor done with its batch -- reset and with its thread free again
    // -- going back to its pool to be handed out anew. Called from the
    // executor's own thread.
    void returnExecutor(faabric::executor::Executor* executor);

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
    // One pool of warm executors per executorPoolKey, each with its own lock,
    // so handing out and taking back executors neither scans a pool nor
    // contends with anything outside it.
    struct ExecutorPool
    {
        std::mutex mx;
        // Every executor of the pool, by address, owning it.
        std::unordered_map<faabric::executor::Executor*,
                           std::shared_ptr<faabric::executor::Executor>>
          all;
        // The idle ones, the most recently returned last: it is the one
        // handed out next, its caches the warmest.
        std::vector<faabric::executor::Executor*> idle;
    };
    // Only written when a pool is added or all are dropped.
    std::shared_mutex executorPoolsMx;
    std::unordered_map<std::string, std::shared_ptr<ExecutorPool>>
      executorPools;

    // The pool for `key`, created if missing and `create` is set.
    std::shared_ptr<ExecutorPool> getExecutorPool(const std::string& key,
                                                  bool create);

    std::atomic<int> runningExecutors{ 0 };
    std::atomic<int64_t> currentWindowSec{ 0 };
    std::atomic<long long> currentSecondSum{ 0 };
    std::atomic<long long> currentSecondCount{ 0 };
    std::atomic<double> lastSecondAverage{ 0.0 };

    faabric::util::ThreadSafeQueue<std::string> readyDispatchQueue;
    std::vector<std::thread> dispatchThreads;
    void dispatchWorkerLoop();

    // ---- Event-driven dispatch ----
    // Dispatchers sleep until a queue has something that could run: requests
    // arrived, a batch finished and freed its run slot, or requests came back
    // to a queue. The event marks the queue pending and wakes a dispatcher,
    // so requests start as soon as they can instead of at the next tick.
    //
    // Queues with something to dispatch (queue key -> when marked), and the
    // ones a dispatcher is working on: each queue is worked on by one
    // dispatcher at a time.
    std::mutex dispatchMx;
    std::condition_variable dispatchCv;
    std::map<std::string, int64_t> dispatchPending;
    std::set<std::string> dispatchActive;
    // Guarded by dispatchMx.
    bool stopDispatcher = false;

    void markDispatchPending(const std::string& userFuncPar);

    // Marks every non-empty queue. After a pause in dispatching (a central
    // state update), and as a safety net against a missed event.
    void markAllDispatchPending();

    // Picks the pending queue to work on next and marks it active: the one
    // whose next request descends from the oldest input under ModeFlux, the
    // one pending longest otherwise. Caller holds dispatchMx.
    std::string pickDispatchQueue();

    // Wakes the chained-call dispatcher and the result reporter.
    faabric::util::WakeSignal chainedWake;
    faabric::util::WakeSignal resultsWake;

    // ---- Threads ----
    faabric::snapshot::SnapshotRegistry& reg;

    std::unordered_map<uint32_t, faabric::transport::Message>
      threadResultMessages;

    // ---- Planner----
    faabric::planner::KeepAliveThread keepAliveThread;

    // ---- Actual scheduling ----
    SchedulerReaperThread reaperThread;

    bool executorAvailable(const std::string& funcStr);

    // Which pool of warm executors serves `msg`. ModeFlux keeps one pool per
    // operator, shared by all its paridx: nothing in an executor is bound to
    // a paridx -- state is reached through the paridx of the message being
    // executed. Every other mode keeps one pool per paridx.
    std::string executorPoolKey(const faabric::Message& msg) const;

    // ModeFlux, per paridx: the run-slot count behind the maxExecutors cap,
    // and what the paridx actually achieves here, measured over rebalancing
    // windows. Measured rather than modelled on purpose: it takes in the
    // dispatch cadence, executor preparation and CPU contention, none of
    // which the execution time alone shows.
    struct FluxParidxLoad
    {
        // Batches running right now. The executor pool is shared by the
        // operator, so the cap is counted here rather than by pool size.
        int running = 0;
        // The current window: when it started, when `running` last changed,
        // running batches integrated over time so far, requests finished.
        int64_t windowStartUs = 0;
        int64_t lastChangeUs = 0;
        double runningIntegralUs = 0;
        int64_t finished = 0;
        // The last complete window: requests finished per second, and the
        // executor time one request took (average running batches over that
        // rate). The latter keeps its last measured value through idle
        // windows.
        double ratePerSec = 0;
        double slotTimeUs = 0;
    };
    // Keyed by user/func/parIdx.
    std::mutex fluxParidxRunMx;
    std::map<std::string, FluxParidxLoad> fluxParidxLoad;

    // Accounts the time since `running` last changed, and opens the first
    // window of a paridx seen for the first time.
    static void advanceFluxLoad(FluxParidxLoad& load, int64_t nowUs);

    // Takes one of the paridx's run slots for a batch about to be dispatched,
    // or returns false if all are in use. Handed back by
    // releaseParidxRunFlux if no batch ends up dispatched, or by
    // finishParidxRunFlux when the batch finishes.
    bool reserveParidxRunFlux(const std::string& funcParStr);

    void releaseParidxRunFlux(const std::string& funcParStr);

    void finishParidxRunFlux(const std::string& funcParStr, int requests);

    // Closes the current measurement window of every paridx and opens the
    // next. Once per rebalancing period.
    void rollFluxParidxWindows();

    std::map<std::string, FluxParidxLoad> snapshotFluxParidxLoads();

    // What the StateAwareScheduler reads about one of this worker's queues
    // when routing chained requests (keyed like the queue: user_func_par).
    faabric::batch_scheduler::StateAwareScheduler::FluxLocalLoad
    fluxLocalLoadOf(const std::string& userFuncPar);

    // Time (us) handing work over to another worker has taken, averaged over
    // recent migrations: from freezing the units to flipping their owners.
    // What a moved request loses before it can start on the other side.
    // Written by the rebalancing thread, read by the stats report.
    std::atomic<double> fluxMigrationCostUs{ 0 };

    // Cumulative counters behind WorkerStats.fluxMigration, since the last
    // flush. See FluxMigrationStats.
    struct FluxMigrationCounters
    {
        std::atomic<int64_t> handovers{ 0 };
        std::atomic<int64_t> failed{ 0 };
        std::atomic<int64_t> outUnits{ 0 };
        std::atomic<int64_t> outStateReqs{ 0 };
        std::atomic<int64_t> outStateless{ 0 };
        std::atomic<int64_t> outStateBytes{ 0 };
        std::atomic<int64_t> migrateUs{ 0 };
        std::atomic<int64_t> inUnits{ 0 };
        std::atomic<int64_t> inStateReqs{ 0 };
        std::atomic<int64_t> inStateless{ 0 };
        std::atomic<int64_t> inStateBytes{ 0 };
        std::atomic<int64_t> fwdTombstone{ 0 };
        std::atomic<int64_t> fwdOwner{ 0 };
        std::atomic<int64_t> rerouted{ 0 };

        void reset()
        {
            for (auto* counter : { &handovers,
                                   &failed,
                                   &outUnits,
                                   &outStateReqs,
                                   &outStateless,
                                   &outStateBytes,
                                   &migrateUs,
                                   &inUnits,
                                   &inStateReqs,
                                   &inStateless,
                                   &inStateBytes,
                                   &fwdTombstone,
                                   &fwdOwner,
                                   &rerouted }) {
                counter->store(0, std::memory_order_relaxed);
            }
        }
    };
    FluxMigrationCounters fluxCounters;

    // Marks `msg` as having gone on to another worker once more.
    static void countForward(faabric::Message& msg)
    {
        msg.set_forwardcount(msg.forwardcount() + 1);
    }

    // Queue depths the last rebalancing round saw, by queue key: tells a
    // paridx that has had work waiting all along from one hit by a fresh
    // burst. Only touched by the rebalancing thread.
    std::map<std::string, int> fluxPreviousDepths;

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

    // ---- ModeFlux units of state ----
    // Ownership, locking and migration all work on units of state (see
    // StateAwareScheduler::fluxUnitKey): the whole paridx of a stateful
    // operator, or one shard of a partitioned operator's single paridx. Every
    // map below is keyed by unit key.

    // Units this worker has admitted requests for -- owned per Redis, or not
    // needing state at all -- plus the paridx whose FunctionState exists here.
    // Keeps admitMessageFlux to one set lookup per message once an operator is
    // running.
    std::shared_mutex fluxKnownUnitsMx;
    std::set<std::string> fluxKnownUnits;

    // Throttle for logFluxExecutorCensus.
    std::atomic<int64_t> lastFluxExecutorLogMs{ 0 };

    // ---- ModeFlux rebalancing ----
    // Guards the four maps below. Never held across a network call.
    std::mutex fluxMigrationMx;
    // Units being handed off right now. Requests for them are parked in
    // fluxPendingMsgs rather than queued, and nothing dispatches them.
    std::set<std::string> fluxMigratingUnits;
    std::map<std::string, std::vector<std::unique_ptr<faabric::Message>>>
      fluxPendingMsgs;
    // Units this worker handed off. A request that still reaches us for one
    // was routed on a stale owner and is forwarded. Dropped if the unit ever
    // comes back.
    std::map<std::string, faabric::ShardMove> fluxMovedUnits;
    // Earliest time (epoch ms) a unit may move again, so it does not bounce
    // between workers on consecutive rounds of stale stats.
    std::map<std::string, int64_t> fluxUnitCooldownUntilMs;
    // Size of fluxMigratingUnits + fluxMovedUnits, read lock-free by the
    // enqueue and dispatch paths to skip the mutex until the first migration.
    std::atomic<int> fluxRouteRecords{ 0 };

    // One lock per unit. A dispatched batch holds the lock of every unit its
    // requests address, shared, until the batch has finished executing; a
    // migration holds it exclusively, which is how it waits out every running
    // access to the unit -- including one that has read and locked the state
    // and has yet to write it back. Batches of other shards of the same
    // paridx are not held up.
    std::shared_mutex fluxUnitLocksMx;
    std::map<std::string, std::shared_ptr<std::shared_mutex>> fluxUnitLocks;
    std::shared_ptr<std::shared_mutex> getFluxUnitLock(
      const std::string& unitKey);

    std::thread fluxRebalanceThread;
    // ms between rebalancing rounds; 0 disables rebalancing. Also the horizon
    // a round plans for: what a paridx will not get through here before the
    // next round is what is worth moving.
    int fluxRebalancePeriod = 1000;
    // ms a unit stays put after it moved.
    int fluxMigrateCooldown = 5000;
    // Most requests one worker hands off per round, over all targets,
    // stateless and stateful alike. A unit goes whole or not at all, so one
    // whose backlog exceeds what is left is skipped -- unless it is the first
    // unit of the round, which may go over, or a unit bigger than the budget
    // could never move.
    int fluxMigrateRequests = 10000;
    // Executors a worker can run at once. Flux caps executors per paridx, not
    // per worker; this is what bounds a worker as a whole.
    int fluxWorkerSlots = 0;

    void fluxRebalanceLoop();

    /**
     * ModeFlux: one rebalancing round.
     *
     * When: a paridx is behind here -- more is waiting than it will finish
     * before the next round, judged by the rate it actually achieved over
     * the last window (or, for a fresh burst on a paridx that was idle, by
     * all of its run slots).
     *
     * How much: only that excess. What it will get through here stays.
     *
     * Which: the oldest input first. Stateless requests one by one, units of
     * state whole with every request waiting for them, ranked by their
     * oldest request. A partitioned paridx sheds shards; a stateful paridx
     * only moves if the target would drain it sooner than it drains here.
     *
     * Where: to the worker that can get through the most of it before the
     * next round, after paying the measured migration cost -- free run slots
     * of the paridx and free executor slots, less its own backlog. Between
     * workers offering the same number of batches, one already running the
     * operator or holding its state wins.
     *
     * Each target gets its share in a single migration, and a round moves at
     * most fluxMigrateRequests requests.
     */
    void rebalanceFlux();

    /**
     * ModeFlux: hand `units` (unit keys, each with the requests waiting for
     * it) and up to the given count of requests from each stateless queue in
     * `stateless` to `target`, in one request.
     *
     * Blocks new dispatches of every unit, waits for running ones to finish,
     * ships the lot, flips all owners in Redis at once, then forwards
     * whatever arrived meanwhile. All or nothing: if the hand-over fails,
     * every unit stays and every request goes back where it was. Returns
     * {units moved, stateless requests moved}.
     */
    std::pair<int, int> migrateToWorkerFlux(
      const std::string& target,
      const std::vector<std::string>& units,
      const std::vector<std::pair<std::string, int>>& stateless);

    /**
     * ModeFlux enqueue hook. Parks a request for a unit being migrated, or
     * collects one for a unit we handed off into `forwards` (by new owner).
     * Returns true if the request was taken; false to queue it here.
     */
    bool divertMessageFlux(
      std::unique_ptr<faabric::Message>& msg,
      const std::string& unitKey,
      std::map<std::string, std::list<std::unique_ptr<faabric::Message>>>&
        forwards);

    /**
     * ModeFlux dispatch hook. Takes the lock of unit `unitKey` for a batch
     * about to run, adding it to `locks` unless the batch already holds it.
     * Fails, and takes nothing, if the unit is being migrated or was handed
     * off: the caller must then not run the request (see
     * returnUndispatchableFlux).
     */
    bool lockUnitForDispatchFlux(
      const std::string& unitKey,
      std::map<std::string, std::unique_ptr<faabric::util::SharedLock>>& locks);

    /**
     * ModeFlux dispatch hook. A request pulled from a queue that must not run
     * here after all: parked if its unit is mid-migration, forwarded if the
     * unit was handed off, otherwise put back in its queue.
     */
    void returnUndispatchableFlux(std::unique_ptr<faabric::Message> msg,
                                  const std::string& unitKey);

    void clearFluxMigrationState();

    /**
     * The hosts this worker may route to. ModeFlux pins no operator to a
     * worker and claims a unit's owner on demand, so every registered host
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
     * ModeFlux: decide whether a request for unit `unitKey` may run here, the
     * first time a request for that unit arrives, and bring the unit's
     * function state into being if so.
     *
     * ModeFlux pre-creates nothing -- the planner places no state and sends
     * no state-info broadcast -- so the arriving request is what creates the
     * state it addresses. Units handed off are caught earlier, by
     * divertMessageFlux. What is left is checked against Redis, the
     * authority on ownership: a request whose unit Redis gives to another
     * worker was routed on a stale view and must not run here, or it would
     * run against state this worker does not hold.
     *
     * A partitioned operator's paridx is split by shard across workers, so
     * its FunctionState is created without the per-paridx ownership check and
     * holds only the keys of the shards owned here.
     *
     * Returns the owner to forward the request to, or an empty string to
     * queue it here. Stateless operators and known units are always kept.
     */
    std::string admitMessageFlux(const faabric::Message& msg,
                                 const std::string& userFuncPar,
                                 const std::string& unitKey);

    /**
     * ModeFlux only: one line naming every paridx's executors as free/total,
     * plus the worker-wide total.
     *
     * Logged from the blocked path of executorAvailable, where one paridx has
     * hit its own cap. The whole worker is listed rather than just that paridx
     * because the cap is per paridx and the worker has no ceiling of its own:
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
