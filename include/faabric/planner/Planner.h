#pragma once

#include <faabric/batch-scheduler/BatchScheduler.h>
#include <faabric/batch-scheduler/SchedulingDecision.h>
#include <faabric/batch-scheduler/StateAwareScheduler.h>
#include <faabric/planner/FunctionMetrics.h>
#include <faabric/planner/PlannerState.h>
#include <faabric/planner/planner.pb.h>
#include <faabric/proto/faabric.pb.h>
#include <faabric/scheduler/InstancesRuntimeStats.h>
#include <faabric/snapshot/SnapshotRegistry.h>
#include <faabric/util/queue.h>

#include <shared_mutex>
#include <utility>

namespace faabric::planner {
enum FlushType
{
    NoFlushType = 0,
    Hosts = 1,
    Executors = 2,
    SchedulingState = 3,
};

typedef std::map<std::string, std::shared_ptr<Host>> HostPtrMap;

/* The planner is a standalone component that has a global view of the state
 * of a distributed faabric deployment.
 */
class Planner
{
  public:
    Planner();

    ~Planner();

    // ----------
    // Planner config
    // ----------

    PlannerConfig getConfig();

    void printConfig() const;

    // ----------
    // Util public API
    // ----------

    bool reset();

    bool flush(faabric::planner::FlushType flushType);

    // ----------
    // Host membership public API
    // ----------

    std::vector<std::shared_ptr<Host>> getAvailableHosts(bool locked = false);

    bool registerHost(const Host& hostIn, bool overwrite);

    // Return wheter the host map has been updated and the newest host map
    const std::pair<bool, HostPtrMap&> getRegisteredHost(std::string hostIp);

    // Best effort host removal. Don't fail if we can't
    void removeHost(const Host& hostIn);

    // ----------
    // Request scheduling public API
    // ----------

    // Setters/getters for individual message results

    void setMessageResultBatch(
      std::shared_ptr<faabric::BatchExecuteRequest> batchMsg);

    // Takes in every result batch that is waiting under one hold of the
    // request-status lock.
    void drainMessageResults();

    // Get all the results recorded for one batch
    std::shared_ptr<faabric::BatchExecuteRequestStatus> getBatchResults(
      int32_t appId);

    std::shared_ptr<faabric::batch_scheduler::SchedulingDecision>
    getSchedulingDecision(std::shared_ptr<BatchExecuteRequest> req);

    int getInFlightAppsSize();

    // Helper method to get the number of migrations that have happened since
    // the planner was last reset
    int getNumMigrations();

    // Main entrypoint to request the execution of batches

    bool enqueueBatchRequest(std::shared_ptr<faabric::BatchExecuteRequest> req);

    void scheduleMessages(std::shared_ptr<BatchExecuteRequest> req,
                          bool isChained = false);

    void doEnqueueSchedMessages(
      std::vector<std::string> hosts,
      std::vector<std::unique_ptr<faabric::Message>> msgs);

    // ----------
    // Function State public API
    // ----------
    bool registerApp(faabric::planner::RegisterApplicationRequest& rawReq,
                     std::unique_ptr<batch_scheduler::Application> app);

    void distributeApp(faabric::planner::RegisterApplicationRequest& rawReq);

    bool resetParameter(
      const faabric::planner::ResetStreamParameterRequest& req);

    // `trigger` says who asked: "external" (the HTTP endpoint), or the
    // planner itself on "qos" / "degradation" (FaaSFlow, schedule mode 3),
    // then with the latency that set it off and what it was held against.
    void rescheduleApp(int rescheduleMode,
                       int hostNum = 0,
                       const std::string& trigger = "external",
                       double triggerLatencyMs = 0,
                       double triggerReferenceMs = 0);

    void setPersistentState(const faabric::planner::MapMessage& mapMsg);

    std::string getPersistentStateFromWorker(const std::string& key);

    void setPersistentStateFromWorker(
      const faabric::planner::MapMessage& mapMsg);

    // ----------
    // Metrics public API
    // ----------
    // Get all Mertrics: Locking congestion time, processing time, etc.
    // MAP<Function, MAP<metric, value>>
    std::map<std::string, FunctionMetrics> collectMetrics();

    std::string outputResult();

    // Predict how many hosts are needed for a given input rate using the
    // current model parameters, without triggering a reschedule.
    int predictHostNum(double inputRate);

  private:
    std::shared_ptr<batch_scheduler::StateAwareScheduler> stateAwareScheduler =
      std::dynamic_pointer_cast<batch_scheduler::StateAwareScheduler>(
        faabric::batch_scheduler::getBatchScheduler());

    // There's a singleton instance of the planner running, but it must allow
    // concurrent requests
    // Two mutex are used where plannerMx is used for message info collect, e.g.
    // inFlightReqs, message results, etc.
    std::shared_mutex plannerMx;
    std::shared_mutex reconfigMx;
    // plannerStateMx is used for function scheduling.
    // std::shared_mutex plannerStateMx;

    PlannerState state;
    PlannerConfig config;

    // Batch scheduling queue
    std::atomic<int> maxWaitingQueueSize{ 100000 };
    std::atomic<int> maxInflightApps{ 10000 };
    faabric::util::ThreadSafeQueue<std::shared_ptr<faabric::Message>>
      waitingMessageQueue;
    // Next Message::inputSeq to hand out. Input batches take a contiguous
    // block each, so a batch's messages keep their order.
    std::atomic<int64_t> nextInputSeq{ 1 };

    // Bumped by every flush. The runtime-stats thread keeps the stats of the
    // previous round to hand back to the workers with the next one; when this
    // has moved on, those stats belong to a flushed run and are dropped
    // instead -- otherwise the first round of the new run would hand the
    // workers the old run's state moves and loads.
    std::atomic<uint64_t> flushGeneration{ 0 };
    std::thread processWaitingQueueThread;
    void processWaitingQueueLoop();

    // Result batches from the workers, waiting to be taken in. Whichever
    // server thread holds resultDrainMx takes in all of them at once; the
    // others leave theirs here and return. Taking the request-status lock
    // once per batch instead had every server thread queue up on it, and a
    // worker sends a batch for every couple of results.
    faabric::util::ThreadSafeQueue<std::shared_ptr<faabric::BatchExecuteRequest>>
      resultBatchQueue;
    std::mutex resultDrainMx;

    // Wake the input scheduler (inputs arrived, or an app finished and freed
    // an in-flight slot) and the dispatcher (requests scheduled), instead of
    // them polling on a timer.
    faabric::util::WakeSignal inputWake;
    faabric::util::WakeSignal scheduledWake;

    // ---- Batch Execution ----
    bool stopThreadTimer = false;

    // ---- Batch Call Scheduled Requests ----
    std::thread dequeueScheduledMsgsThread;

    std::thread updateRuntimeStatsThread;

    int scheduleMode = 0;

    // The RLS coefficient estimator exists for the Binpack (mode 0) capacity
    // model: ScalingSignals' coeffC/alpha/beta/gamma are read by
    // computeTargetHostNum, groupNodesCapacity and computeChainedCostCoeff,
    // all of which only run for mode 0. Every other mode -- ModeFlux included
    // -- would be fitting it every stats round for nothing.
    static bool coeffEstimatorUsedBy(int mode)
    {
        return mode == faabric::batch_scheduler::ModeBinpack;
    }
    bool streamMode = faabric::util::getSystemConfig().streamMode;
    long lastParallelismUpdate;
    int parallelismUpdateInterval;
    bool isPreloadParallelism;

    // Migration is used to track the number of migrations that have happened
    // since the last reset. This is nessecary to make sure worker knows when it
    // has received all the migrated messages and states.
    int migrationVersion = 0;

    struct MigrationRecord
    {
        int duration;
        int oldHosts;
        int newHosts;
    };
    // Record duration and old/new worker counts for each migration version.
    std::map<int, MigrationRecord> migrationDurations;

    // What the workers reported for the last state-info sync, summed over
    // them (doDistributeStatesInfo). Guarded by plannerMx.
    struct SyncCost
    {
        int workers = 0;
        // Workers whose syncStateInfo failed (down, timed out): skipped.
        int failedWorkers = 0;
        // When the planner had the last worker's reply (epoch us; 0: none).
        int64_t lastAckUs = 0;
        // Each worker's whole pause, stop to resume (round 3's reply).
        int64_t workerPauseUsSum = 0;
        int64_t workerPauseUsMax = 0;
        // The longest a worker took to stop: its running batches finishing.
        int64_t drainUsMax = 0;
        // The three rounds of a re-plan as the planner saw them, each from
        // sending to the last reply.
        int64_t pausePhaseUs = 0;
        int64_t migratePhaseUs = 0;
        int64_t resumePhaseUs = 0;
        int64_t packUsMax = 0;
        int64_t sendUsMax = 0;
        int64_t waitUsMax = 0;
        int64_t stateBytes = 0;
        int64_t sentStateBytes = 0;
        int64_t queuedMessages = 0;
        int64_t sentMessages = 0;
    };
    SyncCost lastSyncCost;

    // One re-plan of the application as the planner saw it (rescheduleApp):
    // while it runs the planner holds plannerMx, so it schedules no input.
    struct RescheduleRecord
    {
        int version = 0;
        int mode = 0;
        std::string trigger;
        double triggerLatencyMs = 0;
        double triggerReferenceMs = 0;
        int64_t startMs = 0;
        // Waiting for plannerMx, then holding it: planning, the workers'
        // hand-over, re-routing what the planner had queued.
        int64_t lockWaitMs = 0;
        int64_t planMs = 0;
        int64_t distributeMs = 0;
        int64_t requeueMs = 0;
        int64_t pauseMs = 0;
        // The migration: from the planner starting to compute the partition
        // until every worker has moved its state and requests and executes
        // again (its reply) and the planner schedules again.
        double migrationMs = 0;
        int inFlightInputs = 0;
        int plannerRequeued = 0;
        int operators = 0;
        // Operators running on a different set of workers than before, and
        // the share of all operator weight now on a worker that did not have
        // it.
        int operatorsMoved = 0;
        double weightMoved = 0;
        bool placementChanged = false;
        // Operators the re-plan redeployed.
        int operatorsMigrated = 0;
        int oldHosts = 0;
        int newHosts = 0;
        SyncCost workers;
    };
    std::vector<RescheduleRecord> rescheduleRecords;
    // Re-plans that came out with the placement they started from: nothing
    // migrated, so they are no migration cost and get no record.
    int unchangedReschedules = 0;

    // std::atomic<unsigned int> atomicChainedCounter{ 1 };

    // Snapshot registry to distribute snapshots in THREADS requests
    faabric::snapshot::SnapshotRegistry& snapshotRegistry;

    // ----------
    // Util private API
    // ----------

    void flushHosts();

    void flushExecutors();

    void flushSchedulingState();

    // ----------
    // Host membership private API
    // ----------

    // Check if a host's registration timestamp has expired
    bool isHostExpired(std::shared_ptr<Host> host, long epochTimeMs = 0);

    // ----------
    // Request scheduling private API
    // ----------
    // bool isUpdateState = false;
    int schedHostNum = 0;

    int dispatchPeriod = 20; // ms

    faabric::scheduler::InstancesRuntimeStats runtimeStats;

    int runtimeReconfigPeriod = 1000; // ms

    // Auto-scaling: evaluate host count every scalingDecisionPeriodMs
    int scalingDecisionPeriodMs = 10000; // ms

    // Minimum interval between two consecutive reschedule operations (ms)
    // Periodic reschedule interval
    long periodicRescheduleIntervalMs = 10000; // ms
    long lastPeriodicRescheduleMs = 0;

    // FaaSFlow (schedule mode 3) partition iterations started by the planner
    // itself, as the paper has it: "each partition iteration is activated
    // when the workflow experiences significant performance degradation or
    // QoS violation". Checked once per stats round on the mean end-to-end
    // latency of the last faasFlowLatencyWindowSec seconds:
    //  - QoS violation: above faasFlowQosMs (0 = no target);
    //  - degradation: more than faasFlowDegradePct percent above the best
    //    the current partition has delivered (0 = off).
    // Never sooner than faasFlowCooldownMs after the previous iteration,
    // which is also the time its feedback metrics have to build up. Both
    // thresholds 0 (the default) leaves re-partitioning to the endpoint.
    std::atomic<int> faasFlowQosMs = 0;
    std::atomic<int> faasFlowDegradePct = 0;
    std::atomic<int> faasFlowCooldownMs = 10000;
    std::atomic<int> faasFlowLatencyWindowSec = 3;
    // When the last re-plan finished, and the best window latency since.
    std::atomic<int64_t> lastRescheduleEndMs = 0;
    std::atomic<double> faasFlowReferenceLatencyUs = 0;
    void evaluateFaaSFlowRepartition();

    // Input-rate change detection
    double stableInputRate = 0.0;       // baseline rate at last reschedule
    double pendingInputRate = 0.0;      // rate when change was first detected
    long inputRateChangeDetectedMs = 0; // 0 = no change pending
    int inputRateStabilityWindowMs = 5000; // ms to wait for stability
    double inputRateDeviationRatio = 0.2;  // tolerance: 0.2 = 20%
    double pendingDevToleranceRatio = 0.2;

    // Per-worker feasibility search: the largest fraction of a worker's CPU
    // budget C the scaler is allowed to plan up to. Leaving headroom (< 1.0)
    // absorbs the prediction error and short-term bursts.
    double workerLoadHeadroom = 0.9;

    // Absolute tolerance (req/s) of the capacity Binpack saturation binary
    // search (StateAwareScheduler::groupNodesCapacity). Pushed to the
    // scheduler via resetParameter("capacity_search_tol").
    double capacitySearchTol = 100.0;

    // Per-round growth ceiling of the Binpack executor budget, as a multiple
    // of the executors currently running. Pushed to the scheduler via
    // resetParameter("exec_budget_growth_cap").
    double execBudgetGrowthCap = 2.0;

    // Soft-affinity score weights for schedule mode 4
    // (Score = ws·Data + wb·Balance). Pushed to the scheduler via
    // resetParameter("soft_affinity_ws" / "soft_affinity_wb").
    double softAffinityWs = 0.5;
    double softAffinityWb = 0.5;

    // Minimum samples before the mode-4 non-linear overhead curve r(p) is
    // trusted. Pushed via resetParameter("nl_min_fit_samples").
    int nlMinFitSamples = 3;

    // Per-worker executor capacity (one executor = one CPU) recorded when
    // resetParameter("max_executors") passes through on its way to the
    // workers. 0 = not configured;
    int maxExecutorsPerWorker = 0;

    // CLAST-placement hybrid for schedule modes 3 and 4: keep only their
    // worker-count estimate and place the operators with the mode-0 (CLAST)
    // capacity packer. Pushed to the scheduler via
    // resetParameter("clast_placement"). Ignored by every other schedule mode.
    bool isClastPlacement = false;

    int computeTargetHostNum(
      const faabric::planner::ApplicationMetrics::ScalingSignals& signals,
      int currentHostNum,
      int maxHostNum);

    // Evaluate whether a reschedule should be triggered this cycle.
    // Returns true if a reschedule was fired.
    bool evaluateReschedule(
      const faabric::planner::ApplicationMetrics::ScalingSignals& signals,
      int maxHostNum);

    void dequeueScheduledMsgs();

    bool isOutputting = false;
    std::atomic<bool> isWarmup = false;
    std::atomic<bool> autoScalingEnabled = false;

    void doDistributeStatesInfo(
      int curVersion,
      const std::map<std::string, faabric::batch_scheduler::ScheduledOperator>
        preOperatorsMap,
      bool initialize = false);

    void doDistributeCustomInfo(std::shared_ptr<faabric::CustomRequest> msg);

    // Returns how many queued messages it routed again.
    int doRescheduleMessages();

    void updateRuntimeStats();

    std::map<std::string, std::unique_ptr<faabric::WorkerStats>>
    fetchWorkerStatsAsync(const std::vector<std::string>& targetIps,
                          const faabric::WorkerRuntimeStatsRequest& req);
};

Planner& getPlanner();
}
