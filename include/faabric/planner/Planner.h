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

    void rescheduleApp(int rescheduleMode, int hostNum = 0);

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
    std::thread processWaitingQueueThread;
    void processWaitingQueueLoop();

    // ---- Batch Execution ----
    bool stopThreadTimer = false;

    // ---- Batch Call Scheduled Requests ----
    std::thread dequeueScheduledMsgsThread;

    std::thread updateRuntimeStatsThread;

    int scheduleMode = 0;
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

    int runtimeReconfigPeriod = 5000; // ms

    // Auto-scaling: evaluate host count every scalingDecisionPeriodMs
    int scalingDecisionPeriodMs = 10000; // ms

    // Minimum interval between two consecutive reschedule operations (ms)
    // Periodic reschedule interval
    long periodicRescheduleIntervalMs = 10000; // ms
    long lastPeriodicRescheduleMs = 0;

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

    void doRescheduleMessages();

    void updateRuntimeStats();

    std::map<std::string, std::unique_ptr<faabric::WorkerStats>>
    fetchWorkerStatsAsync(const std::vector<std::string>& targetIps);
};

Planner& getPlanner();
}
