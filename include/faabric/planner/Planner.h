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

    bool resetParameter(const std::string& key,
                        const int32_t value,
                        bool plannerParameter = false);

    void rescheduleApp(int rescheduleMode, int hostNum = 0);

    void setPersistentState(const faabric::planner::MapMessage& mapMsg);

    // ----------
    // Metrics public API
    // ----------
    // Get all Mertrics: Locking congestion time, processing time, etc.
    // MAP<Function, MAP<metric, value>>
    std::map<std::string, FunctionMetrics> collectMetrics();

    std::string outputResult();

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

    // Record migration duration for each migration version.
    std::map<int, int> migrationDurations;

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
    int  scalingDecisionPeriodMs = 10000; // ms
    long lastScalingDecisionMs   = 0;

    // Minimum interval between two consecutive reschedule operations (ms)
    long rescheduleIntervalMs  = 20000;
    long lastRescheduleMs      = 0;

    int computeTargetHostNum(
      const faabric::planner::ApplicationMetrics::ScalingSignals& signals,
      int currentHostNum,
      int maxHostNum);

    void dequeueScheduledMsgs();

    bool isOutputting = false;
    std::atomic<bool> isWarmup = false;

    void doDistributeStatesInfo(
      int curVersion,
      const std::map<std::string, faabric::batch_scheduler::ScheduledOperator>
        preOperatorsMap,
      bool initialize = false);

    void doDistributeCustomInfo(std::shared_ptr<faabric::CustomRequest> msg);

    void doRescheduleMessages();

    void updateRuntimeStats();
};

Planner& getPlanner();
}
