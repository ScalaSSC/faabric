#pragma once

#include <faabric/batch-scheduler/BatchScheduler.h>
#include <faabric/batch-scheduler/SchedulingDecision.h>
#include <faabric/batch-scheduler/StateAwareScheduler.h>
#include <faabric/planner/FunctionMetrics.h>
#include <faabric/planner/PlannerState.h>
#include <faabric/planner/planner.pb.h>
#include <faabric/proto/faabric.pb.h>
#include <faabric/snapshot/SnapshotRegistry.h>
#include <faabric/util/queue.h>

#include <shared_mutex>

namespace faabric::planner {
enum FlushType
{
    NoFlushType = 0,
    Hosts = 1,
    Executors = 2,
    SchedulingState = 3,
};

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

    std::vector<std::shared_ptr<Host>> getAvailableHosts();

    bool registerHost(const Host& hostIn, bool overwrite);

    // Best effort host removal. Don't fail if we can't
    void removeHost(const Host& hostIn);

    // ----------
    // Request scheduling public API
    // ----------

    // Setters/getters for individual message results

    void setMessageResultBatch(
      std::shared_ptr<faabric::BatchExecuteRequest> batchMsg);

    std::shared_ptr<faabric::Message> getMessageResult(
      std::shared_ptr<faabric::Message> msg);

    // Setter/Getter to bypass the planner's scheduling for a specific app
    void preloadSchedulingDecision(
      int appId,
      std::shared_ptr<batch_scheduler::SchedulingDecision> decision);

    std::shared_ptr<batch_scheduler::SchedulingDecision>
    getPreloadedSchedulingDecision(int32_t appId,
                                   std::shared_ptr<BatchExecuteRequest> ber);

    // Get all the results recorded for one batch
    std::shared_ptr<faabric::BatchExecuteRequestStatus> getBatchResults(
      int32_t appId);

    std::shared_ptr<faabric::batch_scheduler::SchedulingDecision>
    getSchedulingDecision(std::shared_ptr<BatchExecuteRequest> req);

    faabric::batch_scheduler::InFlightReqs getInFlightReqs();

    int getInFlightAppsSize();

    // Helper method to get the number of migrations that have happened since
    // the planner was last reset
    int getNumMigrations();

    // Main entrypoint to request the execution of batches

    void scheduleMessages(std::shared_ptr<BatchExecuteRequest> req,
                          bool isChained = false);

    void enqueueMessageBatch(
      std::vector<std::string> hosts,
      std::vector<std::unique_ptr<faabric::Message>> msgs);
    // ----------
    // Function State public API
    // ----------
    bool updateFuncParallelism(const std::string& userFunction,
                               int changedParallelism);

    bool resetBatchsize(int32_t newSize);

    bool resetMaxReplicas(int32_t newMaxReplicas);

    bool resetParameter(const std::string& key,
                        const int32_t value,
                        bool plannerParameter = false);

    // ----------
    // Metrics public API
    // ----------
    // Get all Mertrics: Locking congestion time, processing time, etc.
    // MAP<Function, MAP<metric, value>>
    std::map<std::string, FunctionMetrics> collectMetrics();

    void outputAppResultsToJson();

  private:
    std::shared_ptr<batch_scheduler::StateAwareScheduler> stateAwareScheduler =
      std::dynamic_pointer_cast<batch_scheduler::StateAwareScheduler>(
        faabric::batch_scheduler::getBatchScheduler());

    // There's a singleton instance of the planner running, but it must allow
    // concurrent requests
    std::shared_mutex plannerMx;
    std::shared_mutex plannerStateMx;

    PlannerState state;
    PlannerConfig config;

    faabric::util::ThreadSafeQueue<
      std::shared_ptr<faabric::BatchExecuteRequest>>
      batchExecuteReqQueue;

    // ---- Batch Execution ----
    bool stopThreadTimer = false;

    // ---- Batch Call Scheduled Requests ----
    std::thread dequeueScheduledMsgsThread;

    bool streamMode = faabric::util::getSystemConfig().streamMode;
    long lastParallelismUpdate;
    int parallelismUpdateInterval;
    bool isPreloadParallelism;

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

    int dispatchPeriod = 20; // ms

    void dequeueScheduledMsgs();
};

Planner& getPlanner();
}
