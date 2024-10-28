#pragma once

#include <faabric/executor/Executor.h>
#include <faabric/planner/PlannerClient.h>
#include <faabric/proto/faabric.pb.h>
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

/*
 * A queue stores the uninvoked requests.
 */
class BatchQueue
{
  public:
    BatchQueue(std::string userFuncParIn)
    {
        userFuncPar = userFuncParIn;
        // Last time is the lastest time bewtween earliest insert time and the
        // lastest invoke time.
        lastTime = faabric::util::getGlobalClock().epochMillis();
    }
    std::string userFuncPar;
    long lastTime;
    std::queue<std::shared_ptr<faabric::Message>> batchQueue;

    // Attributes for partitioned stateful
    int lockVersion;
    int rangeStart;
    int rangeEnd;

    void insertMsg(std::shared_ptr<faabric::Message> msg)
    {
        // If the queue is empty, which means insert the first msg, reset the
        // invoke time.
        if (batchQueue.size() == 0) {
            resetlastTime();
        }
        batchQueue.push(msg);
    }
    int getTimeInterval()
    {
        return faabric::util::getGlobalClock().epochMillis() - lastTime;
    }
    void resetlastTime()
    {
        lastTime = faabric::util::getGlobalClock().epochMillis();
    }
};

class Scheduler
{
  public:
    Scheduler();

    ~Scheduler();

    void executeBatch(std::shared_ptr<faabric::BatchExecuteRequest> req);

    void executeBatchLazy(std::shared_ptr<faabric::BatchExecuteRequest> req);

    void enqueueMessageBatch(std::shared_ptr<faabric::MessageBatch> msgs);

    // Check the waiting queue peroiodically.
    void batchTimerCheck();

    void executeBatchForQueue(const std::string& userFuncPar,
                              BatchQueue& waitingBatch,
                              faabric::util::FullLock& lock);

    void executeBatchForPartitionQueue(
      const std::string& userFuncPar,
      faabric::util::PartitionedStateMessageQueue& waitingBatch,
      faabric::util::FullLock& lock);

    void resetParameter(std::string key, int32_t value);
    
    void resetBatchsize(int32_t newSize);

    void reset();

    void resetThreadLocalCache();

    void shutdown();

    bool isShutdown() { return _isShutdown; }

    void broadcastSnapshotDelete(const faabric::Message& msg,
                                 const std::string& snapshotKey);

    int reapStaleExecutors();

    long getFunctionExecutorCount(const faabric::Message& msg);

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

    std::string getThisHost();

    void addHostToGlobalSet();

    void addHostToGlobalSet(
      const std::string& host,
      std::shared_ptr<faabric::HostResources> overwriteResources = nullptr);

    void removeHostFromGlobalSet(const std::string& host);

    void setThisHostResources(faabric::HostResources& res);

    void resetMaxReplicas(int32_t newMaxReplicas);

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

  private:
    std::string thisHost;

    faabric::util::SystemConfig& conf;

    std::shared_mutex mx;

    std::atomic<bool> _isShutdown = false;

    // Maximum number of replicas per function
    int maxReplicas = 8;

    // Maximum number of concurrent executors in the worker
    int maxExecutors = 40;

    bool isRepartition = true;

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
      faabric::Message& msg,
      faabric::util::FullLock& schedulerLock);

    // ---- Accounting and debugging ----
    std::vector<faabric::Message> recordedMessages;

    // ---- Point-to-point ----
    faabric::transport::PointToPointBroker& broker;

    // A queue stores the uninvoked requests: MAP<UserFuncPar, Queue>
    std::map<std::string, BatchQueue> waitingQueues;
    // A queue stores the uninvoked partitioned function requests:
    // MAP<UserFuncPar, Queue>,
    std::map<std::string, faabric::util::PartitionedStateMessageQueue>
      partitionedWaitingQueues;
    // ---- Batch Execution ----
    std::thread batchTimerThread;
    bool stopBatchTimer = false;
};

}
