#include <faabric/batch-scheduler/SchedulingDecision.h>
#include <faabric/executor/Executor.h>
#include <faabric/executor/ExecutorContext.h>
#include <faabric/executor/ExecutorTask.h>
#include <faabric/mpi/MpiWorldRegistry.h>
#include <faabric/planner/PlannerClient.h>
#include <faabric/proto/faabric.pb.h>
#include <faabric/scheduler/Scheduler.h>
#include <faabric/snapshot/SnapshotClient.h>
#include <faabric/snapshot/SnapshotRegistry.h>
#include <faabric/state/State.h>
#include <faabric/transport/PointToPointBroker.h>
#include <faabric/util/clock.h>
#include <faabric/util/config.h>
#include <faabric/util/dirty.h>
#include <faabric/util/environment.h>
#include <faabric/util/func.h>
#include <faabric/util/gids.h>
#include <faabric/util/locks.h>
#include <faabric/util/logging.h>
#include <faabric/util/macros.h>
#include <faabric/util/memory.h>
#include <faabric/util/queue.h>
#include <faabric/util/snapshot.h>
#include <faabric/util/string_tools.h>
#include <faabric/util/timing.h>

// Default snapshot size here is set to support 32-bit WebAssembly, but could be
// made configurable on the function call or language.
#define ONE_MB (1024L * 1024L)
#define ONE_GB (1024L * ONE_MB)
#define DEFAULT_MAX_SNAP_SIZE (4 * ONE_GB)

#define POOL_SHUTDOWN -1
#define STREAM_BATCH -2

namespace faabric::executor {

// TODO - avoid the copy of the message here?
Executor::Executor(faabric::Message& msg)
  : boundMessage(msg)
  , reg(faabric::snapshot::getSnapshotRegistry())
  , tracker(faabric::util::getDirtyTracker())
  , threadPoolSize(faabric::util::getUsableCores())
  , lastExec(faabric::util::startTimer())
  , threadPoolThreads(threadPoolSize)
  , threadTaskQueues(threadPoolSize)
{
    faabric::util::SystemConfig& conf = faabric::util::getSystemConfig();

    assert(!boundMessage.user().empty());
    assert(!boundMessage.function().empty());

    // Set an ID for this Executor
    id = conf.endpointHost + "_" + std::to_string(faabric::util::generateGid());
    SPDLOG_DEBUG("Starting executor {}", id);

    // Mark all thread pool threads as available
    for (int i = 0; i < threadPoolSize; i++) {
        availablePoolThreads.insert(i);
    }
}

/**
 * Shuts down the executor and clears all its state, including its thread pool.
 *
 * This must be called before destructing an executor. This is because the
 * tidy-up requires implementations of virtual methods held in subclasses, that
 * may depend on state that those subclass instances hold. Because destructors
 * run in inheritance order, this means that state may have been destructed
 * before the executor destructor runs.
 */
void Executor::shutdown()
{
    // This method will be called during the scheduler reset and destructor,
    // hence it cannot rely on the presence of the scheduler or any of its
    // state.

    SPDLOG_DEBUG("Executor {} shutting down", id);

    for (int i = 0; i < threadPoolThreads.size(); i++) {
        // Skip any uninitialised, or already remove threads
        if (threadPoolThreads[i] == nullptr) {
            continue;
        }

        // Send a kill message
        SPDLOG_TRACE("Executor {} killing thread pool {}", id, i);
        threadTaskQueues[i].enqueue(
          std::make_tuple(ExecutorTask(POOL_SHUTDOWN, nullptr),
                          std::unique_ptr<faabric::util::SharedLock>()));

        // Wait for thread to terminate
        if (threadPoolThreads[i]->joinable()) {
            threadPoolThreads[i]->request_stop();
            threadPoolThreads[i]->join();
        }

        // Mark as killed
        threadPoolThreads[i] = nullptr;
    }

    _isShutdown = true;
}

Executor::~Executor()
{
    if (!_isShutdown) {
        SPDLOG_ERROR("Destructing Executor {} without shutting down first", id);
    }
}

clockid_t Executor::executeBatchTasks(
  std::shared_ptr<faabric::BatchExecuteRequest> req,
  std::unique_ptr<std::shared_lock<std::shared_mutex>> stateLock)
{
    const std::string funcStr = faabric::util::funcToString(req);
    auto& firstMsg = req->mutable_messages()->at(0);
    const std::string userFuncPar = firstMsg.user() + "_" +
                                    firstMsg.function() + "_" +
                                    std::to_string(firstMsg.parallelismid());
    std::string msgIds = "";
    for (int i = 0; i < req->messages_size(); i++) {
        msgIds += std::to_string(req->messages(i).id()) + " ";
    }
    SPDLOG_TRACE("{} executing appid: {}/ msgIds: {} of {} tasks of {}",
                 id,
                 req->appid(),
                 msgIds,
                 req->messages_size(),
                 userFuncPar);

    // Note that this lock is specific to this executor, so will only block
    // when multiple threads are trying to schedule tasks. This will only
    // happen when child threads of the same function are competing to
    // schedule more threads, hence is rare so we can afford to be
    // conservative here.
    faabric::util::UniqueLock lock(threadsMutex);

    // Update the last-executed time for this executor
    lastExec = faabric::util::startTimer();

    std::string thisHost = faabric::util::getSystemConfig().endpointHost;

    std::string snapshotKey = firstMsg.snapshotkey();

    // Non-threads need to restore from a snapshot if they are given a
    // snapshot key.
    if (!firstMsg.snapshotkey().empty()) {
        // Restore from snapshot if provided
        std::string snapshotKey = firstMsg.snapshotkey();
        SPDLOG_DEBUG("Restoring {} from snapshot {}", funcStr, snapshotKey);
        restore(snapshotKey);
    } else {
        SPDLOG_TRACE(
          "Not restoring {}. threads=false, key={}", funcStr, snapshotKey);
    }

    // Batch Counter is used to track the runing tasks in executor.
    batchCounter.fetch_add(1, std::memory_order_release);

    if (availablePoolThreads.empty()) {
        SPDLOG_ERROR("No available thread pool threads (size: {})",
                     threadPoolSize);
        throw std::runtime_error("No available thread pool threads!");
    }

    // Take next from those that are available
    int threadPoolIdx = *availablePoolThreads.begin();
    availablePoolThreads.erase(threadPoolIdx);

    // In there it should always be thread 0
    SPDLOG_TRACE("Assigned current batch functions to thread {}",
                 threadPoolIdx);

    auto task = ExecutorTask(STREAM_BATCH, req);
    std::tuple<ExecutorTask, std::unique_ptr<faabric::util::SharedLock>>
      queueItem(std::move(task), std::move(stateLock));
    // Enqueue the task
    threadTaskQueues[threadPoolIdx].enqueue(std::move(queueItem));

    // Lazily create the thread
    if (threadPoolThreads.at(threadPoolIdx) == nullptr) {
        threadPoolThreads.at(threadPoolIdx) = std::make_shared<std::jthread>(
          std::bind_front(&Executor::threadPoolThread, this), threadPoolIdx);
    }

    clockid_t cid{};
    if (pthread_getcpuclockid(
          threadPoolThreads.at(threadPoolIdx)->native_handle(), &cid) != 0) {
        perror("pthread_getcpuclockid");
        // Handle error: maybe return an invalid value or throw
    }
    return cid;
}

long Executor::getMillisSinceLastExec()
{
    return faabric::util::getTimeDiffMillis(lastExec);
}

std::shared_ptr<faabric::util::SnapshotData> Executor::getMainThreadSnapshot(
  faabric::Message& msg,
  bool createIfNotExists)
{
    std::string snapshotKey = faabric::util::getMainThreadSnapshotKey(msg);
    bool exists = false;
    {
        faabric::util::SharedLock lock(threadExecutionMutex);
        exists = reg.snapshotExists(snapshotKey);
    }

    if (!exists && createIfNotExists) {
        faabric::util::FullLock lock(threadExecutionMutex);
        if (!reg.snapshotExists(snapshotKey)) {
            SPDLOG_DEBUG("Creating main thread snapshot: {} for {}",
                         snapshotKey,
                         faabric::util::funcToString(msg, false));

            std::shared_ptr<faabric::util::SnapshotData> snap =
              std::make_shared<faabric::util::SnapshotData>(getMemoryView(),
                                                            getMaxMemorySize());
            reg.registerSnapshot(snapshotKey, snap);
        } else {
            return reg.getSnapshot(snapshotKey);
        }
    } else if (!exists) {
        SPDLOG_ERROR("No main thread snapshot {}", snapshotKey);
        throw std::runtime_error("No main thread snapshot");
    }

    return reg.getSnapshot(snapshotKey);
}

/* TODO(thread-opt): currently we never delete snapshots
void Executor::deleteMainThreadSnapshot(const faabric::Message& msg)
{
    std::string snapshotKey = faabric::util::getMainThreadSnapshotKey(msg);

    if (reg.snapshotExists(snapshotKey)) {
        SPDLOG_DEBUG("Deleting main thread snapshot for {}",
                     faabric::util::funcToString(msg, false));

        // Broadcast the deletion
        sch.broadcastSnapshotDelete(msg, snapshotKey);

        // Delete locally
        reg.deleteSnapshot(snapshotKey);
    }
}
*/

void Executor::setThreadResult(
  faabric::Message& msg,
  int32_t returnValue,
  const std::string& key,
  const std::vector<faabric::util::SnapshotDiff>& diffs)
{
    bool isMaster =
      msg.mainhost() == faabric::util::getSystemConfig().endpointHost;
    if (isMaster) {
        if (!diffs.empty()) {
            // On main we queue the diffs locally directly, on a remote
            // host we push them back to main
            SPDLOG_DEBUG("Queueing {} diffs for {} to snapshot {} (group {})",
                         diffs.size(),
                         faabric::util::funcToString(msg, false),
                         key,
                         msg.groupid());

            auto snap = reg.getSnapshot(key);

            // Here we don't have ownership over all of the snapshot diff data,
            // but that's ok as the executor memory will outlast the snapshot
            // merging operation.
            snap->queueDiffs(diffs);
        }
    } else {
        // Push thread result and diffs together
        faabric::snapshot::getSnapshotClient(msg.mainhost())
          ->pushThreadResult(msg.appid(), msg.id(), returnValue, key, diffs);
    }

    // Finally, set the message result in the planner
    faabric::planner::getPlannerClient().setMessageResult(
      std::make_shared<faabric::Message>(msg));
}

void Executor::threadPoolThread(std::stop_token st, int threadPoolIdx)
{
    SPDLOG_DEBUG("Thread pool thread {}:{} starting up", id, threadPoolIdx);

    const auto conf = faabric::util::getSystemConfig();

    // We terminate these threads by sending a shutdown message, but having this
    // check means they won't hang infinitely if destructed.
    while (!st.stop_requested()) {
        SPDLOG_TRACE("Thread starting loop {}:{}", id, threadPoolIdx);

        ExecutorTask task;
        std::unique_ptr<faabric::util::SharedLock> stateLock;

        try {
            auto dequeuedItem =
              threadTaskQueues[threadPoolIdx].dequeue(conf.boundTimeout);
            task = std::move(std::get<0>(dequeuedItem));
            stateLock = std::move(std::get<1>(dequeuedItem));
        } catch (faabric::util::QueueTimeoutException& ex) {
            SPDLOG_TRACE(
              "Thread {}:{} got no messages in timeout {}ms, looping",
              id,
              threadPoolIdx,
              conf.boundTimeout);

            continue;
        }

        // If the thread is being killed, the executor itself
        // will handle the clean-up
        if (task.messageIndex == POOL_SHUTDOWN) {
            SPDLOG_DEBUG("Killing thread pool thread {}:{}", id, threadPoolIdx);
            return;
        }

        // If we want to execute batch-processing
        if (task.messageIndex == STREAM_BATCH) {
            SPDLOG_DEBUG(
              "Thread {}:{} executing task appid: {} (batch processing)",
              id,
              threadPoolIdx,
              task.req->appid());
            // Set up context
            ExecutorContext::set(this, task.req, task.messageIndex);
            faabric::scheduler::getScheduler().notifyExecutorStart();

            // Execute the task
            int32_t returnValue;
            int32_t executeBatchSize = task.req->messages_size();
            try {
                returnValue =
                  executeTask(threadPoolIdx, task.messageIndex, task.req);
            } catch (const std::exception& ex) {
                returnValue = 1;
                std::string errorMessage =
                  fmt::format("Task threw exception. What: {}", ex.what());
                SPDLOG_ERROR(errorMessage);
                for (int i = 0; i < task.req->messages_size(); i++) {
                    task.req->mutable_messages()->at(i).set_outputdata(
                      errorMessage);
                }
                throw ex;
            }
            // Unset context
            ExecutorContext::unset();

            // Set the return value
            for (int i = 0; i < task.req->messages_size(); i++) {
                task.req->mutable_messages()->at(i).set_returnvalue(
                  returnValue);
                task.req->mutable_messages()->at(i).set_executebatchsize(
                  executeBatchSize);
            }

            std::atomic_thread_fence(std::memory_order_release);
            int oldTaskCount = 0;
            bool isLastThreadInExecutor = false;
            oldTaskCount = batchCounter.fetch_sub(1);
            isLastThreadInExecutor = oldTaskCount == 1;
            assert(oldTaskCount >= 1);

            faabric::Message firstMsg = task.req->messages().at(0);
            SPDLOG_DEBUG("Task {} finished by thread {}:{} ({} left)",
                         faabric::util::funcToString(firstMsg, true),
                         id,
                         threadPoolIdx,
                         oldTaskCount - 1);

            if (isLastThreadInExecutor) {
                reset(firstMsg);
                releaseClaim();
            }

            // Return this thread index to the pool available for scheduling
            {
                faabric::util::UniqueLock lock(threadsMutex);
                availablePoolThreads.insert(threadPoolIdx);
            }

            faabric::scheduler::getScheduler().notifyExecutorFinished();
            // Enqueue the message result
            faabric::scheduler::getScheduler().enqueueSetResults(
              std::move(task.req));

            if (stateLock) {
                SPDLOG_DEBUG(
                  "statelock unlocked by thread {}:{}", id, threadPoolIdx);
                stateLock->unlock();
            }

            continue;
        }
    }
}

bool Executor::tryClaim()
{
    bool expected = false;
    bool wasClaimed = claimed.compare_exchange_strong(expected, true);
    return wasClaimed;
}

bool Executor::availableClaim()
{
    bool available = !claimed.load();
    return available;
}

void Executor::releaseClaim()
{
    claimed.store(false);
}

// ------------------------------------------
// HOOKS
// ------------------------------------------

int32_t Executor::executeTask(int threadPoolIdx,
                              int msgIdx,
                              std::shared_ptr<faabric::BatchExecuteRequest> req)
{
    return 0;
}

void Executor::reset(faabric::Message& msg)
{
    faabric::util::UniqueLock lock(threadsMutex);

    chainedMessages.clear();
}

std::span<uint8_t> Executor::getMemoryView()
{
    SPDLOG_WARN("Executor for {} has not implemented memory view method",
                faabric::util::funcToString(boundMessage, false));
    return {};
}

void Executor::setMemorySize(size_t newSize)
{
    SPDLOG_WARN("Executor has not implemented set memory size method");
}

size_t Executor::getMaxMemorySize()
{
    SPDLOG_WARN("Executor has not implemented max memory size method");

    return 0;
}

faabric::Message& Executor::getBoundMessage()
{
    return boundMessage;
}

bool Executor::isExecuting()
{
    int currentCount = batchCounter.load(std::memory_order_acquire);
    return currentCount > 0;
}

void Executor::restore(const std::string& snapshotKey)
{
    std::span<uint8_t> memView = getMemoryView();
    if (memView.empty()) {
        SPDLOG_ERROR("No memory on {} to restore {}", id, snapshotKey);
        throw std::runtime_error("No memory to restore executor");
    }

    // Expand memory if necessary
    auto snap = reg.getSnapshot(snapshotKey);
    setMemorySize(snap->getSize());

    // Map the memory onto the snapshot
    snap->mapToMemory({ memView.data(), snap->getSize() });
}

void Executor::addChainedMessage(const faabric::Message& msg)
{
    // SPDLOG_DEBUG("Adding chained message {} to executor", msg.id());
    faabric::util::UniqueLock lock(threadsMutex);
    // SPDLOG_DEBUG("LOCKED obtained by addChainedMessage");

    auto it = chainedMessages.find(msg.id());
    if (it != chainedMessages.end()) {
        SPDLOG_ERROR("Message {} already in chained messages!", msg.id());
        throw ChainedCallException("Message already registered!");
    }

    chainedMessages[msg.id()] = std::make_shared<faabric::Message>(msg);
}

const faabric::Message& Executor::getChainedMessage(int messageId)
{
    faabric::util::UniqueLock lock(threadsMutex);

    auto it = chainedMessages.find(messageId);
    if (it == chainedMessages.end()) {
        SPDLOG_ERROR("Message {} does not correspond to a chained function!",
                     messageId);
        throw ChainedCallException(
          "Unrecognised message ID for chained function");
    }

    return *(it->second);
}

std::vector<faabric::util::SnapshotDiff> Executor::mergeDirtyRegions(
  const Message& msg,
  const std::vector<char>& extraDirtyPages)
{
    std::vector<faabric::util::SnapshotDiff> diffs;
    auto mainThreadSnapKey = faabric::util::getMainThreadSnapshotKey(msg);

    // Stop non-thread-local tracking as we're the last in the batch
    std::span<uint8_t> memView = getMemoryView();
    tracker->stopTracking(memView);

    // Merge all dirty regions
    {
        faabric::util::FullLock lock(threadExecutionMutex);

        // Merge together regions from all threads
        faabric::util::mergeManyDirtyPages(dirtyRegions,
                                           threadLocalDirtyRegions);

        // Clear thread-local dirty regions, no longer needed
        threadLocalDirtyRegions.clear();

        // Merge the globally tracked regions
        std::vector<char> globalDirtyRegions = tracker->getDirtyPages(memView);
        faabric::util::mergeDirtyPages(dirtyRegions, globalDirtyRegions);
    }

    // Fill snapshot gaps with overwrite regions first
    auto snap = reg.getSnapshot(mainThreadSnapKey);
    snap->fillGapsWithBytewiseRegions();

    // Compare snapshot with all dirty regions for this executor
    {
        // Do the diffing
        faabric::util::FullLock lock(threadExecutionMutex);
        diffs = snap->diffWithDirtyRegions(memView, dirtyRegions);
        dirtyRegions.clear();
    }

    // If last in batch on this host, clear the merge regions (only
    // needed for doing the diffing on the current host)
    SPDLOG_DEBUG("Clearing merge regions for {}", mainThreadSnapKey);
    snap->clearMergeRegions();

    // FIXME: is it very expensive to return these diffs?
    return diffs;
}

std::set<unsigned int> Executor::getChainedMessageIds()
{
    faabric::util::UniqueLock lock(threadsMutex);

    std::set<unsigned int> returnSet;
    for (auto it : chainedMessages) {
        returnSet.insert(it.first);
    }

    return returnSet;
}
}
