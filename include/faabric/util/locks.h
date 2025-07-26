#pragma once

#include <faabric/util/logging.h>

#include <atomic>
#include <condition_variable>
#include <iostream>
#include <list>
#include <map>
#include <memory>
#include <mutex>
#include <queue>
#include <set>
#include <shared_mutex>
#include <thread>
#include <vector>

#define DEFAULT_FLAG_WAIT_MS 10000

namespace faabric::util {
typedef std::unique_lock<std::mutex> UniqueLock;
typedef std::unique_lock<std::shared_mutex> FullLock;
typedef std::shared_lock<std::shared_mutex> SharedLock;

class FlagWaiter : public std::enable_shared_from_this<FlagWaiter>
{
  public:
    FlagWaiter(int timeoutMsIn = DEFAULT_FLAG_WAIT_MS);

    void waitOnFlag();

    void setFlag(bool value);

  private:
    int timeoutMs;

    std::mutex flagMx;
    std::condition_variable cv;
    std::atomic<bool> flag;
};

class IndivLock
{
  public:
    IndivLock()
      : locked(false)
    {}

    void acquire();

    void release();

    bool isLocked();

  private:
    std::mutex mtx;
    std::condition_variable cv;
    bool locked;
    std::queue<std::thread::id> waiting_threads;
};

/***
 * This Lock is used by Paritioned State to partition the state into ranges and
 * only lock the required ranges.
 ***/

class RangeLock
{
  public:
    RangeLock(int begin, int end);
    void createVersion(int version, const std::map<int, int>& ranges);
    void acquire(int version, int begin, int end);
    void release(int version, int begin, int end);

    struct LockInfo
    {
        int currentVersion;
        int rangeMin;
        int rangeMax;
    };

    LockInfo getInfo() const;

  private:
    struct Range
    {
        std::mutex mtx;
        bool isLocked;

        struct WaitInfo
        {
            std::chrono::steady_clock::time_point timestamp;
            std::condition_variable* cv;
        };

        struct Compare
        {
            bool operator()(const WaitInfo& a, const WaitInfo& b)
            {
                // Earlier timestamp has higher priority
                return a.timestamp > b.timestamp;
            }
        };

        std::priority_queue<WaitInfo, std::vector<WaitInfo>, Compare> waitQueue;
        Range()
          : isLocked(false)
        {}
        Range(const Range&) = delete;
        Range& operator=(const Range&) = delete;
    };

    int rangeMin;
    int rangeMax;
    // The first version should start from 1
    int currentVersion = 0;
    std::map<int, std::map<std::pair<int, int>, Range>> versionLocks;
    std::mutex globalMutex;

    bool areSubRangesContinuous(const std::map<int, int>& ranges,
                                int min,
                                int max) const;
    bool canAcquireLock(int version, int begin, int end);
};

class ConditionVariableQueue
{
  public:
    // Push a new element to the queue
    void push(std::thread::id id, std::condition_variable* cv)
    {
        std::lock_guard<std::mutex> lock(mtx);
        if (id_map.find(id) != id_map.end()) {
            throw std::runtime_error("ID already exists in the queue");
        }
        queue.emplace_back(id, cv);
        id_map[id] = std::prev(queue.end());
    }

    // Pop the front element from the queue
    void pop()
    {
        std::lock_guard<std::mutex> lock(mtx);
        if (queue.empty()) {
            throw std::runtime_error("Queue is empty");
        }
        id_map.erase(queue.front().first);
        queue.pop_front();
    }

    // Get the front element of the queue
    std::pair<std::thread::id, std::condition_variable*> front()
    {
        std::lock_guard<std::mutex> lock(mtx);
        if (queue.empty()) {
            throw std::runtime_error("Queue is empty");
        }
        return queue.front();
    }

    // Delete an element by its thread ID
    void deleteById(std::thread::id id)
    {
        std::lock_guard<std::mutex> lock(mtx);
        auto it = id_map.find(id);
        if (it == id_map.end()) {
            return;
        }
        queue.erase(it->second);
        id_map.erase(it);
    }

    int size()
    {
        std::lock_guard<std::mutex> lock(mtx);
        return queue.size();
    }

  private:
    // List to store the queue elements
    std::list<std::pair<std::thread::id, std::condition_variable*>> queue;
    // Unordered map to store the iterators to the list elements
    std::unordered_map<
      std::thread::id,
      std::list<std::pair<std::thread::id, std::condition_variable*>>::iterator>
      id_map;
    // Mutex to protect the data structures
    std::mutex mtx;
};

class MultiKeyLock
{
  public:
    std::set<std::string> tryAcquire(const std::set<std::string>& keys)
    {
        std::unique_lock<std::mutex> lock(mtx);
        std::set<std::string> acquiredKeys;

        std::thread::id thisId = std::this_thread::get_id();
        std::condition_variable cvLocal;

        for (const auto& key : keys) {
            // If the key is already locked, skip it
            if (lockedKeys.count(key) == 1) {
                continue;
            }
            // Otherwise, gain its lock.
            lockedKeys.insert(key);
            acquiredKeys.insert(key);
        }

        // If no key gains.
        if (acquiredKeys.empty()) {
            // Register this thread to Keys' waiting list.
            for (const auto& key : keys) {
                waitingThreads[key].push(thisId, &cvLocal);
            }
            // Inside predicate, lock is locked.
            cvLocal.wait(lock, [this, &keys] {
                for (const auto& key : keys) {
                    if (lockedKeys.count(key) == 0) {
                        return true;
                    }
                }
                return false;
            });

            // Try to get the lock again. Previous thread maybe release multiple
            // keys.
            for (const auto& key : keys) {
                waitingThreads[key].deleteById(thisId);
                if (lockedKeys.count(key) == 1) {
                    continue;
                }
                lockedKeys.insert(key);
                acquiredKeys.insert(key);
            }
        }

        return acquiredKeys;
    }

    void release(const std::set<std::string>& keys)
    {
        std::unique_lock<std::mutex> lock(mtx);

        for (const auto& key : keys) {
            lockedKeys.erase(key);
            if (waitingThreads[key].size() > 0) {
                auto idMtx = waitingThreads[key].front();
                // idMtx only has one element
                idMtx.second->notify_all();
            }
        }
    }

    void flush()
    {
        std::unique_lock<std::mutex> lock(mtx);

        if (!lockedKeys.empty()) {
            SPDLOG_ERROR("Cannot flush MultiKeyLock while keys are locked");
            throw std::runtime_error(
              "Flush MultiKeyLock while keys are locked");
        } else if (!waitingThreads.empty()) {
            SPDLOG_ERROR("Cannot flush MultiKeyLock while threads are waiting");
            throw std::runtime_error(
              "Flush MultiKeyLock while threads are waiting");
        }
        waitingThreads.clear();
        lockedKeys.clear();
    }

  private:
    std::mutex mtx;
    std::map<std::string, ConditionVariableQueue> waitingThreads;
    std::set<std::string> lockedKeys;
};
}