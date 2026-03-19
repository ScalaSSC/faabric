#pragma once

#include <faabric/util/logging.h>
#include <faabric/util/timing.h>

#include <chrono>
#include <deque>
#include <map>
#include <mutex>
#include <shared_mutex>
#include <string>

namespace faabric::scheduler {

using Clock = std::chrono::steady_clock;
using TimePoint = Clock::time_point;

struct InstanceStatsResult
{
    int executedCount = 0;
    std::map<std::string, int> sourceStats;
    int chainedCallCount = 0;
    std::map<std::string, int> chainedCallStats;
};

struct AverageAndCount
{
    int average = 0;
    int count = 0;
};

struct InstanceMetricsResult
{
    std::map<time_t, AverageAndCount> workerQueueTimeStats;
    std::map<time_t, AverageAndCount> workerQueueNumStats;
    std::map<time_t, AverageAndCount> workerExecTimeStats;
};

class InstanceStats
{
  public:
    InstanceStats(std::string instanceName)
      : instanceName(instanceName)
    {}
    // Instance name (e.g., "user_function_0")
    const std::string instanceName;
    // Change these maps to store a deque of events per stat key.
    std::map<std::string, std::map<time_t, int>> sourceStats;
    std::map<std::string, std::map<time_t, int>> chainedCallStats;
    // Add more stats as needed.
    // Worker queuing time, stats pair <average,count>.
    std::map<time_t, AverageAndCount> workerQueueTimeStats;
    std::map<time_t, AverageAndCount> workerQueueNumStats;
    std::map<time_t, AverageAndCount> workerExecTimeStats;
};

class InstancesRuntimeStats
{
  private:
    mutable std::shared_mutex statsMx;

    int statsTimeout = 1000;                 // Unit: seconds
    std::map<time_t, int> versionTimestamps; // version -> timestamp
    std::map<std::string, InstanceStats> instanceStatsMap;

    InstanceStatsResult computeInstanceStats(InstanceStats& stats,
                                             const TimePoint now)
    {
        InstanceStatsResult result{};

        // 1. Calculate the cutoff time.
        auto cutoff = now - std::chrono::seconds(statsTimeout);
        auto cutoffSeconds =
          std::chrono::time_point_cast<std::chrono::seconds>(cutoff);
        time_t cutoffT = cutoffSeconds.time_since_epoch().count();

        // Process sourceStats.
        for (auto const& [source, events] : stats.sourceStats) {
            int sum = 0;
            for (auto const& [timestamp, count] : events) {
                if (timestamp >= cutoffT) {
                    sum += count;
                }
            }
            if (sum > 0) {
                result.sourceStats[source] = sum;
                result.executedCount += sum;
            }
        }

        // Process chainedCallStats similarly.
        for (auto const& [dest, events] : stats.chainedCallStats) {
            int sum = 0;
            for (auto const& [timestamp, count] : events) {
                if (timestamp >= cutoffT) {
                    sum += count;
                }
            }
            if (sum > 0) {
                result.chainedCallStats[dest] = sum;
                result.chainedCallCount += sum;
            }
        }

        return result;
    }

    void updateRollingAverage(std::map<time_t, AverageAndCount>& timeMap,
                              time_t currentTimeT,
                              int newValue)
    {
        // 1. Update the Rolling Average for the current second
        auto& [currentAvg, currentCount] = timeMap[currentTimeT];
        long long currentTotal =
          static_cast<long long>(currentAvg) * currentCount;
        currentTotal += newValue;
        currentCount++;

        currentAvg = static_cast<int>(currentTotal / currentCount);

        // 2. Prune old entries
        time_t cutoffT = currentTimeT - statsTimeout;
        while (!timeMap.empty() && timeMap.begin()->first < cutoffT) {
            timeMap.erase(timeMap.begin());
        }
    }

  public:
    void versionUpdate(int version)
    {
        std::unique_lock lock(statsMx);
        time_t currentTimeT = faabric::util::getEpochSeconds();
        versionTimestamps[currentTimeT] = version;
    }

    void instanceAdd(const std::string& instanceName,
                     const std::string& source,
                     int count)
    {
        std::unique_lock lock(statsMx);

        // Get current time truncated to seconds.
        time_t currentTimeT = faabric::util::getEpochSeconds();

        // Get or create the main stats object for the instance.
        auto& stats = instanceStatsMap.try_emplace(instanceName, instanceName)
                        .first->second;

        // Get the specific map for the source.
        auto& eventMap = stats.sourceStats[source];

        // --- Core Optimization ---
        // 1. Aggregate: Add the count to the current second's entry.
        eventMap[currentTimeT] += count;

        // 2. Prune: Efficiently remove outdated seconds from the map.
        time_t cutoffT = currentTimeT - statsTimeout;
        while (!eventMap.empty() && eventMap.begin()->first < cutoffT) {
            eventMap.erase(eventMap.begin());
        }
    }

    // Record a chained call event.
    void instanceGenerate(const std::string& instanceName,
                          const std::string& dest,
                          int count)
    {
        std::unique_lock lock(statsMx);

        time_t currentTimeT = faabric::util::getEpochSeconds();

        auto& stats = instanceStatsMap.try_emplace(instanceName, instanceName)
                        .first->second;

        auto& eventMap = stats.chainedCallStats[dest];

        // --- Core Optimization ---
        eventMap[currentTimeT] += count;

        time_t cutoffT = currentTimeT - statsTimeout;
        while (!eventMap.empty() && eventMap.begin()->first < cutoffT) {
            eventMap.erase(eventMap.begin());
        }
    }

    std::map<time_t, int> getVersionTimestamps()
    {
        std::shared_lock lock(statsMx);
        return versionTimestamps;
    }

    // getAllStats iterates over the map only once while holding lock.
    std::map<std::string, InstanceStatsResult> getAllStats()
    {
        std::unique_lock lock(statsMx);
        std::map<std::string, InstanceStatsResult> allStats;
        auto now = std::chrono::steady_clock::now();
        for (auto& [instanceName, stats] : instanceStatsMap) {
            allStats[instanceName] = computeInstanceStats(stats, now);
        }
        return allStats;
    }

    // 1. Record a worker queue time event
    void instanceWorkerQueueTime(const std::string& instanceName, int queueTime)
    {
        std::unique_lock lock(statsMx);
        time_t currentTimeT = faabric::util::getEpochSeconds();
        auto& stats = instanceStatsMap.try_emplace(instanceName, instanceName)
                        .first->second;

        updateRollingAverage(
          stats.workerQueueTimeStats, currentTimeT, queueTime);
    }

    // 2. Record a worker queue number event
    void instanceWorkerQueueNum(const std::string& instanceName, int queueNum)
    {
        std::unique_lock lock(statsMx);
        time_t currentTimeT = faabric::util::getEpochSeconds();
        auto& stats = instanceStatsMap.try_emplace(instanceName, instanceName)
                        .first->second;

        updateRollingAverage(stats.workerQueueNumStats, currentTimeT, queueNum);
    }

    // 3. Record a worker execute time event
    void instanceWorkerExecTime(const std::string& instanceName, int execTime)
    {
        std::unique_lock lock(statsMx);
        time_t currentTimeT = faabric::util::getEpochSeconds();
        auto& stats = instanceStatsMap.try_emplace(instanceName, instanceName)
                        .first->second;

        updateRollingAverage(stats.workerExecTimeStats, currentTimeT, execTime);
    }

    std::map<std::string, InstanceMetricsResult> getWorkerMetrics(
      bool isRuntime = false)
    {
        // Use shared_lock for read-only thread safety
        std::shared_lock<std::shared_mutex> lock(statsMx);
        std::map<std::string, InstanceMetricsResult> metrics;

        if (!isRuntime) {
            for (const auto& [instanceName, stats] : instanceStatsMap) {
                metrics[instanceName] =
                  InstanceMetricsResult{ stats.workerQueueTimeStats,
                                       stats.workerQueueNumStats,
                                       stats.workerExecTimeStats };
            }
            return metrics;
        }
        time_t lastSecondT = faabric::util::getEpochSeconds() - 1;

        for (const auto& [instanceName, stats] : instanceStatsMap) {
            InstanceMetricsResult lastSecondResult;

            auto qtIt = stats.workerQueueTimeStats.find(lastSecondT);
            if (qtIt != stats.workerQueueTimeStats.end()) {
                lastSecondResult.workerQueueTimeStats[lastSecondT] =
                  qtIt->second;
            }

            auto qnIt = stats.workerQueueNumStats.find(lastSecondT);
            if (qnIt != stats.workerQueueNumStats.end()) {
                lastSecondResult.workerQueueNumStats[lastSecondT] =
                  qnIt->second;
            }

            auto etIt = stats.workerExecTimeStats.find(lastSecondT);
            if (etIt != stats.workerExecTimeStats.end()) {
                lastSecondResult.workerExecTimeStats[lastSecondT] =
                  etIt->second;
            }

            metrics[instanceName] = std::move(lastSecondResult);
        }
        return metrics;
    }

    void reset()
    {
        std::unique_lock lock(statsMx);
        versionTimestamps.clear();
        instanceStatsMap.clear();
    }
};
} // namespace faabric::scheduler