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
    // Worker queuing time.
    using TimeStatPair = std::pair<int, int>;
    std::map<time_t, TimeStatPair> workerQueueTimeStats;
};

class InstancesRuntimeStats
{
  private:
    mutable std::shared_mutex statsMx;

    int statsTimeout = 10;
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

  public:
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

    // getAllStats iterates over the map only once while holding lock.
    std::map<std::string, InstanceStatsResult> getAllStats()
    {
        std::unique_lock lock(statsMx);
        std::map<std::string, InstanceStatsResult> allStats;
        auto now = std::chrono::steady_clock::now();
        for (auto& [instanceName, stats] : instanceStatsMap) {
            // Pass 'now' to the compute function.
            allStats[instanceName] = computeInstanceStats(stats, now);
        }
        return allStats;
    }

    // Record a worker queue time event
    void instanceWorkerQueueTime(const std::string& instanceName, int queueTime)
    {
        std::unique_lock lock(statsMx);

        time_t currentTimeT = faabric::util::getEpochSeconds();

        // Get the specific stats map
        auto& stats = instanceStatsMap.try_emplace(instanceName, instanceName)
                        .first->second;
        auto& timeMap = stats.workerQueueTimeStats;

        // Update the Rolling Average for the current second
        auto& [currentAvg, currentCount] = timeMap[currentTimeT];
        long long currentTotal =
          static_cast<long long>(currentAvg) * currentCount;
        currentTotal += queueTime;
        currentCount++;

        currentAvg = static_cast<int>(currentTotal / currentCount);

        // Prune old entries
        time_t cutoffT = currentTimeT - statsTimeout;
        while (!timeMap.empty() && timeMap.begin()->first < cutoffT) {
            timeMap.erase(timeMap.begin());
        }
    }

    std::map<std::string, std::pair<int, int>> getAverageQueuingTimes()
    {
        std::shared_lock<std::shared_mutex> lock(statsMx);

        std::map<std::string, std::pair<int, int>> latestTimes;

        for (const auto& [instanceName, stats] : instanceStatsMap) {
            if (!stats.workerQueueTimeStats.empty()) {
                auto it = stats.workerQueueTimeStats.rbegin();
                latestTimes[instanceName] = it->second;
            }
        }
        return latestTimes;
    }

    void logAverageQueuingTimes()
    {
        auto latestStats = getAverageQueuingTimes();

        if (latestStats.empty()) {
            SPDLOG_DEBUG("No instance queuing stats available.");
            return;
        }

        fmt::memory_buffer buf;
        fmt::format_to(std::back_inserter(buf),
                       "Current Instance Queuing Stats:");

        bool hasData = false;
        for (const auto& [instanceName, stats] : latestStats) {
            const auto& [avgTime, count] = stats;

            if (count > 0) {
                fmt::format_to(std::back_inserter(buf),
                               "\n  - {}: {}us (n={})",
                               instanceName,
                               avgTime,
                               count);
                hasData = true;
            }
        }

        if (hasData) {
            SPDLOG_DEBUG(fmt::to_string(buf));
        }
    }
};
} // namespace faabric::scheduler