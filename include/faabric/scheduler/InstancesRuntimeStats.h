#pragma once

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
    const std::string instanceName;
    // Change these maps to store a deque of events per stat key.
    std::map<std::string, std::map<time_t, int>> sourceStats;
    std::map<std::string, std::map<time_t, int>> chainedCallStats;
};

class InstancesRuntimeStats
{
  private:
    mutable std::shared_mutex statsMx;

    int statsTimeout = 10; // seconds
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
        auto now = Clock::now();
        auto nowSeconds =
          std::chrono::time_point_cast<std::chrono::seconds>(now);
        time_t currentTimeT = nowSeconds.time_since_epoch().count();

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

        auto now = Clock::now();
        auto nowSeconds =
          std::chrono::time_point_cast<std::chrono::seconds>(now);
        time_t currentTimeT = nowSeconds.time_since_epoch().count();

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

    // Refactored getAllStats iterates over the map only once while holding the
    // lock.
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
};
} // namespace faabric::scheduler