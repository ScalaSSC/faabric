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

struct TimedEntry
{
    TimePoint timestamp;
    int count;
};

class InstanceStats
{
  public:
    InstanceStats(std::string instanceName)
      : instanceName(instanceName)
    {}
    const std::string instanceName;
    // Change these maps to store a deque of events per stat key.
    std::map<std::string, std::deque<TimedEntry>> sourceStats;
    std::map<std::string, std::deque<TimedEntry>> chainedCallStats;
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
        auto cutoff = now - std::chrono::seconds(statsTimeout);

        // Process sourceStats.
        for (auto& [source, events] : stats.sourceStats) {
            // Prune outdated events.
            while (!events.empty() && events.front().timestamp < cutoff) {
                events.pop_front();
            }
            // Sum up remaining events.
            int sum = 0;
            for (const auto& entry : events) {
                sum += entry.count;
            }
            result.sourceStats[source] = sum;
            result.executedCount += sum;
        }

        // Process chainedCallStats similarly.
        for (auto& [dest, events] : stats.chainedCallStats) {
            while (!events.empty() && events.front().timestamp < cutoff) {
                events.pop_front();
            }
            int sum = 0;
            for (const auto& entry : events) {
                sum += entry.count;
            }
            result.chainedCallStats[dest] = sum;
            result.chainedCallCount += sum;
        }

        return result;
    }

  public:
    void instanceAdd(const std::string& instanceName,
                     const std::string& source,
                     int count)
    {
        std::unique_lock lock(statsMx);
        auto now = std::chrono::steady_clock::now();
        auto cutoff = now - std::chrono::seconds(statsTimeout);

        // Try to emplace a new InstanceStats if it doesn't already exist.
        auto [it, inserted] =
          instanceStatsMap.emplace(instanceName, InstanceStats(instanceName));
        InstanceStats& stats = it->second;

        // Prune outdated events before adding the new one.
        auto& events = stats.sourceStats[source];
        while (!events.empty() && events.front().timestamp < cutoff) {
            events.pop_front();
        }

        events.push_back({ now, count });
    }

    // Record a chained call event.
    void instanceGenerate(const std::string& instanceName,
                          const std::string& dest,
                          int count)
    {
        std::unique_lock lock(statsMx);
        auto now = std::chrono::steady_clock::now();
        auto cutoff = now - std::chrono::seconds(statsTimeout);

        // Try to emplace a new InstanceStats if it doesn't already exist.
        auto [it, inserted] =
          instanceStatsMap.emplace(instanceName, InstanceStats(instanceName));
        InstanceStats& stats = it->second;

        // Prune outdated events before adding the new one.
        auto& events = stats.chainedCallStats[dest];
        while (!events.empty() && events.front().timestamp < cutoff) {
            events.pop_front();
        }

        events.push_back({ now, count });
    }

    InstanceStatsResult getInstanceStats(const std::string& instanceName)
    {
        std::unique_lock lock(statsMx);
        auto it = instanceStatsMap.find(instanceName);
        if (it == instanceStatsMap.end()) {
            return InstanceStatsResult{};
        }
        auto now = std::chrono::steady_clock::now();
        return computeInstanceStats(it->second, now);
    }

    // Refactored getAllStats iterates over the map only once while holding the
    // lock.
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
};
} // namespace faabric::scheduler