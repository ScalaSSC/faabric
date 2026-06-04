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

struct InstanceSecondStats
{
    AverageAndCount queueTime;
    AverageAndCount queueNum;
    AverageAndCount execTime;
    int throughput  = 0;
    int inputCount  = 0; // requests arriving at this instance per second
};

struct InstanceMetricsResult
{
    std::map<time_t, AverageAndCount> workerQueueTimeStats;
    std::map<time_t, AverageAndCount> workerQueueNumStats;
    std::map<time_t, AverageAndCount> workerExecTimeStats;
    std::map<time_t, int> throughputStats;
    std::map<time_t, int> inputCountStats; // arrivals per second
    // Global chained call history: timestamp -> (destHost -> count)
    std::map<time_t, std::map<std::string, int>> chainedCallHistory;
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
    // Worker queuing time, stats pair <average,count>.
    std::map<time_t, InstanceSecondStats> metrics;
};

class InstancesRuntimeStats
{
  private:
    mutable std::shared_mutex statsMx;

    int statsTimeout = 1000;                 // Unit: seconds
    std::map<time_t, int> versionTimestamps; // version -> timestamp
    std::map<std::string, InstanceStats> instanceStatsMap;

    // The statistics of chained request calls.
    // MAP<timestamp, MAP<destination worker IP, count>>
    std::map<time_t, std::map<std::string, int>> chainedCallHistory;

    // Worker-level outgoing chained calls recorded by the Scheduler only.
    // Not per-instance; tracks total calls sent to each dest host per second.
    // MAP<timestamp, MAP<destHost, count>>
    std::map<time_t, std::map<std::string, int>> workerChainHistory;

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

        // Track total arrivals per second for throughput/input ratio.
        stats.metrics[currentTimeT].inputCount += count;
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

        // Also update global chained call history.
        if (!dest.empty()) {
            chainedCallHistory[currentTimeT][dest] += count;
            while (!chainedCallHistory.empty() &&
                   chainedCallHistory.begin()->first < cutoffT) {
                chainedCallHistory.erase(chainedCallHistory.begin());
            }
        }
    }

    // Record outgoing chained calls at worker level (Scheduler-only, not
    // Planner). Writes to workerChainHistory keyed by destHost and timestamp.
    void recordWorkerOutgoingChain(const std::string& destHost, int count)
    {
        std::unique_lock lock(statsMx);
        time_t currentTimeT = faabric::util::getEpochSeconds();

        workerChainHistory[currentTimeT][destHost] += count;

        time_t cutoffT = currentTimeT - statsTimeout;
        while (!workerChainHistory.empty() &&
               workerChainHistory.begin()->first < cutoffT) {
            workerChainHistory.erase(workerChainHistory.begin());
        }
    }

    // Returns the last second's worker-level outgoing chain snapshot.
    std::map<std::string, int> getLastSecWorkerChain() const
    {
        std::shared_lock lock(statsMx);
        time_t lastSec = faabric::util::getEpochSeconds() - 1;
        auto it = workerChainHistory.find(lastSec);
        if (it != workerChainHistory.end()) {
            return it->second;
        }
        return {};
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

    void updateInstanceExecutionMetrics(const std::string& instanceName,
                                        int qTime,
                                        int qNum,
                                        int eTime,
                                        int processedCount = 1)
    {
        std::unique_lock<std::shared_mutex> lock(statsMx);
        time_t now = faabric::util::getEpochSeconds();

        auto& stats = instanceStatsMap.try_emplace(instanceName, instanceName)
                        .first->second;

        auto& secondData = stats.metrics[now];

        auto updateRolling = [](AverageAndCount& target, int newValue) {
            long long total =
              static_cast<long long>(target.average) * target.count;
            total += newValue;
            target.count++;
            target.average = static_cast<int>(total / target.count);
        };

        updateRolling(secondData.queueTime, qTime);
        updateRolling(secondData.queueNum, qNum);
        updateRolling(secondData.execTime, eTime);

        secondData.throughput += processedCount;

        time_t cutoff = now - statsTimeout;
        while (!stats.metrics.empty() &&
               stats.metrics.begin()->first < cutoff) {
            stats.metrics.erase(stats.metrics.begin());
        }
    }

    void recordChainedCall(const std::map<std::string, int>& hostCountMap)
    {
        if (hostCountMap.empty()) {
            return;
        }

        std::unique_lock<std::shared_mutex> lock(statsMx);
        time_t currentTimeT = faabric::util::getEpochSeconds();

        auto& currentSecondMap = chainedCallHistory[currentTimeT];
        for (const auto& [destHost, count] : hostCountMap) {
            if (destHost.empty()) {
                SPDLOG_WARN("Invalid destHost in chained call stats: '{}'",
                            destHost);
                continue;
            }
            currentSecondMap[destHost] += count;
        }

        time_t cutoffT = currentTimeT - statsTimeout;
        while (!chainedCallHistory.empty() &&
               chainedCallHistory.begin()->first < cutoffT) {
            chainedCallHistory.erase(chainedCallHistory.begin());
        }
    }

    std::map<std::string, int> getGlobalChainedTraffic(int secondsLookback = 1)
    {
        std::shared_lock<std::shared_mutex> lock(statsMx);
        std::map<std::string, int> aggregatedStats;

        time_t nowT = faabric::util::getEpochSeconds();
        time_t endT = nowT - 1;
        time_t startT = nowT - secondsLookback;

        if (startT > endT) {
            return aggregatedStats;
        }

        for (auto it = chainedCallHistory.lower_bound(startT);
             it != chainedCallHistory.end();
             ++it) {
            if (it->first >= nowT) {
                break;
            }
            for (const auto& [host, count] : it->second) {
                aggregatedStats[host] += count;
            }
        }

        return aggregatedStats;
    }

    std::map<std::string, InstanceSecondStats> getLastSecondStats()
    {
        std::shared_lock<std::shared_mutex> lock(statsMx);
        std::map<std::string, InstanceSecondStats> result;
        time_t lastSecondT = faabric::util::getEpochSeconds() - 1;
        for (const auto& [instanceName, stats] : instanceStatsMap) {
            auto it = stats.metrics.find(lastSecondT);
            if (it != stats.metrics.end()) {
                result[instanceName] = it->second;
            }
        }
        return result;
    }

    std::map<std::string, InstanceMetricsResult> getWorkerMetrics(
      bool isRuntime = false)
    {
        // Use shared_lock for read-only thread safety
        std::shared_lock<std::shared_mutex> lock(statsMx);
        std::map<std::string, InstanceMetricsResult> metrics;

        // Note: chained-call data is NOT recorded per-instance here. It is
        // worker-level and travels via WorkerStats.workerChainHistory (proto
        // field 10), populated from getLastSecWorkerChain().

        if (!isRuntime) {
            for (const auto& [instanceName, stats] : instanceStatsMap) {
                InstanceMetricsResult currentMetrics;
                for (const auto& [timestamp, secondStats] : stats.metrics) {
                    currentMetrics.workerQueueTimeStats[timestamp] =
                      secondStats.queueTime;
                    currentMetrics.workerQueueNumStats[timestamp] =
                      secondStats.queueNum;
                    currentMetrics.workerExecTimeStats[timestamp] =
                      secondStats.execTime;
                    currentMetrics.throughputStats[timestamp] =
                      secondStats.throughput;
                    currentMetrics.inputCountStats[timestamp] =
                      secondStats.inputCount;
                }
                metrics[instanceName] = std::move(currentMetrics);
            }
            return metrics;
        }
        time_t lastSecondT = faabric::util::getEpochSeconds() - 1;

        for (const auto& [instanceName, stats] : instanceStatsMap) {
            InstanceMetricsResult lastSecondResult;

            auto it = stats.metrics.find(lastSecondT);
            if (it != stats.metrics.end()) {
                const auto& secondData = it->second;

                lastSecondResult.workerQueueTimeStats[lastSecondT] =
                  secondData.queueTime;
                lastSecondResult.workerQueueNumStats[lastSecondT] =
                  secondData.queueNum;
                lastSecondResult.workerExecTimeStats[lastSecondT] =
                  secondData.execTime;
                lastSecondResult.throughputStats[lastSecondT] =
                  secondData.throughput;
                lastSecondResult.inputCountStats[lastSecondT] =
                  secondData.inputCount;
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
        chainedCallHistory.clear();
        workerChainHistory.clear();
    }
};
} // namespace faabric::scheduler