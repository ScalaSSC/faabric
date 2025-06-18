#pragma once

#include <faabric/util/config.h>
#include <faabric/util/locks.h>

#include <deque>
#include <map>
#include <shared_mutex>
#include <string>

namespace faabric::batch_scheduler {

class DispatchTimeStats
{
  public:
    DispatchTimeStats(size_t maxSamples = 100)
      : maxSamples(maxSamples)
      , totalMicros(0)
    {}

    void record(int micros)
    {
        samples.push_back(micros);
        totalMicros += micros;

        if (samples.size() > maxSamples) {
            totalMicros -= samples.front();
            samples.pop_front();
        }
    }

    double getAverage() const
    {
        if (samples.empty()) {
            return 0.0;
        }
        return static_cast<double>(totalMicros) / samples.size();
    }

  private:
    std::deque<int> samples;
    size_t maxSamples;
    int totalMicros;
};

class WorkerLoad
{
  public:
    WorkerLoad(std::string hostIn)
      : host(hostIn)
    {}

    void updateInstancesWaitTime(std::map<std::string, int> instancesLoad)
    {
        instancesWaitTime = std::move(instancesLoad);
    }

  private:
    const std::string host;
    std::map<std::string, int> instancesWaitTime;
};

class WorkersLoadState
{
  public:
    WorkersLoadState() {}

    void updateState(std::string host,
                     std::map<std::string, int> instancesLoad,
                     int transferTime)
    {
        std::unique_lock lock(stateMx);

        if (host == localHost) {
            transferTime = 0;
        }

        // Update or insert instanceWorkerLoad
        for (auto& [instanceName, instanceLoad] : instancesLoad) {
            instanceWorkerLoad[instanceName][host] = instanceLoad;
        }

        // Update or insert WorkerLoad
        auto workerLoadIt = workersLoad.find(host);
        if (workerLoadIt == workersLoad.end()) {
            workerLoadIt = workersLoad.emplace(host, WorkerLoad(host)).first;
        }

        // Update the WorkerLoad data
        workerLoadIt->second.updateInstancesWaitTime(std::move(instancesLoad));
        // Record dispatch time statistics
        auto& dispatchStats = dispatchTimes[host];
        dispatchStats.record(transferTime);

        if (host != localHost) {
            auto& remoteDispatchStats = dispatchTimes["remote"];
            remoteDispatchStats.record(transferTime);
        }

        SPDLOG_DEBUG("Updated worker load for host {} with transfer time {}",
                     host,
                     dispatchStats.getAverage());
    }

    // <WORKER IP, PAIR<LOAD, TRANSFER TIME>>
    std::map<std::string, std::pair<int, int>> getWorkerStats(
      std::string instanceName)
    {
        // Acquire shared lock for safe read access.
        std::shared_lock lock(stateMx);

        std::map<std::string, std::pair<int, int>> result;

        // Find the mapping for the given instance name.
        for (auto& [instance, workerLoad] : instanceWorkerLoad) {
            SPDLOG_DEBUG(
              "Instance: {}, Worker Load: {}", instance, workerLoad.size());
            for ([[maybe_unused]]auto& [workerIP, load] : workerLoad) {
                SPDLOG_DEBUG("Worker IP: {}, Load: {}", workerIP, load);
            }
        }

        auto it = instanceWorkerLoad.find(instanceName);
        if (it != instanceWorkerLoad.end()) {
            // The inner map holds worker IP -> load
            const auto& workerLoads = it->second;
            for (const auto& [workerIP, load] : workerLoads) {
                int avgTransferTime = 0;
                // Lookup dispatch stats for the given worker IP.
                auto dtIt = dispatchTimes.find(workerIP);
                if (dtIt != dispatchTimes.end()) {
                    avgTransferTime =
                      static_cast<int>(dtIt->second.getAverage());
                } else {
                    // Fallback to "remote" dispatch statistics.
                    avgTransferTime =
                      static_cast<int>(dispatchTimes["remote"].getAverage());
                }
                result[workerIP] = { load, avgTransferTime };
            }
        }
        return result;
    }

  private:
    std::shared_mutex stateMx;

    const std::string localHost = faabric::util::getSystemConfig().endpointHost;

    std::map<std::string, WorkerLoad> workersLoad;
    std::map<std::string, DispatchTimeStats> dispatchTimes;

    // MAP<InstanceName, <Host, Load>>
    std::map<std::string, std::map<std::string, int>> instanceWorkerLoad;
};

}