#pragma once

#include <faabric/util/locks.h>

#include <deque>
#include <memory>
#include <shared_mutex>
#include <string>
#include <unordered_map>
#include <vector>

namespace faabric::scheduler {

// InstanceLoad tracks wait times for a single instance.
// It stores only the latest maxSamples samples and maintains a running sum for
// computing the average.
class InstanceLoad
{
  public:
    explicit InstanceLoad(size_t maxSamplesIn);
    void addWaitTime(int waitTime, int queuedMsgNum);
    double getAverage() const;

  private:
    size_t maxSamples;
    std::deque<double> waitTimes;
    double runningSum = 0.0;
    mutable std::shared_mutex mutex;
};

// InstancesLoadState is used by scheduler to statitics the workload locally.
// InstancesLoadState manages statistics for multiple instances.
// Instead of storing InstanceLoad directly (which is non-movable),
// we use std::unique_ptr to store each InstanceLoad.
class InstancesLoadState
{
  public:
    explicit InstancesLoadState(size_t maxSamplesIn);

    void addWaitTime(const std::string& instanceName,
                     int waitTime,
                     int queuedMsgNum);

    double getAverage(const std::string& instanceName) const;

    std::map<std::string, double> getAllAverages() const;

  private:
    size_t maxSamples;
    mutable std::shared_mutex stateMx;
    std::unordered_map<std::string, std::unique_ptr<InstanceLoad>> instances;
};

} // namespace faabric::scheduler
