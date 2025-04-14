#include <faabric/scheduler/InstancesLoadState.h>

namespace faabric::scheduler {

InstanceLoad::InstanceLoad(size_t maxSamples)
  : maxSamples(maxSamples)
{}

void InstanceLoad::addWaitTime(int waitTime, int queuedMsgNum)
{
    if (queuedMsgNum < 0) {
        SPDLOG_ERROR("Invalid queued message number: {}", queuedMsgNum);
        throw std::invalid_argument("Invalid queued message number");
    }
    // Increment queuedMsgNum to account for the enqueued message, ensuring it's
    // never zero.
    queuedMsgNum++;
    double avgWaitTime = static_cast<double>(waitTime) / queuedMsgNum;

    faabric::util::FullLock lock(mutex);
    waitTimes.push_back(avgWaitTime);
    runningSum += avgWaitTime;

    if (waitTimes.size() > maxSamples) {
        runningSum -= waitTimes.front();
        waitTimes.pop_front();
    }
}

double InstanceLoad::getAverage() const
{
    faabric::util::SharedLock lock(mutex);
    if (waitTimes.empty()) {
        return 0.0;
    }
    return runningSum / waitTimes.size();
}

InstancesLoadState::InstancesLoadState(size_t maxSamplesIn)
  : maxSamples(maxSamplesIn)
{}

void InstancesLoadState::addWaitTime(const std::string& instanceName,
                                     int waitTime,
                                     int queuedMsgNum)
{
    faabric::util::FullLock lock(stateMx);

    // SPDLOG_DEBUG(
    //   "Adding wait time {} for instance {} with queued message number {}",
    //   waitTime,
    //   instanceName,
    //   queuedMsgNum);

    // Find the instance in the map
    auto it = instances.find(instanceName);
    if (it == instances.end()) {
        // Create a new InstanceLoad for this instance inside a unique_ptr.
        auto newInstance = std::make_unique<InstanceLoad>(maxSamples);
        // Insert the instance.
        it = instances.emplace(instanceName, std::move(newInstance)).first;
    }
    it->second->addWaitTime(waitTime, queuedMsgNum);
}

double InstancesLoadState::getAverage(const std::string& instanceName) const
{
    faabric::util::SharedLock lock(stateMx);
    auto it = instances.find(instanceName);
    if (it == instances.end()) {
        return 0.0;
    }
    return it->second->getAverage();
}

std::map<std::string, double> InstancesLoadState::getAllAverages() const
{
    std::map<std::string, double> averages;

    faabric::util::SharedLock lock(stateMx);
    for (const auto& instancePair : instances) {
        averages[instancePair.first] = instancePair.second->getAverage();
    }
    return averages;
}

} // namespace faabric::scheduler
