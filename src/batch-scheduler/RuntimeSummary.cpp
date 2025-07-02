#include <faabric/batch-scheduler/RuntimeSummary.h>

namespace faabric::batch_scheduler {

void WindowedRecord::setWindow(std::map<std::string, double>& expDist,
                               std::map<std::string, double>& srcDist,
                               bool isBody)
{
    windowSize = 0;
    workers.clear();

    if (isBody) {
        windowQuota = getBodyWindowedSlots(expDist, srcDist);
    } else {
        // If this operator is the first one int the group, we assign them
        // into workers according to the expected distribution.
        for (const auto& [host, share] : expDist) {
            int hostQuota = static_cast<int>(std::ceil(share * RING_SIZE));
            windowQuota[host] = hostQuota;
        }
    }

    windowPos = 0;
    for (const auto& [host, quota] : windowQuota) {
        workers.push_back(host);
        windowSize += quota;
    }

    if (workers.empty()) {
        SPDLOG_WARN("No workers found for windowed record");
        throw std::runtime_error("No workers found for windowed record");
    }
    assignedCount.clear();
}

void WindowedRecord::updateWindow(std::map<std::string, double>& expDist,
                                  std::map<std::string, double>& srcDist)
{
    windowSize = 0;
    workers.clear();

    windowQuota = getBodyWindowedSlots(expDist, srcDist);

    windowPos = 0;
    for (const auto& [host, quota] : windowQuota) {
        workers.push_back(host);
        windowSize += quota;
    }

    if (workers.empty()) {
        SPDLOG_WARN("No workers found for windowed record");
        throw std::runtime_error("No workers found for windowed record");
    }
    assignedCount.clear();
}

std::map<std::string, int> WindowedRecord::getBodyWindowedSlots(
  std::map<std::string, double>& expDist,
  std::map<std::string, double>& srcDist)
{
    // 1. Sanity check: expected and source distributions must exist.
    if (!expDist.contains(localHost) || !srcDist.contains(localHost)) {
        SPDLOG_WARN("Localhost '{}' not found in distribution", localHost);
        throw std::runtime_error("Local host missing in distribution");
    }

    std::map<std::string, int> hostsSlots;
    double localShare = expDist.at(localHost) / srcDist.at(localHost);
    int localSlots = static_cast<int>(std::round(localShare * RING_SIZE));
    hostsSlots[localHost] = localSlots;

    // 2. If generated request on local host is less than expected, we
    // assign all requests locally.
    if (localSlots >= RING_SIZE)
        return hostsSlots;

    // 3. therwise, we have to assign the remaining requests to other
    // hosts according to round-robin.
    int remaining = RING_SIZE - localSlots;
    std::map<std::string, double> gaps;
    double totalGap = 0.0;

    for (const auto& [host, expShare] : expDist) {
        if (host == localHost)
            continue;
        double srcShare = srcDist.count(host) ? srcDist.at(host) : 0.0;
        double gap = expShare - srcShare;
        if (gap > 0.0) {
            totalGap += gap;
            gaps.emplace(host, gap);
        }
    }

    // 5. Allocate the remaining slots.
    for (auto& [host, gap] : gaps) {
        double portion = gap / totalGap;
        int slot = static_cast<std::size_t>(std::ceil(portion * remaining));
        hostsSlots[host] = slot;
    }

    return hostsSlots;
}

std::string WindowedRecord::schedule(const unsigned int counter)
{
    faabric::util::FullLock lock(wrMx);
    const std::string& rec = workers[counter % workers.size()];
    return doSchedule(rec);
}

std::string WindowedRecord::schedule(const std::string& recommended)
{
    faabric::util::FullLock lock(wrMx);
    return doSchedule(recommended);
}

std::string WindowedRecord::doSchedule(const std::string& initialHost)
{

    // 1) Reset at window boundary
    if (windowPos == windowSize) {
        assignedCount.clear();
        windowPos = 0;
    }

    std::string pickHost = initialHost;
    if (assignedCount[initialHost] >= windowQuota[initialHost]) {
        for (auto& [host, count] : windowQuota) {
            if (assignedCount[host] < windowQuota[host]) {
                pickHost = host;
                break;
            }
        }
    }

    assignedCount[pickHost]++;
    windowPos++;
    return pickHost;
}

void RuntimeSummary::initScheduledOperators(
  const batch_scheduler::Application& application,
  const std::map<std::string, ScheduledOperator>& scheduledOperatorsMapIn,
  bool planner)
{
    faabric::util::FullLock lock(summaryMx);
    scheduledOperatorsMap = scheduledOperatorsMapIn;

    SPDLOG_DEBUG("Initializing scheduled operators distribution ring");
    // Update the expected distribution and source distribution.
    expectedDist.clear();
    sourceDist.clear();

    isPlanner = planner;
    initAll(application, planner);
}

void RuntimeSummary::initAll(const batch_scheduler::Application& application,
                             bool planner)
{
    std::set<std::string> initOperators;
    // For planner, we need to initialize the schedule for input opeartors.
    if (planner) {
        for (const auto& [optName, scheduledOpt] : scheduledOperatorsMap) {
            if (scheduledOpt.node.isInput) {
                initOperators.insert(optName);
            }
        }
    }
    // Otherwise, we just initialize the distribution ring for successor
    // operators (whose source is in this node).
    else {
        for (const auto& [optName, scheduledOpt] : scheduledOperatorsMap) {
            // If the source node of optName is in this node, we need to
            // shcedule it.
            auto sourceNodes = application.getSource(optName);
            for (const auto& sourceNode : sourceNodes) {
                auto sourceOpt = getScheduledOperatorOrThrow(
                  scheduledOperatorsMap, sourceNode->name);
                if (sourceOpt.weightDist.contains(localHost)) {
                    initOperators.insert(optName);
                    break; // Only break the inner for loop
                }
            }
        }
    }
    for (const auto& optName : initOperators) {
        initOpertaor(application, optName);
    }
}

void RuntimeSummary::initOpertaor(
  const batch_scheduler::Application& application,
  std::string operatorName)
{
    SPDLOG_DEBUG("Initializing operator runtime summary for {}", operatorName);
    // Judge the local operator type;
    auto& scheduledOpt =
      getScheduledOperatorOrThrow(scheduledOperatorsMap, operatorName);
    bool isCollocate = scheduledOpt.isCollocate;
    std::string collocateWith = scheduledOpt.collocateWith;

    // Is the source operator local? If so, it is not the head. Otherwise it is.
    auto sourceNodes = application.getSource(operatorName);
    bool isBody = false;
    for (const auto& sourceNode : sourceNodes) {
        if (!scheduledOperatorsMap.contains(sourceNode->name)) {
            continue;
        }
        auto sourceOpt = scheduledOperatorsMap.at(sourceNode->name);
        if (sourceOpt.groupId != scheduledOpt.groupId)
            continue;
        if (!sourceOpt.weightDist.contains(localHost))
            continue;
        if (isCollocate && sourceOpt.isCollocate &&
            sourceOpt.collocateWith == collocateWith) {
            isBody = true;
            break;
        }
        if (!isCollocate) {
            isBody = true;
            break; // Only break the inner for loop
        }
    }
    if (isBody && isCollocate) {
        scheduledOpt.localType = LocalStatelessOperatorType::COLLOCATE_BODY;
    } else if (isBody && !isCollocate) {
        scheduledOpt.localType = LocalStatelessOperatorType::ROUNDROBIN_BODY;
    } else if (!isBody && isCollocate) {
        scheduledOpt.localType = LocalStatelessOperatorType::COLLOCATE_HEAD;
    } else if (!isBody && !isCollocate) {
        scheduledOpt.localType = LocalStatelessOperatorType::ROUNDROBIN_HEAD;
    }

    std::string instanceName = scheduledOpt.node.name + "_0";

    localScheduledOperatorsMap[instanceName] = scheduledOpt.localType;
    // Update the expected distribution and source distribution.

    doInitExpectedDist(scheduledOpt);
    auto expDist = expectedDist[instanceName];
    std::map<std::string, double> srcDist = std::map<std::string, double>();
    if (isBody) {
        doInitSourceDist(scheduledOpt, application);
        srcDist = sourceDist[instanceName];
    }
    // init the source windowed distribution.

    windowedRecords[instanceName] =
      std::make_shared<WindowedRecord>(expDist, srcDist, isBody);
}

void RuntimeSummary::doInitExpectedDist(ScheduledOperator& schedOp)
{
    std::string instanceName = schedOp.node.name + "_0";
    auto hostWeight = schedOp.weightDist;

    int sumWeight = 0;
    for (const auto& [host, weight] : hostWeight) {
        sumWeight += weight;
    }
    if (sumWeight <= 0) {
        std::string errorMsg =
          "Total weight <= 0 for operator " + schedOp.node.name;
        SPDLOG_WARN("{}", errorMsg);
        throw std::runtime_error(errorMsg);
    }
    auto& dist = expectedDist[instanceName];
    for (const auto& [host, weight] : hostWeight) {
        dist[host] = static_cast<double>(weight) / sumWeight;
    }
}

void RuntimeSummary::doInitSourceDist(
  ScheduledOperator& schedOp,
  const batch_scheduler::Application& application)
{
    std::string instanceName = schedOp.node.name + "_0";
    int sumWeight = 0;

    std::map<std::string, int> SourceWeight;
    for (const auto& sourceNode : application.getSource(schedOp.node.name)) {
        auto& sourceOpt =
          getScheduledOperatorOrThrow(scheduledOperatorsMap, sourceNode->name);
        for (const auto& [host, weight] : sourceOpt.weightDist) {
            SourceWeight[host] += weight;
            sumWeight += weight;
        }
    }
    if (sumWeight <= 0) {
        std::string errorMsg =
          "Total weight <= 0 for operator " + schedOp.node.name;
        SPDLOG_WARN("{}", errorMsg);
        throw std::runtime_error(errorMsg);
    }

    auto& dist = sourceDist[instanceName];
    for (const auto& [host, weight] : SourceWeight) {
        dist[host] = static_cast<double>(weight) / sumWeight;
    }
}

std::string RuntimeSummary::getHost(const std::string& instance,
                                    unsigned int counter)
{
    faabric::util::SharedLock lock(summaryMx);
    auto it = windowedRecords.find(instance);
    if (it == windowedRecords.end()) {
        SPDLOG_ERROR("No collocate map for instance {} found", instance);
        throw std::runtime_error("No collocate map for instance " + instance);
    }
    std::string host = it->second->schedule(counter);
    return host;
}

std::string RuntimeSummary::getHost(const std::string& instance,
                                    std::string recommended)
{
    faabric::util::SharedLock lock(summaryMx);
    auto it = windowedRecords.find(instance);
    if (it == windowedRecords.end()) {
        SPDLOG_ERROR("No collocate map for instance {} found", instance);
        throw std::runtime_error("No collocate map for instance " + instance);
    }
    std::string host = it->second->schedule(recommended);
    return host;
}

void RuntimeSummary::updateSourceDist(
  std::map<std::string, std::map<std::string, int>> sourceCountStats,
  bool reschedule)
{
    faabric::util::FullLock lock(summaryMx);
    sourceDist.clear();
    for (const auto& [instanceName, hostCount] : sourceCountStats) {
        int sumCount = 0;
        for (const auto& [host, count] : hostCount) {
            sumCount += count;
        }
        if (sumCount <= 0) {
            // SPDLOG_DEBUG("Total count <= 0 for instance {}", instanceName);
            continue;
        }
        auto& dist = sourceDist[instanceName];
        for (const auto& [host, count] : hostCount) {
            dist[host] = static_cast<double>(count) / sumCount;
        }
    }

    if (!reschedule) {
        return;
    }
    // Do the reschedule job
    for (auto& [instanceName, localType] : localScheduledOperatorsMap) {
        if (!sourceDist.contains(instanceName)) {
            continue;
        }
        if (localType == LocalStatelessOperatorType::ROUNDROBIN_HEAD ||
            localType == LocalStatelessOperatorType::COLLOCATE_HEAD) {
            continue;
        }
        auto expDist = expectedDist.at(instanceName);
        auto srcDist = sourceDist.at(instanceName);
        windowedRecords.at(instanceName)->updateWindow(expDist, srcDist);
    }
}

} // namespace faabric::batch_scheduler
