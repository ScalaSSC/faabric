#include <faabric/batch-scheduler/RuntimeSummary.h>

namespace faabric::batch_scheduler {

// --- Implementation of the new ProbabilisticScheduler ---

ProbabilisticScheduler::ProbabilisticScheduler(
  const std::map<std::string, double>& weights)
{
    if (weights.empty()) {
        SPDLOG_ERROR("Cannot create ProbabilisticScheduler with empty weights");
        throw std::runtime_error("Empty weights for scheduler");
    }

    double cumulative = 0.0;
    for (const auto& [host, weight] : weights) {
        cumulative += weight;
        cdf.emplace_back(cumulative, host);
    }

    // Sanity check to ensure the total probability is close to 1.0
    if (cdf.empty() || std::abs(cdf.back().first - 1.0) > 1e-9) {
        SPDLOG_WARN("Weights do not sum to 1.0 for scheduler. Sum is {}",
                    cdf.back().first);
    }
}

const std::string& ProbabilisticScheduler::schedule() const
{
    // Use a thread-local random number generator for performance and safety.
    // This avoids locking a global generator.
    thread_local std::mt19937 generator(std::random_device{}());
    thread_local std::uniform_real_distribution<double> distribution(0.0, 1.0);

    double p = distribution(generator);

    // Find the host whose cumulative probability range contains 'p'
    for (const auto& [cumulative_prob, host] : cdf) {
        if (p <= cumulative_prob) {
            return host;
        }
    }

    // Fallback to the last host (should only happen with floating point errors)
    return cdf.back().second;
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
            if (scheduledOpt.node.type !=
                batch_scheduler::NodeType::STATELESS) {
                continue; // Only stateless operators are considered
            }
            if (scheduledOpt.node.isInput) {
                initOperators.insert(optName);
            }
        }
    }
    // Otherwise, we just initialize the distribution ring for successor
    // operators (whose source is in this node).
    else {
        for (const auto& [optName, scheduledOpt] : scheduledOperatorsMap) {
            if (scheduledOpt.node.type !=
                batch_scheduler::NodeType::STATELESS) {
                continue; // Only stateless operators are considered
            }
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

    // localScheduledOperatorsMap[instanceName] = scheduledOpt.localType;
    // Update the expected distribution and source distribution.

    doInitExpectedDist(scheduledOpt);
    auto& expDist = expectedDist[instanceName];

    // CHANGED: Create a ProbabilisticScheduler instead of a WindowedRecord.
    // Note: The complex logic from getBodyWindowedSlots and srcDist is removed
    // as it's part of the stateful, corrective windowing model. The
    // probabilistic model simply uses the target expected distribution.
    probabilisticSchedulers[instanceName] =
      std::make_shared<ProbabilisticScheduler>(expDist);
}

std::string RuntimeSummary::getHost(const std::string& instance,
                                    unsigned int counter)
{
    // The 'counter' argument is ignored in the probabilistic model.
    faabric::util::SharedLock lock(summaryMx);
    auto it = probabilisticSchedulers.find(instance);
    if (it == probabilisticSchedulers.end()) {
        SPDLOG_ERROR("No scheduler for instance {} found", instance);
        throw std::runtime_error("No scheduler for instance " + instance);
    }
    // Directly call the lock-free schedule method
    return it->second->schedule();
}

std::string RuntimeSummary::getHost(const std::string& instance,
                                    const std::string& recommended)
{
    // The 'recommended' host is also ignored, as scheduling is purely random.
    return recommended; // This is a no-op in the probabilistic model.
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

void RuntimeSummary::updateSourceDist(
  std::map<std::string, std::map<std::string, int>> sourceCountStats,
  bool reschedule)
{
    // TODO currently do nothing.
}
}