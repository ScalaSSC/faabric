#include <faabric/batch-scheduler/RuntimeSummary.h>

#include <algorithm> // For std::max

namespace faabric::batch_scheduler {

struct LoadImbalance
{
    std::vector<std::pair<std::string, double>> hostsWithDecifit;
    double totalDeficit = 0.0;
};

LoadImbalance calculateLoadImbalance(
  const std::map<std::string, double>& expDist,
  const std::map<std::string, double>& implDist)
{
    LoadImbalance imbalance;
    for (const auto& [host, expectLoad] : expDist) {
        double implLoad = faabric::util::getOrThrow(implDist, host);
        if (implLoad > expectLoad) {
            double deficit = implLoad - expectLoad;
            imbalance.hostsWithDecifit.emplace_back(host, deficit);
            imbalance.totalDeficit += deficit;
        }
    }
    return imbalance;
}

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

std::string RuntimeSummary::getHost(const std::string& instance,
                                    unsigned int counter)
{
    // The 'counter' argument is ignored in the probabilistic model.
    faabric::util::SharedLock lock(summaryMx);
    auto scheduler = util::getOrThrow(probabilisticSchedulers, instance);
    // Directly call the lock-free schedule method
    return scheduler->schedule();
}

std::string RuntimeSummary::getHost(const std::string& instance,
                                    const std::string& recommended)
{
    faabric::util::SharedLock lock(summaryMx);
    recommendedHostMap[instance][recommended]++;
    auto metaScheduler = util::getOrThrow(metaSchedulers, instance);
    auto scheduler = util::getOrThrow(*metaScheduler, recommended);

    return scheduler->schedule(); // This is a no-op in the probabilistic model.
}

void RuntimeSummary::initScheduledOperators(
  const batch_scheduler::Application& application,
  const std::map<std::string, ScheduledOperator>& scheduledOperatorsMapIn,
  bool planner,
  int scheduleMode)
{
    faabric::util::FullLock lock(summaryMx);
    scheduledOperatorsMap = scheduledOperatorsMapIn;

    SPDLOG_DEBUG("Initializing scheduled operators distribution ring");
    expectedDistMap.clear();
    implDistMap.clear();
    recommendedHostMap.clear();
    isPlanner = planner;

    initAll(application, planner, scheduleMode);
}

void RuntimeSummary::initAll(const batch_scheduler::Application& application,
                             bool planner,
                             int scheduleMode)
{
    std::set<std::string> initOperators;
    // For planner, we need to initialize the schedule for input opeartors.
    if (planner) {
        for (const auto& [optName, scheduledOpt] : scheduledOperatorsMap) {
            if (scheduledOpt.node.type !=
                batch_scheduler::NodeType::STATELESS) {
                continue; // Only stateless operators are considered
            }
            if (scheduledOpt.node.isInput || scheduleMode == 7) {
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
                if (!scheduledOperatorsMap.contains(sourceNode->name)) {
                    continue;
                }
                auto sourceOpt =
                  util::getOrThrow(scheduledOperatorsMap, sourceNode->name);
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

    std::string instanceName = scheduledOpt.node.name + "_0";

    doInitExpectedDist(scheduledOpt);
    auto& expDist = expectedDistMap[instanceName];
    implDistMap[instanceName] = expDist;

    if (isBody && isCollocate) {
        scheduledOpt.localType = LocalStatelessOperatorType::COLLOCATE_BODY;
        if (implDistMap[instanceName].count(localHost) == 0) {
            scheduledOpt.localType = LocalStatelessOperatorType::COLLOCATE_HEAD;
        }
    } else if (isBody && !isCollocate) {
        scheduledOpt.localType = LocalStatelessOperatorType::ROUNDROBIN_BODY;
        if (implDistMap[instanceName].count(localHost) == 0) {
            scheduledOpt.localType =
              LocalStatelessOperatorType::ROUNDROBIN_HEAD;
        }
    } else if (!isBody && isCollocate) {
        scheduledOpt.localType = LocalStatelessOperatorType::COLLOCATE_HEAD;
    } else if (!isBody && !isCollocate) {
        scheduledOpt.localType = LocalStatelessOperatorType::ROUNDROBIN_HEAD;
    }

    localOperatorsMap[instanceName] = scheduledOpt.localType;
    // Update the expected distribution and source distribution.

    auto& schedLocalTpye = scheduledOpt.localType;
    std::map<std::string, double> schedulingWeights;
    if (schedLocalTpye == LocalStatelessOperatorType::COLLOCATE_BODY) {
        recommendedHostMap[instanceName][localHost] = 1;
        schedulingWeights[localHost] = 1.0;
        auto metaScheduler =
          buildMetaScheduler(instanceName, schedulingWeights);
        metaSchedulers[instanceName] =
          std::make_shared<MetaScheduler>(metaScheduler);
    } else if (schedLocalTpye == LocalStatelessOperatorType::ROUNDROBIN_BODY) {
        schedulingWeights[localHost] = 1.0;
        probabilisticSchedulers[instanceName] =
          std::make_shared<ProbabilisticScheduler>(schedulingWeights);
    } else if (schedLocalTpye == LocalStatelessOperatorType::COLLOCATE_HEAD) {
        std::map<std::string, double> schedulingWeights = expDist;
        auto metaScheduler =
          buildMetaScheduler(instanceName, schedulingWeights);
        metaSchedulers[instanceName] =
          std::make_shared<MetaScheduler>(metaScheduler);
    } else if (schedLocalTpye == LocalStatelessOperatorType::ROUNDROBIN_HEAD) {
        schedulingWeights = expDist;
        probabilisticSchedulers[instanceName] =
          std::make_shared<ProbabilisticScheduler>(schedulingWeights);
    } else {
        SPDLOG_ERROR("Unknown local operator type for instance {}",
                     instanceName);
        throw std::runtime_error("Unknown local operator type for instance");
    }
}

void RuntimeSummary::doInitExpectedDist(ScheduledOperator& schedOp)
{
    std::string instanceName = schedOp.node.name + "_0";
    auto hostWeight = schedOp.weightDist;

    double sumWeight = 0;
    for (const auto& [host, weight] : hostWeight) {
        sumWeight += weight;
    }
    if (sumWeight <= 0) {
        std::string errorMsg =
          "Total weight <= 0 for operator " + schedOp.node.name;
        SPDLOG_WARN("{}", errorMsg);
        throw std::runtime_error(errorMsg);
    }
    auto& dist = expectedDistMap[instanceName];
    for (const auto& [host, weight] : hostWeight) {
        dist[host] = weight / sumWeight;
    }
}

void RuntimeSummary::TuneImplDist(
  const std::string instanceName,
  const std::map<std::string, int>& observedDist)
{
    SPDLOG_DEBUG(
      "Tuning implementation distribution for instance {} with observed "
      "distribution",
      instanceName);

    if (implDistMap.count(instanceName) == 0) {
        SPDLOG_ERROR(
          "No implementation distribution for instance {}, skipping tuning",
          instanceName);
    }
    auto implDist = util::getOrThrow(implDistMap, instanceName);
    if (expectedDistMap.count(instanceName) == 0) {
        SPDLOG_ERROR(
          "No expectedDistMap distribution for instance {}, skipping tuning",
          instanceName);
    }
    const auto& expectedDist = util::getOrThrow(expectedDistMap, instanceName);

    auto observedProbs = util::calculateProportions(observedDist);
    if (observedProbs.empty()) {
        return;
    }

    // Calculate the Error: Error[W]=Target[W]−Observed[W]
    // Calculate the New Distribution: NewActual[W]=OldActual[W]+α×Error[W]
    // --- NewActual = OldActual + alpha * (Target - Observed)
    std::map<std::string, double> newImplDist;
    double newImplDistSum = 0.0;

    for (const auto& [worker, expectedProb] : expectedDist) {
        double oldImplProb = implDist[worker];
        double observedProb = observedProbs[worker];

        double error = expectedProb - observedProb;
        double newProb = oldImplProb + alpha.load() * error;

        // Ensure the new probability is not negative
        newProb = std::max(0.0, newProb);

        newImplDist[worker] = newProb;
        newImplDistSum += newProb;
    }

    if (newImplDistSum > 0) {
        for (auto& [worker, prob] : newImplDist) {
            prob /= newImplDistSum;
        }
    }

    implDistMap[instanceName] = newImplDist;
}

std::map<std::string, double> RuntimeSummary::calculateBodyWeights(
  const std::string instanceName,
  const std::map<std::string, int>& observedDist)
{
    TuneImplDist(instanceName, observedDist);

    const auto& expectedDist = util::getOrThrow(expectedDistMap, instanceName);
    const auto& implDist = implDistMap.at(instanceName);
    double implLocalProb = util::getOrThrow(implDist, localHost);
    double expectedLocalProb = util::getOrThrow(expectedDist, localHost);

    if (implLocalProb >= expectedLocalProb) {
        return { { localHost, 1.0 } };
    }

    std::map<std::string, double> schedulingWeights;

    double keepRatio = implLocalProb / expectedLocalProb;
    schedulingWeights[localHost] = keepRatio;
    auto imbalance = calculateLoadImbalance(expectedDist, implDist);

    // Distribute the offloaded part, with a safety check
    double offloadRatio = 1.0 - keepRatio;
    if (imbalance.totalDeficit > 0.0) {
        for (const auto& [decifitHost, surplus] : imbalance.hostsWithDecifit) {
            double offloadPortion = surplus / imbalance.totalDeficit;
            schedulingWeights[decifitHost] += offloadRatio * offloadPortion;
        }
    }

    return schedulingWeights;
}

MetaScheduler RuntimeSummary::buildMetaScheduler(
  const std::string& instanceName,
  std::map<std::string, double> schedulingWeights)
{
    SPDLOG_DEBUG("Building body meta-scheduler for instance {}", instanceName);

    std::map<std::string, double> recoHostProp;
    if (recommendedHostMap.contains(instanceName)) {
        auto recommendedHost = recommendedHostMap[instanceName];
        recoHostProp = util::calculateProportions(recommendedHost);
    } else {
        recoHostProp = schedulingWeights;
    }
    MetaScheduler metaScheduler;

    LoadImbalance imbalance;
    for (const auto& [host, schedulingWeight] : schedulingWeights) {
        double recoWeight = recoHostProp[host];
        if (schedulingWeight > recoWeight) {
            double deficit = schedulingWeight - recoWeight;
            imbalance.hostsWithDecifit.emplace_back(host, deficit);
            imbalance.totalDeficit += deficit;
        }
    }

    for (const auto& [host, schedulingWeight] : schedulingWeights) {
        double recoWeight = recoHostProp[host];
        // For deficit operator, schedule it locally only
        std::map<std::string, double> hostScheduleWeight;
        if (schedulingWeight > recoWeight) {
            hostScheduleWeight[host] = recoWeight / schedulingWeight;
            double offloadRatio = 1 - hostScheduleWeight[host];

            for (const auto& [deficitHost, deficit] :
                 imbalance.hostsWithDecifit) {
                double offloadPortion = deficit / imbalance.totalDeficit;
                hostScheduleWeight[deficitHost] +=
                  offloadRatio * offloadPortion;
            }
        }
        if (schedulingWeight <= recoWeight) {
            hostScheduleWeight[host] = 1.0;
        }
        metaScheduler.emplace(
          host, std::make_shared<ProbabilisticScheduler>(hostScheduleWeight));
    }

    return metaScheduler;
}

void RuntimeSummary::RoundRobinBodyTune(
  const std::string instanceName,
  const std::map<std::string, int>& observedDist)
{
    SPDLOG_DEBUG(
      "Tuning round-robin body for instance {} with observed distribution",
      instanceName);

    auto schedulingWeights = calculateBodyWeights(instanceName, observedDist);

    faabric::util::FullLock lock(summaryMx);
    probabilisticSchedulers[instanceName] =
      std::make_shared<ProbabilisticScheduler>(schedulingWeights);
}

void RuntimeSummary::CollocateHeadTune(
  const std::string instanceName,
  const std::map<std::string, int>& observedDist)
{
    SPDLOG_DEBUG(
      "Tuning collocate head for instance {} with observed distribution",
      instanceName);
    TuneImplDist(instanceName, observedDist);

    auto schedulingWeights = implDistMap[instanceName];
    auto metaScheduler = buildMetaScheduler(instanceName, schedulingWeights);

    faabric::util::FullLock lock(summaryMx);
    metaSchedulers[instanceName] =
      std::make_shared<MetaScheduler>(metaScheduler);
}

void RuntimeSummary::CollocateBodyTune(
  const std::string instanceName,
  const std::map<std::string, int>& observedDist)
{
    SPDLOG_DEBUG(
      "Tuning collocate body for instance {} with observed distribution",
      instanceName);
    auto schedulingWeight = calculateBodyWeights(instanceName, observedDist);
    auto metaScheduler = buildMetaScheduler(instanceName, schedulingWeight);

    faabric::util::FullLock lock(summaryMx);
    metaSchedulers[instanceName] =
      std::make_shared<MetaScheduler>(metaScheduler);
}

// observedDistMap
// MAP <instance name: <host, count>> number of requests observed executed on
// each host.
void RuntimeSummary::requestDistTune(
  const std::map<std::string, std::map<std::string, int>>& observedDistMap)
{
    SPDLOG_DEBUG("Updating stateless request distribution");
    if (expectedDistMap.empty() || implDistMap.empty()) {
        return; // Nothing to tune if expected distribution is empty
    }

    for (const auto& [instanceName, operatorType] : localOperatorsMap) {
        if (!observedDistMap.contains(instanceName)) {
            SPDLOG_DEBUG("No observed distribution for instance {}",
                         instanceName);
            continue;
        }
        auto observedDist = util::getOrThrow(observedDistMap, instanceName);
        switch (operatorType) {
            case LocalStatelessOperatorType::ROUNDROBIN_HEAD:
                break; // We don't tune it.
            case LocalStatelessOperatorType::ROUNDROBIN_BODY:
                RoundRobinBodyTune(instanceName, observedDist);
                break;
            case LocalStatelessOperatorType::COLLOCATE_HEAD:
                CollocateHeadTune(instanceName, observedDist);
                break;
            case LocalStatelessOperatorType::COLLOCATE_BODY:
                CollocateBodyTune(instanceName, observedDist);
                break;
            default:
                SPDLOG_ERROR("Unknown operator type for instance {}",
                             instanceName);
                throw std::runtime_error("Unknown operator type for instance");
        }
    }
    recommendedHostMap.clear();
}

void RuntimeSummary::setAlpha(double value){
    alpha.store(value);
}

void RuntimeSummary::reset()
{
    faabric::util::FullLock lock(summaryMx);
    SPDLOG_DEBUG("Clearing runtime summary");
    localOperatorsMap.clear();
    expectedDistMap.clear();
    implDistMap.clear();
    recommendedHostMap.clear();
    scheduledOperatorsMap.clear();
    probabilisticSchedulers.clear();
    metaSchedulers.clear();
}
}