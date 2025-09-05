#include <faabric/batch-scheduler/RuntimeSummary.h>

#include <algorithm> // For std::max

namespace faabric::batch_scheduler {

// --- Implementation of the new ProbabilisticScheduler ---

MetaScheduler RuntimeSummary::buildHeadMetaScheduler(
  const std::string& instanceName)
{
    SPDLOG_DEBUG("Building head meta-scheduler for instance {}", instanceName);

    auto expectDistribution = util::getOrThrow(expectedDistMap, instanceName);
    auto implDistribution = util::getOrThrow(implDistMap, instanceName);
    MetaScheduler metaScheduler;

    // When the implementation distribution is higher than expected
    // distribution, it means this host received less msgs than expected. So we
    // want to schedule more msgs to this host.
    std::vector<std::pair<std::string, double>> underloadedHosts;
    double totalDeficit = 0.0;

    for (const auto& [host, expectLoad] : expectDistribution) {
        double implLoad = implDistribution[host];
        if (implLoad > expectLoad) {
            double deficit = implLoad - expectLoad;
            underloadedHosts.emplace_back(host, deficit);
            totalDeficit += deficit;
        }
    }

    for (const auto& [host, expectLoad] : expectDistribution) {
        std::map<std::string, double> schedulingWeights;
        double implLoad = implDistribution[host];

        if (implLoad >= expectLoad || underloadedHosts.empty()) {
            schedulingWeights[host] = 1.0;
        } else {
            double keepRatio = implLoad / expectLoad;
            schedulingWeights[host] = keepRatio;

            // The rest is offloaded to the underloaded hosts
            double offloadRatio = 1.0 - keepRatio;
            for (const auto& [underloadedHost, deficit] : underloadedHosts) {
                double offloadPortion = deficit / totalDeficit;
                schedulingWeights[underloadedHost] +=
                  offloadRatio * offloadPortion;
            }
        }
        metaScheduler.emplace(
          host, std::make_shared<ProbabilisticScheduler>(schedulingWeights));
    }

    metaSchedulers[instanceName] =
      std::make_shared<MetaScheduler>(metaScheduler);
    return metaScheduler;
}

MetaScheduler RuntimeSummary::buildBodyMetaScheduler(
  const std::string& instanceName)
{
    SPDLOG_DEBUG("Building body meta-scheduler for instance {}", instanceName);

    auto expectDistribution = util::getOrThrow(expectedDistMap, instanceName);
    auto implDistribution = util::getOrThrow(implDistMap, instanceName);

    MetaScheduler metaScheduler;

    // When the implementation distribution is higher than expected
    // distribution, it means this host received less msgs than expected. So we
    // want to schedule more msgs to this host.
    std::vector<std::pair<std::string, double>> underloadedHosts;
    double totalDeficit = 0.0;

    for (const auto& [host, expectLoad] : expectDistribution) {
        double implLoad = implDistribution[host];
        if (implLoad > expectLoad) {
            double deficit = implLoad - expectLoad;
            underloadedHosts.emplace_back(host, deficit);
            totalDeficit += deficit;
        }
    }

    // We prefer to fill local hosts first.
    double implLocal = implDistribution[localHost];
    double expectLocal = util::getOrThrow(expectDistribution, localHost);
    if (implLocal > expectLocal) {
        std::map<std::string, double> schedulingWeights;
        for (const auto& [host, expectLoad] : expectDistribution) {
            schedulingWeights[localHost] = 1.0;
            metaScheduler.emplace(
              host,
              std::make_shared<ProbabilisticScheduler>(schedulingWeights));
        }
    } else {
        std::map<std::string, double> schedulingProp;
        // Distribute the load across all hosts.
        double keepRatio = implLocal / expectLocal;
        schedulingProp[localHost] = keepRatio;

        // The rest is offloaded to the underloaded hosts
        double offloadRatio = 1.0 - keepRatio;
        for (const auto& [underloadedHost, deficit] : underloadedHosts) {
            double offloadPortion = deficit / totalDeficit;
            schedulingProp[underloadedHost] += offloadRatio * offloadPortion;
        }

        auto recommendedHost = recommendedHostMap[instanceName];
        if (recommendedHost.empty()) {
            recommendedHost[localHost] = 1;
        }
        std::map<std::string, double> recoHostProp =
          util::calculateProportions(recommendedHost);

        std::map<std::string, double> hostsDeflics;
        double totalDeflic = 0.0;
        for (const auto& [host, proportion] : schedulingProp) {
            if (proportion > recoHostProp[host]) {
                hostsDeflics[host] = proportion - recoHostProp[host];
                totalDeflic += hostsDeflics[host];
            }
        }
        for (const auto& [host, expectWeight] : expectDistribution) {
            std::map<std::string, double> schedulingWeights;

            if (recommendedHost.count(host) == 0) {
                schedulingWeights[localHost] = 1.0;
            } else {
                double proportion = schedulingProp[host];
                if (proportion - recoHostProp[host] >= 0) {
                    schedulingWeights[host] = 1.0;
                } else {
                    schedulingWeights[host] = proportion / recoHostProp[host];
                    double distributionFactor = 1 - schedulingWeights[host];
                    for (const auto& [deflicHost, deflic] : hostsDeflics) {
                        schedulingWeights[deflicHost] =
                          distributionFactor * deflic / totalDeflic;
                    }
                }
            }
            metaScheduler.emplace(
              host,
              std::make_shared<ProbabilisticScheduler>(schedulingWeights));
        }
    }

    metaSchedulers[instanceName] =
      std::make_shared<MetaScheduler>(metaScheduler);
    return metaScheduler;
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

void RuntimeSummary::initScheduledOperators(
  const batch_scheduler::Application& application,
  const std::map<std::string, ScheduledOperator>& scheduledOperatorsMapIn,
  bool planner)
{
    faabric::util::FullLock lock(summaryMx);
    scheduledOperatorsMap = scheduledOperatorsMapIn;

    SPDLOG_DEBUG("Initializing scheduled operators distribution ring");
    // Update the expected distribution and source distribution.
    expectedDistMap.clear();
    implDistMap.clear();
    recommendedHostMap.clear();

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
            scheduledOpt.localType = LocalStatelessOperatorType::ROUNDROBIN_HEAD;
        }
    } else if (!isBody && isCollocate) {
        scheduledOpt.localType = LocalStatelessOperatorType::COLLOCATE_HEAD;
    } else if (!isBody && !isCollocate) {
        scheduledOpt.localType = LocalStatelessOperatorType::ROUNDROBIN_HEAD;
    }

    localOperatorsMap[instanceName] = scheduledOpt.localType;
    // Update the expected distribution and source distribution.

    auto& schedLocalTpye = scheduledOpt.localType;
    if (schedLocalTpye == LocalStatelessOperatorType::COLLOCATE_BODY) {
        recommendedHostMap[instanceName][localHost] = 1;
        buildBodyMetaScheduler(instanceName);
    } else if (schedLocalTpye == LocalStatelessOperatorType::ROUNDROBIN_BODY) {
        std::map<std::string, double> schedulingWeights;
        schedulingWeights[localHost] = 1.0;
        probabilisticSchedulers[instanceName] =
          std::make_shared<ProbabilisticScheduler>(schedulingWeights);
    } else if (schedLocalTpye == LocalStatelessOperatorType::COLLOCATE_HEAD) {
        buildHeadMetaScheduler(instanceName);
    } else if (schedLocalTpye == LocalStatelessOperatorType::ROUNDROBIN_HEAD) {
        std::map<std::string, double> schedulingWeights;
        schedulingWeights = expDist;
        probabilisticSchedulers[instanceName] =
          std::make_shared<ProbabilisticScheduler>(schedulingWeights);
    } else {
        SPDLOG_ERROR("Unknown local operator type for instance {}",
                     instanceName);
        throw std::runtime_error("Unknown local operator type for instance");
    }
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
        return; // Nothing to tune if observed distribution is empty
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
        double newProb = oldImplProb + alpha * error;

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

void RuntimeSummary::CollocateHeadTune(
  const std::string instanceName,
  const std::map<std::string, int>& observedDist)
{
    TuneImplDist(instanceName, observedDist);
    SPDLOG_DEBUG(
      "Tuning collocate head for instance {} with observed distribution",
      instanceName);
    buildHeadMetaScheduler(instanceName);
}

void RuntimeSummary::CollocateBodyTune(
  const std::string instanceName,
  const std::map<std::string, int>& observedDist)
{
    TuneImplDist(instanceName, observedDist);
    SPDLOG_DEBUG(
      "Tuning collocate body for instance {} with observed distribution",
      instanceName);
    buildBodyMetaScheduler(instanceName);
}

void RuntimeSummary::RoundRobinBodyTune(
  const std::string instanceName,
  const std::map<std::string, int>& observedDist)
{
    TuneImplDist(instanceName, observedDist);

    SPDLOG_DEBUG(
      "Tuning round-robin body for instance {} with observed distribution",
      instanceName);

    const auto& expectedDist = util::getOrThrow(expectedDistMap, instanceName);
    auto& implDist = implDistMap[instanceName];
    double implLocalProb = util::getOrThrow(implDist, localHost);
    double expectedLocalProb = util::getOrThrow(expectedDist, localHost);

    std::map<std::string, double> schedulingWeights;
    if (implLocalProb >= expectedLocalProb) {
        schedulingWeights[localHost] = 1.0;
    } else {
        schedulingWeights[localHost] = implLocalProb / expectedLocalProb;

        std::vector<std::pair<std::string, double>> underloadedHosts;
        double totalDeficit = 0.0;
        for (const auto& [host, expectLoad] : expectedDist) {
            double implLoad = implDist[host];
            if (implLoad > expectLoad) {
                double deficit = implLoad - expectLoad;
                underloadedHosts.emplace_back(host, deficit);
                totalDeficit += deficit;
            }
        }

        double offloadRatio = 1.0 - schedulingWeights[localHost];
        for (const auto& [underloadedHost, deficit] : underloadedHosts) {
            double offloadPortion = deficit / totalDeficit;
            schedulingWeights[underloadedHost] += offloadRatio * offloadPortion;
        }
    }
    probabilisticSchedulers[instanceName] =
      std::make_shared<ProbabilisticScheduler>(schedulingWeights);
}

// observedDistMap
// MAP <instance name: <host, count>> number of requests observed executed on
// each host.
void RuntimeSummary::requestDistTune(
  const std::map<std::string, std::map<std::string, int>>& observedDistMap)
{
    faabric::util::FullLock lock(summaryMx);
    SPDLOG_DEBUG("Updating stateless request distribution");
    if (expectedDistMap.empty()) {
        return; // Nothing to tune if expected distribution is empty
    }
    for (const auto& [instanceName, operatorType] : localOperatorsMap) {
        if (!observedDistMap.contains(instanceName)) {
            SPDLOG_DEBUG("No observed distribution for instance {}",
                         instanceName);
            continue;
        }
        SPDLOG_DEBUG("finding the observed distribution for instance {}",
                     instanceName);
        auto observedDist = util::getOrThrow(observedDistMap, instanceName);
        if (operatorType == LocalStatelessOperatorType::ROUNDROBIN_HEAD) {
            continue;
        } else if (operatorType ==
                   LocalStatelessOperatorType::ROUNDROBIN_BODY) {
            RoundRobinBodyTune(instanceName, observedDist);
        } else if (operatorType == LocalStatelessOperatorType::COLLOCATE_HEAD) {
            CollocateHeadTune(instanceName, observedDist);
        } else if (operatorType == LocalStatelessOperatorType::COLLOCATE_BODY) {
            CollocateBodyTune(instanceName, observedDist);
        } else {
            SPDLOG_ERROR("Unknown operator type for instance {}", instanceName);
            throw std::runtime_error("Unknown operator type for instance");
        }
    }
    recommendedHostMap.clear();
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