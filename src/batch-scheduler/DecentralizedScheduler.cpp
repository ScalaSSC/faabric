#include <faabric/batch-scheduler/DecentralizedScheduler.h>
#include <faabric/batch-scheduler/SchedulingDecision.h>
#include <faabric/util/locks.h>
#include <faabric/util/logging.h>

namespace faabric::batch_scheduler {

void DecentralizedScheduler::setScheuduledOperatorMap(
  const std::map<std::string, ScheduledOperator>& scheuduledOperatorMapIn)
{
    faabric::util::FullLock lock(scheduleMx);
    scheduledOperatorsMap = scheuduledOperatorMapIn;
}

void DecentralizedScheduler::resetScheduler()
{
    StateAwareScheduler::resetScheduler();
}

void DecentralizedScheduler::syncStatesInfo(
  const std::map<std::string, faabric::batch_scheduler::FunctionStateInfo>&
    statesInfo)
{
    faabric::util::FullLock lock(scheduleMx);

    // Clean up the existing state info
    functionParallelism.clear();
    stateHost.clear();
    statePartitionBy.clear();
    stateHashRing.clear();

    SPDLOG_INFO("Denctralscheduler syns {} function states", statesInfo.size());
    // func is userFuncPar
    for (const auto& [userFunc, info] : statesInfo) {
        SPDLOG_INFO("Stateful function {} with {} parallelism",
                    userFunc,
                    info.parallelism);

        if (info.parallelism != info.stateHost.size()) {
            SPDLOG_ERROR("Parallelism and stateHost size mismatch");
            throw std::runtime_error("Parallelism and stateHost size mismatch");
        }

        functionParallelism[userFunc] = info.parallelism;
        for (const auto& [parallelismIdx, host] : info.stateHost) {
            std::string userFuncPar =
              userFunc + "_" + std::to_string(parallelismIdx);
            stateHost[userFuncPar] = host;
        }

        // If the state is partitioned, update the Hash method.
        if (info.partitionBy == "" || info.partitionBy == "None") {
            continue;
        }
        statePartitionBy[userFunc] = info.partitionBy;
        auto scheduledOpt =
          getScheduledOperatorOrThrow(scheduledOperatorsMap, userFunc);
        auto parallelismDist = scheduledOpt.parallelismDist;
        auto weightDist = scheduledOpt.weightDist;
        std::map<int, int> parStateReqWeight;
        for (const auto& [idx, ip] : parallelismDist) {
            if (weightDist.contains(ip)) {
                parStateReqWeight[idx] = weightDist.at(ip);
            } else {
                SPDLOG_ERROR("Weight distribution for {} not found", ip);
                throw std::runtime_error("Weight distribution not found");
            }
        }
        stateHashRing[userFunc] =
          std::make_shared<faabric::util::ConsistentHashRing>(
            parStateReqWeight);
    }

    runtimeSummary.initScheduledOperators(
      *application, scheduledOperatorsMap, false);

    printScheduleInfomation();
}
}