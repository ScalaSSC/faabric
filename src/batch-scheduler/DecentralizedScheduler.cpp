#include <faabric/batch-scheduler/DecentralizedScheduler.h>
#include <faabric/batch-scheduler/SchedulingDecision.h>
#include <faabric/util/locks.h>
#include <faabric/util/logging.h>

namespace faabric::batch_scheduler {

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
    for (const auto& [func, info] : statesInfo) {
        SPDLOG_INFO(
          "Stateful function {} with {} parallelism", func, info.parallelism);

        if (info.parallelism != info.stateHost.size()) {
            SPDLOG_ERROR("Parallelism and stateHost size mismatch");
            throw std::runtime_error("Parallelism and stateHost size mismatch");
        }

        functionParallelism[func] = info.parallelism;
        for (const auto& [parallelismIdx, host] : info.stateHost) {
            std::string stateKey = func + "_" + std::to_string(parallelismIdx);
            stateHost[stateKey] = host;
            SPDLOG_INFO("Mapping: {} -> {}", stateKey, host);
        }

        // If the state is partitioned, update the Hash method.
        if (info.partitionBy == "" || info.partitionBy == "None") {
            continue;
        }
        SPDLOG_INFO("Function {} is partitioned stateful with {}",
                    func,
                    info.partitionBy);
        statePartitionBy[func] = info.partitionBy;
        auto weightDist = parStateReqWeight[func];
        stateHashRing[func] =
          std::make_shared<faabric::util::ConsistentHashRing>(weightDist);
    }
}

}