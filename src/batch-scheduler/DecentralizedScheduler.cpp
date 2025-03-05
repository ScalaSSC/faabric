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
    for (const auto& [func, info] : statesInfo) {
        SPDLOG_INFO(
          "Stateful function {} with {} parallelism", func, info.parallelism);

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
        stateHashRing[func] =
          std::make_shared<faabric::util::ConsistentHashRing>(
            functionParallelism[func]);
    }
}

std::string DecentralizedScheduler::scheduleMessage(
  const HostMap& hostMap,
  const std::unique_ptr<Message>& msg)
{
    // SPDLOG_DEBUG("DecentralizedScheduler scheduling message");

    if (msg->user().empty() || msg->function().empty()) {
        throw std::runtime_error("User or function is empty");
    }
    std::string userFunc = msg->user() + "_" + msg->function();
    std::string host = "unknown";
    // For function-state function, assign near state
    if (functionParallelism.contains(userFunc)) {
        // If function-state has not been initialized, initialize it.
        faabric::util::FullLock lock(scheduleMx);
        // TODO - get parallelism is not thread safe now
        auto parallelismInfo = getHashAndParallelismIndex(userFunc, *msg);
        lock.unlock();
        std::string userFuncPar =
          userFunc + "_" + std::to_string(parallelismInfo.parallelismIdx);
        userFunc = userFuncPar;
        if (stateHost.find(userFuncPar) == stateHost.end()) {
            throw std::runtime_error("StateHost is not initialized");
        }
        host = stateHost[userFuncPar];
        // Register the parallelismIdx to it.
        // If Scheduling failed, the next scheduling will overwrite it.
        msg->set_messagetype(parallelismInfo.messageType);
        msg->set_hash(parallelismInfo.hash);
        msg->set_parallelismid(parallelismInfo.parallelismIdx);
    }
    // Otherwise the request by using round robin.
    else {
        host = localHost;
        msg->set_messagetype(0);
    }
    if (host == "unknown") {
        throw std::runtime_error("Host is unknown");
    }

    SPDLOG_TRACE("Scheduling message {} to host {}", userFunc, host);
    return host;
}
}