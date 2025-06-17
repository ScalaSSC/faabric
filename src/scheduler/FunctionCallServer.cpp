#include <faabric/executor/ExecutorFactory.h>
#include <faabric/scheduler/FunctionCallServer.h>
#include <faabric/snapshot/SnapshotRegistry.h>
#include <faabric/state/State.h>
#include <faabric/transport/common.h>
#include <faabric/transport/macros.h>
#include <faabric/util/config.h>
#include <faabric/util/func.h>
#include <faabric/util/logging.h>

namespace faabric::scheduler {
FunctionCallServer::FunctionCallServer()
  : faabric::transport::MessageEndpointServer(
      FUNCTION_CALL_ASYNC_PORT,
      FUNCTION_CALL_SYNC_PORT,
      FUNCTION_INPROC_LABEL,
      faabric::util::getSystemConfig().functionServerThreads)
  , scheduler(getScheduler())
{}

void FunctionCallServer::doAsyncRecv(transport::Message& message)
{
    uint8_t header = message.getMessageCode();
    switch (header) {
        case faabric::scheduler::FunctionCalls::ExecuteFunctions: {
            recvExecuteFunctions(message.udata());
            break;
        }
        case faabric::scheduler::FunctionCalls::ExecuteFunctionsBatch: {
            recvExecuteFunctionsBatch(message.udata());
            break;
        }
        case faabric::scheduler::FunctionCalls::SetMessageResult: {
            recvSetMessageResult(message.udata());
            break;
        }
        case faabric::scheduler::FunctionCalls::ResetParameter: {
            recvResetParameter(message.udata());
            break;
        }
        case faabric::scheduler::FunctionCalls::SetPersistentState: {
            recvSetPersistentState(message.udata());
            break;
        }
        default: {
            throw std::runtime_error(
              fmt::format("Unrecognized async call header: {}", header));
        }
    }
}

std::unique_ptr<google::protobuf::Message> FunctionCallServer::doSyncRecv(
  transport::Message& message)
{
    uint8_t header = message.getMessageCode();
    switch (header) {
        case faabric::scheduler::FunctionCalls::Flush: {
            return recvFlush(message.udata());
        }
        case faabric::scheduler::FunctionCalls::SyncStatesInfo: {
            return recvSyncStatesInfo(message.udata());
        }
        case faabric::scheduler::FunctionCalls::MigrateStates: {
            return recvMigrateStates(message.udata());
        }
        // case faabric::scheduler::FunctionCalls::GetWorkerLoad: {
        //     return recvGetWorkerLoad(message.udata());
        // }
        case faabric::scheduler::FunctionCalls::GetRuntimeStats: {
            return recvGetRuntimeStats(message.udata());
        }
        default: {
            throw std::runtime_error(
              fmt::format("Unrecognized sync call header: {}", header));
        }
    }
}

std::unique_ptr<google::protobuf::Message> FunctionCallServer::recvFlush(
  std::span<const uint8_t> buffer)
{
    SPDLOG_INFO("Flushing host {}",
                faabric::util::getSystemConfig().endpointHost);

    // Clear the scheduler
    faabric::scheduler::getScheduler().reset();

    // Clear out any cached state
    faabric::state::getGlobalState().forceClearAll(false);

    // Clear the executor factory
    faabric::executor::getExecutorFactory()->flushHost();

    return std::make_unique<faabric::EmptyResponse>();
}

std::unique_ptr<google::protobuf::Message>
FunctionCallServer::recvSyncStatesInfo(std::span<const uint8_t> buffer)
{
    SPDLOG_INFO("Syncing state info in host {}",
                faabric::util::getSystemConfig().endpointHost);

    PARSE_MSG(planner::SyncStatesInfoRequest, buffer.data(), buffer.size())

    // Update the stateless and partitioned stateful operator weights.
    std::map<std::string, std::map<std::string, int>> newStatelessReqWeight;
    std::map<std::string, std::map<int, int>> newParStateReqWeight;
    std::map<std::string, std::string> newOptCollocate;
    std::map<std::string, std::string> newOptCollocateHead;

    const auto& srwMap = parsedMsg.statelessreqweight();
    for (const auto& outer : srwMap) {
        const std::string& funcPar = outer.first;
        const auto& innerMsg = outer.second;

        const auto& hostMap = innerMsg.hostweight();
        for (const auto& hostPair : hostMap) {
            newStatelessReqWeight[funcPar][hostPair.first] = hostPair.second;
        }
    }

    const auto& pswMap = parsedMsg.parstatereqweight();
    for (const auto& outer : pswMap) {
        const std::string& userFunc = outer.first;
        const auto& innerMsg = outer.second;

        const auto& partMap = innerMsg.partitionweight();
        for (const auto& partPair : partMap) {
            newParStateReqWeight[userFunc][partPair.first] = partPair.second;
        }
    }

    const auto& ocMap = parsedMsg.operatorcollocatemap();
    for (const auto& [statelessOp, parStateOp] : ocMap) {
        newOptCollocate[statelessOp] = parStateOp;
    }

    const auto& ochMap = parsedMsg.operatorcollocateheadmap();
    for (const auto& [statelessOp, parStateOp] : ochMap) {
        newOptCollocateHead[statelessOp] = parStateOp;
    }

    std::map<std::string, faabric::batch_scheduler::FunctionStateInfo>
      tempStatesInfoMap;

    auto statesInfo = parsedMsg.statesinfo();
    for (const auto& stateInfo : statesInfo) {
        faabric::batch_scheduler::FunctionStateInfo info;
        info.functionName = stateInfo.functionname();
        info.partitionBy = stateInfo.partitionby();
        info.stateKey = stateInfo.statekey();
        info.parallelism = stateInfo.parallelism();
        for (const auto& entry : stateInfo.statehost()) {
            info.stateHost.emplace(entry.first, entry.second);
        }
        tempStatesInfoMap.insert({ stateInfo.functionname(), info });

        SPDLOG_DEBUG("Received state info: {}",
                     faabric::batch_scheduler::to_string(info));
    }

    scheduler.updateStatesInfo(newStatelessReqWeight,
                               newParStateReqWeight,
                               newOptCollocate,
                               newOptCollocateHead,
                               tempStatesInfoMap);

    return std::make_unique<planner::SyncStatesInfoResponse>();
}

std::unique_ptr<google::protobuf::Message>
FunctionCallServer::recvMigrateStates(std::span<const uint8_t> buffer)
{
    SPDLOG_DEBUG("RECEIVE Migrating states");

    PARSE_MSG(faabric::StateMigrationRequest, buffer.data(), buffer.size())

    std::ostringstream oss;
    for (const auto& state : parsedMsg.migratestates()) {
        oss << state.userfuncpar() << ":" << state.serializedstate().size()
            << "/";
    }

    SPDLOG_DEBUG("Migrating {} states to local host {}: {}",
                 parsedMsg.migratestates_size(),
                 faabric::util::getSystemConfig().endpointHost,
                 oss.str());

    std::multimap<std::string, std::string> immiStates;
    for (const auto& state : parsedMsg.migratestates()) {
        immiStates.emplace(state.userfuncpar(), state.serializedstate());
    }

    scheduler.storeMigrateState(std::move(immiStates));

    return std::make_unique<faabric::EmptyResponse>();
}

// std::unique_ptr<google::protobuf::Message>
// FunctionCallServer::recvGetWorkerLoad(std::span<const uint8_t> buffer)
// {
//     PARSE_MSG(faabric::EmptyRequest, buffer.data(), buffer.size())
//     auto instancesLoads =
//     faabric::scheduler::getScheduler().statsLocalLoad();

//     faabric::InstancesLoadState response;
//     auto* loadMap = response.mutable_instancesload();
//     for (const auto& [instanceName, instanceLoad] : instancesLoads) {
//         (*loadMap)[instanceName] = instanceLoad;
//     }

//     return std::make_unique<faabric::InstancesLoadState>(response);
// }

void logRuntimeStatsUpdateRequest(const faabric::RuntimeStatsUpdateRequest& req)
{
    std::ostringstream oss;
    oss << "RuntimeStatsUpdateRequest:\n";

    int numResults = req.collectedstats_size();
    oss << "Number of collected results: " << numResults << "\n";

    for (int i = 0; i < numResults; i++) {
        const auto& result = req.collectedstats(i);
        oss << "Result [" << i << "]:\n";
        oss << "  Host: " << result.host() << "\n";

        int numInstances = result.instancesstats_size();
        oss << "  Number of instance stats: " << numInstances << "\n";

        for (int j = 0; j < numInstances; j++) {
            const auto& instance = result.instancesstats(j);
            oss << "    Instance [" << j << "]:\n";
            oss << "      Instance Name: " << instance.instancename() << "\n";
            oss << "      Executed Count: " << instance.executedcount() << "\n";
            oss << "      Chained Call Count: " << instance.chainedcallcount()
                << "\n";

            // Log the sourceStats map.
            oss << "      sourceStats: { ";
            for (const auto& entry : instance.sourcestats()) {
                oss << entry.first << ": " << entry.second << " ";
            }
            oss << "}\n";

            // Log the chainedCallStats map.
            oss << "      chainedCallStats: { ";
            for (const auto& entry : instance.chainedcallstats()) {
                oss << entry.first << ": " << entry.second << " ";
            }
            oss << "}\n";
        }
    }

    spdlog::debug("{}", oss.str());
}

std::unique_ptr<google::protobuf::Message>
FunctionCallServer::recvGetRuntimeStats(std::span<const uint8_t> buffer)
{
    PARSE_MSG(faabric::RuntimeStatsUpdateRequest, buffer.data(), buffer.size())

    // Collect the stats
    auto stats = faabric::scheduler::getScheduler().getRuntimeStats();

    faabric::RuntimeStatsResult response;
    response.set_host(faabric::util::getSystemConfig().endpointHost);

    for (const auto& [instanceName, instanceStats] : stats) {
        faabric::InstanceStatsPayload* payload = response.add_instancesstats();
        payload->set_instancename(instanceName);
        payload->set_executedcount(instanceStats.executedCount);

        for (const auto& [source, count] : instanceStats.sourceStats) {
            (*payload->mutable_sourcestats())[source] = count;
        }

        payload->set_chainedcallcount(instanceStats.chainedCallCount);

        for (const auto& [dest, count] : instanceStats.chainedCallStats) {
            (*payload->mutable_chainedcallstats())[dest] = count;
        }
    }

    // Update the new collected stats to scheduler
    // Get the source stats.
    // logRuntimeStatsUpdateRequest(parsedMsg);
    std::map<std::string, std::map<std::string, int>> sourceCountStats;
    for (const auto& result : parsedMsg.collectedstats()) {
        std::string hostIp = result.host();
        for (const auto& instance : result.instancesstats()) {
            std::string instanceName = instance.instancename();
            int chainedCallCount = instance.chainedcallcount();
            sourceCountStats[instanceName][hostIp] = chainedCallCount;
        }
    }

    scheduler.updateStatelessDist(sourceCountStats);

    return std::make_unique<faabric::RuntimeStatsResult>(std::move(response));
}

void FunctionCallServer::recvExecuteFunctions(std::span<const uint8_t> buffer)
{
    PARSE_MSG(faabric::BatchExecuteRequest, buffer.data(), buffer.size())

    SPDLOG_ERROR("recvExecuteFunctions is not supported");
    throw std::runtime_error("recvExecuteFunctions is not supported");
}

void FunctionCallServer::recvExecuteFunctionsBatch(
  std::span<const uint8_t> buffer)
{
    PARSE_MSG(faabric::MessageBatch, buffer.data(), buffer.size())

    SPDLOG_DEBUG("Batch execute call Batch size: {}",
                 parsedMsg.messages_size());

    scheduler.enqueueMessageBatch(
      std::make_unique<faabric::MessageBatch>(parsedMsg));
}

void FunctionCallServer::recvSetMessageResult(std::span<const uint8_t> buffer)
{
    PARSE_MSG(faabric::Message, buffer.data(), buffer.size())
    faabric::planner::getPlannerClient().setMessageResultLocally(
      std::make_shared<faabric::Message>(parsedMsg));
}

void FunctionCallServer::recvResetParameter(std::span<const uint8_t> buffer)
{
    PARSE_MSG(faabric::planner::ResetStreamParameterRequest,
              buffer.data(),
              buffer.size());
    std::string key = parsedMsg.parameter();
    int32_t value = parsedMsg.value();
    SPDLOG_INFO("FunctionCall Server Resetting parameter {} to {}", key, value);
    if (key == "is_repartition") {
        faabric::scheduler::getScheduler().resetParameter(key, value);
    } else if (key == "max_executors") {
        faabric::scheduler::getScheduler().resetParameter(key, value);
    } else if (key == "planner_call_interval") {
        faabric::scheduler::getScheduler().resetParameter(key, value);
    } else if (key == "batch_size") {
        faabric::scheduler::getScheduler().resetParameter(key, value);
    } else if (key == "max_replicas") {
        faabric::scheduler::getScheduler().resetParameter(key, value);
    } else if (key == "schedule_mode") {
        faabric::scheduler::getScheduler().resetParameter(key, value);
    } else {
        throw std::runtime_error(
          fmt::format("Unrecognized parameter key: {}", key));
    }
}

void FunctionCallServer::recvSetPersistentState(std::span<const uint8_t> buffer)
{
    PARSE_MSG(faabric::planner::MapMessage, buffer.data(), buffer.size())
    const auto& protoMap = parsedMsg.payload();

    std::map<std::string, std::string> kvMap;
    for (const auto& entry : protoMap) {
        // SPDLOG_DEBUG("Setting persistent state: {} -> {}", entry.first,
        //              entry.second);
        kvMap.emplace(entry.first, entry.second);
    }

    faabric::scheduler::getScheduler().setLocalPersistentState(kvMap);
}

}
