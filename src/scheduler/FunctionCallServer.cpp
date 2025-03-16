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
    SPDLOG_INFO("Syncing state info to host {}",
                faabric::util::getSystemConfig().endpointHost);

    PARSE_MSG(planner::SyncStatesInfoRequest, buffer.data(), buffer.size())
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

    scheduler.updateStatesInfo(tempStatesInfoMap);

    return std::make_unique<planner::SyncStatesInfoResponse>();
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
    } else if (key == "decentral_schedule_mode") {
        faabric::scheduler::getScheduler().resetParameter(key, value);
    } else {
        throw std::runtime_error(
          fmt::format("Unrecognized parameter key: {}", key));
    }
}

}
