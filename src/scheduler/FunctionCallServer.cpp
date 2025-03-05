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
        case faabric::scheduler::FunctionCalls::ExecuteFunctionsLazy: {
            recvExecuteFunctionsLazy(message.udata());
            break;
        }
        case faabric::scheduler::FunctionCalls::SetMessageResult: {
            recvSetMessageResult(message.udata());
            break;
        }
        case faabric::scheduler::FunctionCalls::ResetBatchsize: {
            recvResetBatchsize(message.udata());
            break;
        }
        case faabric::scheduler::FunctionCalls::ResetMaxReplicas: {
            recvResetMaxReplicas(message.udata());
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

void FunctionCallServer::recvExecuteFunctions(std::span<const uint8_t> buffer)
{
    PARSE_MSG(faabric::BatchExecuteRequest, buffer.data(), buffer.size())

    // This host has now been told to execute these functions no matter what
    for (int i = 0; i < parsedMsg.messages_size(); i++) {
        parsedMsg.mutable_messages()->at(i).set_starttimestamp(
          faabric::util::getGlobalClock().epochMillis());
        parsedMsg.mutable_messages()->at(i).set_executedhost(
          faabric::util::getSystemConfig().endpointHost);
    }

    scheduler.executeBatch(
      std::make_shared<faabric::BatchExecuteRequest>(parsedMsg));
}

void FunctionCallServer::recvExecuteFunctionsLazy(
  std::span<const uint8_t> buffer)
{
    PARSE_MSG(faabric::BatchExecuteRequest, buffer.data(), buffer.size())

    // TODO - we can set the queue time here
    // This host has now been told to execute these functions no matter what
    // For WAMR, start time stamp is twice. It will be set again in
    // WasmModlue.cpp
    for (int i = 0; i < parsedMsg.messages_size(); i++) {
        parsedMsg.mutable_messages()->at(i).set_starttimestamp(
          faabric::util::getGlobalClock().epochMillis());
        parsedMsg.mutable_messages()->at(i).set_executedhost(
          faabric::util::getSystemConfig().endpointHost);
    }

    scheduler.executeBatchAsyn(
      std::make_shared<faabric::BatchExecuteRequest>(parsedMsg));
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

void FunctionCallServer::recvResetBatchsize(std::span<const uint8_t> buffer)
{
    PARSE_MSG(
      faabric::planner::BatchResetRequest, buffer.data(), buffer.size());
    int32_t batchSize = parsedMsg.batchsize();
    SPDLOG_INFO("Resetting batch size to {}", batchSize);
    faabric::scheduler::getScheduler().resetBatchsize(batchSize);
}

void FunctionCallServer::recvResetMaxReplicas(std::span<const uint8_t> buffer)
{
    PARSE_MSG(
      faabric::planner::MaxReplicasRequest, buffer.data(), buffer.size());
    int32_t maxReplicas = parsedMsg.maxnum();
    SPDLOG_INFO("Resetting max replicas to {}", maxReplicas);
    faabric::scheduler::getScheduler().resetMaxReplicas(maxReplicas);
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
    } else {
        throw std::runtime_error(
          fmt::format("Unrecognized parameter key: {}", key));
    }
}

}
