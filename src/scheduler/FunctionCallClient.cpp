#include <faabric/proto/faabric.pb.h>
#include <faabric/scheduler/FunctionCallClient.h>
#include <faabric/transport/common.h>
#include <faabric/transport/macros.h>
#include <faabric/util/config.h>
#include <faabric/util/queue.h>
#include <faabric/util/testing.h>

namespace faabric::scheduler {

// -----------------------------------
// Mocking
// -----------------------------------

static std::mutex mockMutex;

static std::vector<std::pair<std::string, faabric::Message>> functionCalls;

static std::vector<std::pair<std::string, faabric::EmptyRequest>> flushCalls;

static std::vector<
  std::pair<std::string, std::shared_ptr<faabric::BatchExecuteRequest>>>
  batchMessages;

static std::vector<std::pair<std::string, std::shared_ptr<faabric::Message>>>
  messageResults;

std::vector<std::pair<std::string, faabric::Message>> getFunctionCalls()
{
    faabric::util::UniqueLock lock(mockMutex);
    return functionCalls;
}

std::vector<std::pair<std::string, faabric::EmptyRequest>> getFlushCalls()
{
    faabric::util::UniqueLock lock(mockMutex);
    return flushCalls;
}

std::vector<
  std::pair<std::string, std::shared_ptr<faabric::BatchExecuteRequest>>>
getBatchRequests()
{
    faabric::util::UniqueLock lock(mockMutex);
    return batchMessages;
}

std::vector<std::pair<std::string, std::shared_ptr<faabric::Message>>>
getMessageResults()
{
    faabric::util::UniqueLock lock(mockMutex);
    return messageResults;
}

void clearMockRequests()
{
    faabric::util::UniqueLock lock(mockMutex);
    functionCalls.clear();
    batchMessages.clear();
    messageResults.clear();
}

// -----------------------------------
// Function Call Client
// -----------------------------------

FunctionCallClient::FunctionCallClient(const std::string& hostIn)
  : faabric::transport::MessageEndpointClient(hostIn,
                                              FUNCTION_CALL_ASYNC_PORT,
                                              FUNCTION_CALL_SYNC_PORT)
{}

void FunctionCallClient::sendFlush()
{
    faabric::EmptyRequest req;
    if (faabric::util::isMockMode()) {
        faabric::util::UniqueLock lock(mockMutex);
        flushCalls.emplace_back(host, req);
    } else {
        faabric::EmptyResponse resp;
        syncSend(faabric::scheduler::FunctionCalls::Flush, &req, &resp);
    }
}

void FunctionCallClient::executeFunctions(
  const std::shared_ptr<faabric::BatchExecuteRequest> req)
{
    if (faabric::util::isMockMode()) {
        faabric::util::UniqueLock lock(mockMutex);
        batchMessages.emplace_back(host, req);
    } else if (faabric::util::getSystemConfig().batchProcess == "on") {
        asyncSend(faabric::scheduler::FunctionCalls::ExecuteFunctionsLazy,
                  req.get());
    } else {
        asyncSend(faabric::scheduler::FunctionCalls::ExecuteFunctions,
                  req.get());
    }
}

void FunctionCallClient::setMessageResult(std::shared_ptr<faabric::Message> msg)
{
    if (faabric::util::isMockMode()) {
        faabric::util::UniqueLock lock(mockMutex);
        messageResults.emplace_back(host, msg);
    } else {
        asyncSend(faabric::scheduler::FunctionCalls::SetMessageResult,
                  msg.get());
    }
}

void FunctionCallClient::resetBatchSize(
  std::shared_ptr<faabric::planner::BatchResetRequest> req)
{
    asyncSend(faabric::scheduler::FunctionCalls::ResetBatchsize, req.get());
}

void FunctionCallClient::resetMaxReplicas(
  std::shared_ptr<faabric::planner::MaxReplicasRequest> req)
{
    asyncSend(faabric::scheduler::FunctionCalls::ResetMaxReplicas, req.get());
}

void FunctionCallClient::resetParameter(
  std::shared_ptr<faabric::planner::ResetStreamParameterRequest> req)
{
    asyncSend(faabric::scheduler::FunctionCalls::ResetParameter, req.get());
}

void FunctionCallClient::executeFunctionsBatch(
  std::list<std::unique_ptr<faabric::Message>>&& msgs)
{
    // Formulate a batch execute request Batch
    auto batchMsgsList = std::make_shared<faabric::MessageBatch>();
    SPDLOG_DEBUG("Batch execute call Batch size: {}", msgs.size());
    for (auto& msg : msgs) {
        batchMsgsList->add_messages()->CopyFrom(*msg);
    }
    asyncSend(faabric::scheduler::FunctionCalls::ExecuteFunctionsBatch,
              batchMsgsList.get());
}

// -----------------------------------
// Static setter/getters
// -----------------------------------

static faabric::util::ConcurrentMap<
  std::string,
  std::shared_ptr<faabric::scheduler::FunctionCallClient>>
  functionCallClients;

std::shared_ptr<FunctionCallClient> getFunctionCallClient(
  const std::string& otherHost)
{
    auto client = functionCallClients.get(otherHost).value_or(nullptr);
    if (client == nullptr) {
        SPDLOG_DEBUG("Adding new function call client for {}", otherHost);
        client =
          functionCallClients.tryEmplaceShared(otherHost, otherHost).second;
    }
    return client;
}

void clearFunctionCallClients()
{
    functionCallClients.clear();
}
}
