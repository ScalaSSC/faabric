#include <faabric/state/FunctionStateClient.h>
#include <faabric/transport/common.h>
#include <faabric/transport/macros.h>
#include <faabric/util/logging.h>
#include <faabric/util/macros.h>

namespace faabric::state {

FunctionStateClient::FunctionStateClient(const std::string& userIn,
                                         const std::string& funcIn,
                                         const int parallelismIdIn,
                                         const std::string& hostIn)
  : faabric::transport::MessageEndpointClient(hostIn,
                                              STATE_ASYNC_PORT,
                                              STATE_SYNC_PORT)
  , user(userIn)
  , func(funcIn)
  , parallelismId(parallelismIdIn)
{}

FunctionStateClient::FunctionStateClient(const std::string& hostIn)
  : FunctionStateClient("unknown", "unknown", 0, hostIn)
{}

void FunctionStateClient::logRequest(const std::string& op)
{
    SPDLOG_TRACE(
      "Requesting {} on {}/{}-{} at {}", op, user, func, parallelismId, host);
}

size_t FunctionStateClient::stateSize(bool lock)
{
    logRequest("functionstate-size");

    faabric::FunctionStateRequest request;
    request.set_user(user);
    request.set_func(func);
    request.set_parallelismid(parallelismId);
    request.set_lock(lock);
    faabric::FunctionStateSizeResponse response;
    syncSend(faabric::state::StateCalls::FunctionSize, &request, &response);

    return response.statesize();
}

void FunctionStateClient::lock()
{
    logRequest("functionstate-lock");

    faabric::FunctionStateRequest request;
    request.set_user(user);
    request.set_func(func);
    request.set_parallelismid(parallelismId);
    faabric::EmptyResponse resp;
    syncSend(faabric::state::StateCalls::FunctionLock, &request, &resp);
}

void FunctionStateClient::unlock()
{

    logRequest("functionstate-unlock");

    faabric::FunctionStateRequest request;
    request.set_user(user);
    request.set_func(func);
    request.set_parallelismid(parallelismId);
    faabric::EmptyResponse resp;
    syncSend(faabric::state::StateCalls::FunctionUnlock, &request, &resp);
}

bool FunctionStateClient::createState(std::string stateKey)
{
    logRequest("functionstate-create");
    if (faabric::util::getSystemConfig().streamTestMode) {
        SPDLOG_DEBUG("STREAM TEST MODE skipped createState");
        return true;
    }
    faabric::FunctionStateTransferRequest request;
    request.set_user(user);
    request.set_func(func);
    request.set_parallelismid(parallelismId);
    request.set_isparition(false);
    if (!stateKey.empty()) {
        request.set_isparition(true);
        request.set_pstatekey(stateKey);
    }
    faabric::EmptyResponse resp;
    syncSend(faabric::state::StateCalls::FunctionCreate, &request, &resp);
    return true;
}

// TODO - Lock the data during pull and push

void FunctionStateClient::pullChunks(const std::vector<StateChunk>& chunks,
                                     uint8_t* bufferStart)
{
    logRequest("functionstate-pull-chunks");

    for (const auto& chunk : chunks) {
        // Prepare request
        faabric::FunctionStateChunkRequest request;
        request.set_user(user);
        request.set_func(func);
        request.set_parallelismid(parallelismId);
        request.set_offset(chunk.offset);
        request.set_chunksize(chunk.length);

        // Send request
        faabric::FunctionStatePart response;
        syncSend(faabric::state::StateCalls::FunctionPull, &request, &response);

        // Copy response data
        std::copy(response.data().begin(),
                  response.data().end(),
                  bufferStart + response.offset());
        // TODO - refine the size increase in pullChunks
    }
}

void FunctionStateClient::pushChunks(const std::vector<StateChunk>& chunks,
                                     uint32_t stateSize,
                                     bool unlock,
                                     std::string partitionKey)
{
    logRequest("functionstate-push-chunks");

    for (auto it = chunks.begin(); it != chunks.end(); ++it) {
        const auto& chunk = *it;

        faabric::FunctionStatePart stateChunk;
        stateChunk.set_user(user);
        stateChunk.set_func(func);
        stateChunk.set_parallelismid(parallelismId);
        stateChunk.set_offset(chunk.offset);
        stateChunk.set_data(chunk.data, chunk.length);
        stateChunk.set_statesize(stateSize);
        stateChunk.set_unlock(false);
        stateChunk.set_pstatekey(partitionKey);
        bool isLast = (std::next(it) == chunks.end());
        if (isLast) {
            stateChunk.set_unlock(unlock);
        }
        faabric::EmptyResponse resp;
        syncSend(faabric::state::StateCalls::FunctionPush, &stateChunk, &resp);
    }
}

void FunctionStateClient::rePartitionState(const std::string& newStateHost)
{
    if (faabric::util::getSystemConfig().streamTestMode) {
        SPDLOG_DEBUG("STREAM TEST MODE skipped rePartitionState");
        return;
    }
    logRequest("functionstate-repartition-state");
    faabric::FunctionStateRepartition request;
    request.set_user(user);
    request.set_func(func);
    request.set_parallelismid(parallelismId);
    request.set_newparitionmap(newStateHost);
    faabric::EmptyResponse resp;
    syncSend(faabric::state::StateCalls::FunctionRepartition, &request, &resp);
}

void FunctionStateClient::addPartitionState(const std::string& pstatekey,
                                            const std::vector<uint8_t>& data)
{

    if (faabric::util::getSystemConfig().streamTestMode) {
        SPDLOG_DEBUG("STREAM TEST MODE skipped addPartitionState");
        return;
    }
    logRequest("functionstate-addpartition-state");
    faabric::FunctionStateAdd request;
    request.set_user(user);
    request.set_func(func);
    request.set_parallelismid(parallelismId);
    request.set_pstatekey(pstatekey);
    request.set_data(data.data(), data.size());
    faabric::EmptyResponse resp;
    syncSend(faabric::state::StateCalls::FunctionParAdd, &request, &resp);
}

void FunctionStateClient::combineParState()
{
    logRequest("functionstate-combineParState");
    faabric::FunctionStateRequest request;
    request.set_user(user);
    request.set_func(func);
    request.set_parallelismid(parallelismId);
    faabric::EmptyResponse resp;
    syncSend(faabric::state::StateCalls::FunctionParCombine, &request, &resp);
}

}
