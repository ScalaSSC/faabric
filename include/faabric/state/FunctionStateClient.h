#pragma once

#include <faabric/proto/faabric.pb.h>
#include <faabric/state/FunctionStateRegistry.h>
#include <faabric/state/State.h>
#include <faabric/transport/MessageEndpoint.h>
#include <faabric/transport/MessageEndpointClient.h>

namespace faabric::state {
class FunctionStateClient : public faabric::transport::MessageEndpointClient
{
  public:
    explicit FunctionStateClient(const std::string& userIn,
                                 const std::string& funcIn,
                                 const int parallelismIdIn,
                                 const std::string& hostIn);
    
    FunctionStateClient(const std::string& hostIn);

    const std::string user;
    const std::string func;
    const int parallelismId;

    size_t stateSize(bool lock = false);

    void lock();

    void unlock();

    void pushChunks(const std::vector<StateChunk>& chunks,
                    uint32_t stateSize,
                    bool unlock = false,
                    std::string parititonKey = "");

    void pullChunks(const std::vector<StateChunk>& chunks,
                    uint8_t* bufferStart);

    void rePartitionState(const std::string& newStateHost);

    void addPartitionState(const std::string& pstatekey,
                           const std::vector<uint8_t>& data);

    void combineParState();

    bool createState(std::string stateKey);

  private:
    // void sendStateRequest(faabric::state::FunctionStateCalls header,
    //                       const uint8_t* data,
    //                       int length);

    void logRequest(const std::string& op);
};
}
