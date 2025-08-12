#pragma once

#include <faabric/proto/faabric.pb.h>
#include <faabric/scheduler/FunctionCallApi.h>
#include <faabric/scheduler/Scheduler.h>
#include <faabric/transport/MessageEndpointServer.h>

#include <list>

namespace faabric::scheduler {
class FunctionCallServer final
  : public faabric::transport::MessageEndpointServer
{
  public:
    FunctionCallServer();

  private:
    Scheduler& scheduler;

    void doAsyncRecv(transport::Message& message) override;

    std::unique_ptr<google::protobuf::Message> doSyncRecv(
      transport::Message& message) override;

    std::unique_ptr<google::protobuf::Message> recvFlush(
      std::span<const uint8_t> buffer);

    std::unique_ptr<google::protobuf::Message> recvSyncStatesInfo(
      std::span<const uint8_t> buffer);

    std::unique_ptr<google::protobuf::Message> recvMigrateStates(
      std::span<const uint8_t> buffer);

    // std::unique_ptr<google::protobuf::Message> recvGetWorkerLoad(
    //   std::span<const uint8_t> buffer);

    std::unique_ptr<google::protobuf::Message> recvGetRuntimeStats(
      std::span<const uint8_t> buffer);

    std::unique_ptr<google::protobuf::Message> recvGetWorkerStats(
      std::span<const uint8_t> buffer);

    std::unique_ptr<google::protobuf::Message> recvCustom(
      std::span<const uint8_t> buffer);

    std::unique_ptr<google::protobuf::Message> recvGetPersistentState(
      std::span<const uint8_t> buffer);

    std::unique_ptr<google::protobuf::Message> recvSetPersistentState(
      std::span<const uint8_t> buffer);

    void recvRegisterApplication(std::span<const uint8_t> buffer);

    void recvExecuteFunctions(std::span<const uint8_t> buffer);

    void recvSetMessageResult(std::span<const uint8_t> buffer);

    void recvResetParameter(std::span<const uint8_t> buffer);

    void recvExecuteFunctionsBatch(std::span<const uint8_t> buffer);
};
}
