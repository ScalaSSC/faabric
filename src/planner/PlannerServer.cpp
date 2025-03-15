#include <faabric/planner/PlannerApi.h>
#include <faabric/planner/PlannerServer.h>
#include <faabric/planner/planner.pb.h>
#include <faabric/transport/common.h>
#include <faabric/transport/macros.h>
#include <faabric/util/config.h>
#include <faabric/util/logging.h>
#include <faabric/util/ptp.h>

#include <fmt/format.h>

namespace faabric::planner {
PlannerServer::PlannerServer()
  : faabric::transport::MessageEndpointServer(
      PLANNER_ASYNC_PORT,
      PLANNER_SYNC_PORT,
      PLANNER_INPROC_LABEL,
      getPlanner().getConfig().numthreadshttpserver())
  , planner(getPlanner())
{}

void PlannerServer::doAsyncRecv(transport::Message& message)
{
    uint8_t header = message.getMessageCode();
    switch (header) {
        case PlannerCalls::SetMessageResult: {
            recvSetMessageResult(message.udata());
            break;
        }
        case PlannerCalls::SetMessageResultBatch: {
            recvSetMessageResultBatch(message.udata());
            break;
        }
        default: {
            // If we don't recognise the header, let the client fail, but don't
            // crash the planner
            SPDLOG_ERROR("Unrecognised async call header: {}", header);
        }
    }
}

std::unique_ptr<google::protobuf::Message> PlannerServer::doSyncRecv(
  transport::Message& message)
{
    uint8_t header = message.getMessageCode();
    switch (header) {
        case PlannerCalls::Ping: {
            return recvPing();
        }
        case PlannerCalls::GetAvailableHosts: {
            return recvGetAvailableHosts();
        }
        case PlannerCalls::RegisterHost: {
            return recvRegisterHost(message.udata());
        }
        case PlannerCalls::RemoveHost: {
            return recvRemoveHost(message.udata());
        }
        case PlannerCalls::GetMessageResult: {
            return recvGetMessageResult(message.udata());
        }
        case PlannerCalls::GetBatchResults: {
            return recvGetBatchResults(message.udata());
        }
        case PlannerCalls::GetSchedulingDecision: {
            return recvGetSchedulingDecision(message.udata());
        }
        case PlannerCalls::GetNumMigrations: {
            return recvGetNumMigrations(message.udata());
        }
        case PlannerCalls::PreloadSchedulingDecision: {
            return recvPreloadSchedulingDecision(message.udata());
        }
        case PlannerCalls::CallBatch: {
            return recvCallBatch(message.udata());
        }
        case PlannerCalls::EnqueueBatch: {
            return recvEnqueueBatch(message.udata());
        }
        default: {
            // If we don't recognise the header, let the client fail, but don't
            // crash the planner
            SPDLOG_ERROR("Unrecognised sync call header: {}", header);
            return std::make_unique<faabric::EmptyResponse>();
        }
    }
}

std::unique_ptr<google::protobuf::Message> PlannerServer::recvPing()
{
    // Pong
    auto response = std::make_unique<faabric::planner::PingResponse>();
    *response->mutable_config() = planner.getConfig();

    return std::move(response);
}

std::unique_ptr<google::protobuf::Message>
PlannerServer::recvGetAvailableHosts()
{
    auto response = std::make_unique<AvailableHostsResponse>();

    auto availableHosts = planner.getAvailableHosts();

    for (auto host : availableHosts) {
        *response->add_hosts() = *host;
    }

    return std::move(response);
}

std::unique_ptr<google::protobuf::Message> PlannerServer::recvRegisterHost(
  std::span<const uint8_t> buffer)
{
    PARSE_MSG(RegisterHostRequest, buffer.data(), buffer.size());

    bool success =
      planner.registerHost(parsedMsg.host(), parsedMsg.overwrite());
    if (!success) {
        SPDLOG_ERROR("Error registering host in Planner!");
    }

    auto response = std::make_unique<faabric::planner::RegisterHostResponse>();
    *response->mutable_config() = planner.getConfig();
    // Prepare host update and state update flags
    response->set_hostsync(false);

    // Set response status
    ResponseStatus status;
    if (success) {
        status.set_status(ResponseStatus_Status_OK);
    } else {
        status.set_status(ResponseStatus_Status_ERROR);
    }
    *response->mutable_status() = status;

    // Add registered host information
    auto [hostSync, hostPtrMap] =
      planner.getRegisteredHost(parsedMsg.host().ip());
    if (hostSync) {
        for (const auto& [host, hostPtr] : hostPtrMap) {
            response->add_registeredhosts()->set_ip(host);
        }
        response->set_hostsync(true);
    }

    return response;
}

std::unique_ptr<google::protobuf::Message> PlannerServer::recvRemoveHost(
  std::span<const uint8_t> buffer)
{
    PARSE_MSG(RemoveHostRequest, buffer.data(), buffer.size());

    // Removing a host is a best-effort operation, we just try to remove it if
    // we find it
    planner.removeHost(parsedMsg.host());

    return std::make_unique<faabric::EmptyResponse>();
}

void PlannerServer::recvSetMessageResult(std::span<const uint8_t> buffer)
{
    SPDLOG_ERROR("SetMessageResult not implemented in PlannerServer");
    throw std::runtime_error(
      "SetMessageResult not implemented in PlannerServer");
}

void PlannerServer::recvSetMessageResultBatch(std::span<const uint8_t> buffer)
{
    PARSE_MSG(BatchExecuteRequest, buffer.data(), buffer.size());
    planner.setMessageResultBatch(
      std::make_shared<faabric::BatchExecuteRequest>(parsedMsg));
}

std::unique_ptr<google::protobuf::Message> PlannerServer::recvGetMessageResult(
  std::span<const uint8_t> buffer)
{
    SPDLOG_ERROR("GetMessageResult not implemented in PlannerServer");
    throw std::runtime_error("GetMessageResult not implemented in PlannerServer");

    return std::make_unique<faabric::Message>();
}

std::unique_ptr<google::protobuf::Message> PlannerServer::recvGetBatchResults(
  std::span<const uint8_t> buffer)
{
    PARSE_MSG(BatchExecuteRequest, buffer.data(), buffer.size());
    auto req = std::make_shared<faabric::BatchExecuteRequest>(parsedMsg);

    auto berStatus = planner.getBatchResults(req->appid());

    if (berStatus == nullptr) {
        return std::make_unique<BatchExecuteRequestStatus>();
    }

    return std::make_unique<BatchExecuteRequestStatus>(*berStatus);
}

std::unique_ptr<google::protobuf::Message>
PlannerServer::recvGetSchedulingDecision(std::span<const uint8_t> buffer)
{
    PARSE_MSG(BatchExecuteRequest, buffer.data(), buffer.size());
    auto req = std::make_shared<faabric::BatchExecuteRequest>(parsedMsg);

    auto decision = planner.getSchedulingDecision(req);

    // If the app is not registered in-flight, return an empty mapping
    if (decision == nullptr) {
        return std::make_unique<faabric::PointToPointMappings>();
    }

    // Build PointToPointMappings from scheduling decision
    faabric::PointToPointMappings mappings =
      faabric::util::ptpMappingsFromSchedulingDecision(decision);

    return std::make_unique<faabric::PointToPointMappings>(mappings);
}

std::unique_ptr<google::protobuf::Message> PlannerServer::recvGetNumMigrations(
  std::span<const uint8_t> buffer)
{
    NumMigrationsResponse response;
    response.set_nummigrations(planner.getNumMigrations());

    return std::make_unique<NumMigrationsResponse>(response);
}

std::unique_ptr<google::protobuf::Message>
PlannerServer::recvPreloadSchedulingDecision(std::span<const uint8_t> buffer)
{   
    SPDLOG_ERROR("PreloadSchedulingDecision not implemented in PlannerServer");
    return std::make_unique<faabric::EmptyResponse>();
}

std::unique_ptr<google::protobuf::Message> PlannerServer::recvCallBatch(
  std::span<const uint8_t> buffer)
{
    SPDLOG_ERROR("CallBatch not implemented in PlannerServer");
    throw std::runtime_error("CallBatch not implemented in PlannerServer");
    return nullptr;
}

std::unique_ptr<google::protobuf::Message> PlannerServer::recvEnqueueBatch(
  std::span<const uint8_t> buffer)
{
    SPDLOG_ERROR("EnqueueBatch not implemented in DecentralizedPlannerServer");
    throw std::runtime_error("EnqueueBatch not implemented in PlannerServer");
    return nullptr;
}

}
