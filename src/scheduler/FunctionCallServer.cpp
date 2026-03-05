#include <faabric/executor/ExecutorFactory.h>
#include <faabric/scheduler/FunctionCallServer.h>
#include <faabric/snapshot/SnapshotRegistry.h>
#include <faabric/state/State.h>
#include <faabric/transport/common.h>
#include <faabric/transport/macros.h>
#include <faabric/util/config.h>
#include <faabric/util/func.h>
#include <faabric/util/logging.h>
#include <faabric/util/message.h>

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
        case faabric::scheduler::FunctionCalls::RegisterApplication: {
            recvRegisterApplication(message.udata());
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
        case faabric::scheduler::FunctionCalls::GetPersistentState: {
            return recvGetPersistentState(message.udata());
        }
        case faabric::scheduler::FunctionCalls::SetPersistentState: {
            return recvSetPersistentState(message.udata());
        }
        case faabric::scheduler::FunctionCalls::GetRuntimeStats: {
            return recvGetRuntimeStats(message.udata());
        }
        case faabric::scheduler::FunctionCalls::GetWorkerStats: {
            return recvGetWorkerStats(message.udata());
        }
        case faabric::scheduler::FunctionCalls::Custom: {
            return recvCustom(message.udata());
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

    auto scheuduledOperatorMap =
      faabric::util::parseScheduledOperatorMap(parsedMsg);

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

    int migrationVersion = parsedMsg.migrationversion();
    bool isInitialization = parsedMsg.is_initialize();

    std::map<std::string, std::set<std::string>> transDestinationMap;
    for (const auto& [ip, hostList] : parsedMsg.transdestinationmap()) {
        std::set<std::string> hosts;
        for (const auto& host : hostList.hosts()) {
            hosts.emplace(host);
        }
        transDestinationMap[ip] = std::move(hosts);
    }

    std::map<std::string, std::set<std::string>> transSourceMap;
    for (const auto& [ip, hostList] : parsedMsg.transsourcemap()) {
        std::set<std::string> hosts;
        for (const auto& host : hostList.hosts()) {
            hosts.emplace(host);
        }
        transSourceMap[ip] = std::move(hosts);
    }

    scheduler.updateStatesInfo(scheuduledOperatorMap,
                               tempStatesInfoMap,
                               migrationVersion,
                               isInitialization,
                               transDestinationMap,
                               transSourceMap);

    return std::make_unique<planner::SyncStatesInfoResponse>();
}

std::unique_ptr<google::protobuf::Message>
FunctionCallServer::recvMigrateStates(std::span<const uint8_t> buffer)
{
    SPDLOG_DEBUG("RECEIVE migrating state and relevant messages");

    PARSE_MSG(faabric::StateMigrationRequest, buffer.data(), buffer.size())

    const std::string& source = parsedMsg.sourcehost();

    std::ostringstream oss;
    for (const auto& state : parsedMsg.migratestates()) {
        oss << state.userfuncpar() << ":" << state.serializedstate().size()
            << "/";
    }

    SPDLOG_DEBUG("Migrating {} states from host {} to local host {}: {}",
                 parsedMsg.migratestates_size(),
                 source,
                 faabric::util::getSystemConfig().endpointHost,
                 oss.str());

    scheduler.processMigrationData(parsedMsg);

    return std::make_unique<faabric::EmptyResponse>();
}

std::unique_ptr<google::protobuf::Message> FunctionCallServer::recvCustom(
  std::span<const uint8_t> buffer)
{
    PARSE_MSG(faabric::CustomRequest, buffer.data(), buffer.size());
    std::string payload = parsedMsg.payload();
    SPDLOG_DEBUG("Received custom request with payload: {}", payload);
    if (payload == "flush_state") {
        scheduler.flushState();
    } else {
        SPDLOG_ERROR("Unrecognized custom request payload: {}", payload);
        throw std::runtime_error(
          fmt::format("Unrecognized custom request payload"));
    }

    return std::make_unique<faabric::EmptyResponse>();
}

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
    int totalCount = 0;
    std::map<std::string, std::map<std::string, int>> observedCountMap;
    for (const auto& result : parsedMsg.collectedstats()) {
        std::string hostIp = result.host();
        for (const auto& instance : result.instancesstats()) {
            std::string instanceName = instance.instancename();
            int executedCount = instance.executedcount();
            observedCountMap[instanceName][hostIp] = executedCount;
            totalCount += executedCount;
        }
    }

    if (totalCount > 0) {
        scheduler.updateStatelessDist(observedCountMap);
    }

    return std::make_unique<faabric::RuntimeStatsResult>(std::move(response));
}

std::unique_ptr<google::protobuf::Message>
FunctionCallServer::recvGetWorkerStats(std::span<const uint8_t> buffer)
{
    PARSE_MSG(faabric::EmptyRequest, buffer.data(), buffer.size())

    SPDLOG_DEBUG("Getting worker stats for host");
    auto snapshot = scheduler.getCpuRecordHistory();
    auto maxReplicas = scheduler.getMaxReplicasMap();
    auto migrationHistory = scheduler.getMigrationHistory();

    WorkerStats out;
    out.set_ip(faabric::util::getSystemConfig().endpointHost);

    while (!snapshot.empty()) {
        auto [execPct, schedPct] = snapshot.front();
        snapshot.pop();

        auto* rec = out.add_history();
        rec->set_cpuexecutepct(execPct);
        rec->set_cpuschedulepct(schedPct);
    }

    for (const auto& [instanceName, count] : maxReplicas) {
        auto* rec = out.add_instancereplicas();
        rec->set_instancename(instanceName);
        rec->set_replicas(count);
    }

    auto* protoMigrationHistory = out.mutable_migrationhistory();
    for (const auto& [version, duration] : migrationHistory) {
        (*protoMigrationHistory)[version] = duration;
    }

    return std::make_unique<faabric::WorkerStats>(std::move(out));
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

    SPDLOG_DEBUG("Batch execute call received batch size: {}",
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
    faabric::scheduler::getScheduler().resetParameter(key, value);
}

void FunctionCallServer::recvRegisterApplication(
  std::span<const uint8_t> buffer)
{
    PARSE_MSG(faabric::planner::RegisterApplicationRequest,
              buffer.data(),
              buffer.size())
    auto applicationPtr = faabric::util::parseApplicationMsg(parsedMsg);

    faabric::scheduler::getScheduler().registerApp(std::move(applicationPtr));
}

std::unique_ptr<google::protobuf::Message>
FunctionCallServer::recvGetPersistentState(std::span<const uint8_t> buffer)
{
    PARSE_MSG(faabric::planner::MapMessage, buffer.data(), buffer.size())
    const auto& protoMap = parsedMsg.payload();

    std::map<std::string, std::string> kvMap;
    for (const auto& entry : protoMap) {
        kvMap.emplace(entry.first, entry.second);
    }

    std::string key = kvMap["key"];

    std::string value =
      faabric::scheduler::getScheduler().getLocalPersistentState(key);

    faabric::planner::MapMessage response;
    response.mutable_payload()->insert({ key, value });
    SPDLOG_DEBUG("Getting persistent state: {} -> {}", key, value);
    return std::make_unique<faabric::planner::MapMessage>(std::move(response));
}

std::unique_ptr<google::protobuf::Message>
FunctionCallServer::recvSetPersistentState(std::span<const uint8_t> buffer)
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

    return std::make_unique<faabric::EmptyResponse>();
}

}
