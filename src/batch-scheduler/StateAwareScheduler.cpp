#include <faabric/batch-scheduler/SchedulingDecision.h>
#include <faabric/batch-scheduler/StateAwareScheduler.h>
#include <faabric/state/FunctionStateClient.h>
#include <faabric/util/batch.h>
#include <faabric/util/logging.h>
// #include <faabric/util/map.h>
#include <faabric/util/serialization.h>
#include <faabric/util/string_tools.h>

#include <algorithm>
#include <limits>
#include <sstream>

#define MAIN_KEY_PREFIX "main_"

namespace faabric::batch_scheduler {

/* Virtual functions which must be implemented. However, it is not used in
 * StateAwareScheduler.
 */

std::shared_ptr<SchedulingDecision> StateAwareScheduler::makeSchedulingDecision(
  HostMap& hostMap,
  const InFlightReqs& inFlightReqs,
  std::shared_ptr<BatchExecuteRequest> req)
{
    auto decision = std::make_shared<SchedulingDecision>(req->appid(), 0);

    SPDLOG_ERROR(
      "makeSchedulingDecision function Not implemented in StateAwareScheduler");
    throw std::runtime_error(
      "makeSchedulingDecision function Not implemented in StateAwareScheduler");

    return decision;
}

bool StateAwareScheduler::isFirstDecisionBetter(
  std::shared_ptr<SchedulingDecision> decisionA,
  std::shared_ptr<SchedulingDecision> decisionB)
{
    SPDLOG_ERROR(
      "isFirstDecisionBetter function Not implemented in StateAwareScheduler");
    throw std::runtime_error(
      "isFirstDecisionBetter function Not implemented in StateAwareScheduler");
    return true;
}

std::vector<Host> StateAwareScheduler::getSortedHosts(
  HostMap& hostMap,
  const InFlightReqs& inFlightReqs,
  std::shared_ptr<faabric::BatchExecuteRequest> req,
  const DecisionType& decisionType)
{
    std::vector<Host> sortedHosts;
    SPDLOG_ERROR(
      "getSortedHosts function Not implemented in StateAwareScheduler");
    throw std::runtime_error(
      "getSortedHosts function Not implemented in StateAwareScheduler");
    return sortedHosts;
}

// ------------------------------------------
// The following functions are implemented in StateAwareScheduler
// ------------------------------------------

std::string to_string(const faabric::batch_scheduler::FunctionStateInfo& info)
{
    std::ostringstream oss;
    oss << "FunctionStateInfo {\n"
        << "  functionName: " << info.functionName << "\n"
        << "  partitionBy: " << info.partitionBy << "\n"
        << "  stateKey: " << info.stateKey << "\n"
        << "  parallelism: " << info.parallelism << "\n"
        << "  stateHost: {";

    bool first = true;
    for (const auto& entry : info.stateHost) {
        if (!first) {
            oss << ", ";
        }
        oss << entry.first << ": " << entry.second;
        first = false;
    }
    oss << "}\n}";
    return oss.str();
}

// Now, we only register the state to the Redis.
bool registerStateToRedis(const std::string& userFunctionParIdx,
                          const std::string& host)
{
    SPDLOG_INFO(
      "Registering state {} to host {} on Redis", userFunctionParIdx, host);
    // Update Redis Information
    redis::Redis& redis = redis::Redis::getState();
    std::string mainKey = MAIN_KEY_PREFIX + userFunctionParIdx;
    std::vector<uint8_t> mainIPBytes = faabric::util::stringToBytes(host);
    redis.set(mainKey, mainIPBytes);
    return true;
}

void deleteStateFromRedis(const std::string& userFunctionParIdx)
{
    SPDLOG_INFO("Deleting state {} from Redis", userFunctionParIdx);
    // Get the Redis instance
    redis::Redis& redis = redis::Redis::getState();
    // Construct the key using the same prefix as for registration
    std::string mainKey = MAIN_KEY_PREFIX + userFunctionParIdx;
    // Delete the key from Redis.
    // Assuming redis.del returns a bool indicating success.
    redis.del(mainKey);
}

// ------------------------------------------
// Base functions
// ------------------------------------------
unsigned int StateAwareScheduler::getNextCounter(const std::string& userFunc)
{
    // Grab—or create—the atomic counter under lock
    std::shared_ptr<std::atomic_uint> counterPtr;
    // 1) Try to find under a shared (reader) lock
    {
        faabric::util::SharedLock lock(counterMx);
        auto it = counterTable.find(userFunc);
        if (it != counterTable.end()) {
            counterPtr = it->second;
        }
    }

    // 2) If not found, take exclusive lock to insert
    if (!counterPtr) {
        faabric::util::FullLock lock(counterMx);
        // Double-check in case someone else inserted meanwhile
        auto& slot = counterTable[userFunc];
        if (!slot) {
            slot = std::make_shared<std::atomic_uint>(0u);
        }
        counterPtr = slot;
    }

    // 3) Finally, do the atomic increment (no lock needed here)
    return counterPtr->fetch_add(1u, std::memory_order_relaxed);
}

/*
HERE is the logic of registering function state to the host.
BEFORE COMPILE
write funcStateRegMap: records function state functions and their partition info
--- These two maps should never be changed after the initialization.
INITIALIZING
When stateful function is invoked first time. It would initialize the function
state with parallelism 1
Faasmctl stream.scale can change the parallelism:
When parallelism is not initlized, it only register new parallelism
Otherwise, it will increase the parallelism and repartition function state
*/

// We register the function state in the functionStateRegister map.

bool StateAwareScheduler::registerApp(
  std::unique_ptr<batch_scheduler::Application> app)
{
    faabric::util::FullLock lock(scheduleMx);

    SPDLOG_INFO("Planner received request to register application {}",
                app->getName());
    application = std::move(app);
    application->displayApplication();
    application->buildInvertConnections();
    return true;
}

void StateAwareScheduler::initApp(const HostMap& hostMap)
{
    faabric::util::FullLock lock(scheduleMx);

    // 1. Register the states
    for (auto& [nodeName, nodePtr] : application->getNodes()) {
        auto node = *nodePtr;
        if (node.type == batch_scheduler::NodeType::STATELESS) {
            continue;
        }
        registerFunctionState(node, hostMap);
    }
}

bool StateAwareScheduler::registerFunctionState(Node& node,
                                                const HostMap& hostMap)
{
    SPDLOG_INFO("Registering function state {} with partitioning by {}",
                node.name,
                node.partitionBy);

    std::string userFunction = node.name;
    std::string partitionBy = node.partitionBy;
    auto type = node.type;
    std::string stateKey;
    if (type == batch_scheduler::NodeType::PARTITIONED_STATEFUL) {
        stateKey = "Unknown";
    } else {
        stateKey = "None";
    }

    if (partitionBy == NONE_STRING || stateKey == NONE_STRING) {
        funcStateRegMap[userFunction] = std::make_tuple("", "");
    } else {
        funcStateRegMap[userFunction] = std::make_tuple(partitionBy, stateKey);
    }

    // Initialize the function state with parallelism 1, if not initialized.
    if (!functionParallelism.contains(userFunction)) {
        doRegisterState(hostMap, userFunction);
        if (node.parallelism >= 1) {
            updateFuncStatePar(userFunction, node.parallelism, hostMap);
        }
    }

    return false;
}

void StateAwareScheduler::doRegisterState(const HostMap& hostMap,
                                          std::string userFunc,
                                          int parallelism)
{
    SPDLOG_INFO("Create func {} with parallelism {}", userFunc, parallelism);
    if (parallelism != 1) {
        SPDLOG_ERROR("Parallelism is not 1, it is not supported now");
        return;
    }
    functionParallelism[userFunc] = 1;
    // The default parallelism is 1 and parallelism Idx is 0
    std::string funcParaId = userFunc + "_0";
    // Assign state to a host.
    int hostIdx =
      stateRbCounter.fetch_add(1, std::memory_order_relaxed) % hostMap.size();
    std::string host = faabric::util::getNthKey(hostMap, hostIdx);
    stateHost[funcParaId] = host;
    // If it is partitioned state, register it.
    std::string partitionBy = std::get<0>(funcStateRegMap[userFunc]);
    std::string stateKey = std::get<1>(funcStateRegMap[userFunc]);
    if (partitionBy != "" && stateKey != "") {
        statePartitionBy[userFunc] = partitionBy;
        stateHashRing[userFunc] =
          std::make_shared<faabric::util::ConsistentHashRing>(
            functionParallelism[userFunc]);
    }
    // Register the state to the host.
    registerStateToRedis(funcParaId, host);
}

bool StateAwareScheduler::updateFuncStatePar(const std::string& userFunction,
                                             int newPar,
                                             const HostMap& hostMap)
{
    SPDLOG_INFO("Scheduler update {} parallelism to {}", userFunction, newPar);

    if (functionParallelism.find(userFunction) == functionParallelism.end()) {
        SPDLOG_ERROR("Function {} is not stateful function", userFunction);
        return false;
    }

    if (newPar > functionParallelism[userFunction]) {
        increaseFuncStatePar(
          userFunction, newPar - functionParallelism[userFunction], hostMap);
        return true;
    }

    if (newPar < functionParallelism[userFunction]) {
        SPDLOG_ERROR("Decrease parallelism is not supported now.");
        throw std::runtime_error("Decrease parallelism is not supported now");
        return false;
    }

    return false;
}

// TODO - change it to increase or decrease function parallelism. It should
// return the old stateHost instead of the true/false
// This function is only used when initializing the function state.
// It not only update the function parallelism info in schueduler, but also
// update info in redis
void StateAwareScheduler::increaseFuncStatePar(const std::string& userFunction,
                                               int numIncrease,
                                               const HostMap& hostMap)
{
    SPDLOG_INFO("Increase {} parallelism for {}", numIncrease, userFunction);

    // Construct userFunctionIdx for new parallelism level
    for (int i = 0; i < numIncrease; i++) {
        int idx = functionParallelism[userFunction] + i;
        std::string userFunctionIdx = userFunction + "_" + std::to_string(idx);

        // Step 1: Count user-specific and total states for each host
        std::map<std::string, int> userFuncCount;
        std::map<std::string, int> totalStateCount;

        for (const auto& [host, _] : hostMap) {
            userFuncCount[host] = 0;
            totalStateCount[host] = 0;
        }

        for (const auto& [userFuncParallelism, host] : stateHost) {
            if (userFuncParallelism.find(userFunction + "_") !=
                std::string::npos) {
                userFuncCount[host]++;
            }
            totalStateCount[host]++;
        }

        // Step 2: Select host with minimum `userFuncCount` and tie-break with
        // `totalStateCount`
        std::string minHost;
        int minUserFuncCount = std::numeric_limits<int>::max();
        int minTotalCount = std::numeric_limits<int>::max();

        for (const auto& [host, _] : hostMap) {
            if ((userFuncCount[host] < minUserFuncCount) ||
                (userFuncCount[host] == minUserFuncCount &&
                 totalStateCount[host] < minTotalCount)) {
                minUserFuncCount = userFuncCount[host];
                minTotalCount = totalStateCount[host];
                minHost = host;
            }
        }

        // Check if a host was found
        if (minHost.empty()) {
            SPDLOG_ERROR("No host found for the new parallelism level");
            return;
        }

        SPDLOG_INFO(
          "Assigning new parallelism {} to {}", userFunctionIdx, minHost);

        // Step 3: Assign the selected host to the new parallelism level
        stateHost[userFunctionIdx] = minHost;

        // Step 4: Update Redis Information and register the state
        std::string partitionBy = std::get<0>(funcStateRegMap[userFunction]);
        std::string stateKey = std::get<1>(funcStateRegMap[userFunction]);
        registerStateToRedis(userFunction + "_" + std::to_string(idx), minHost);
    }

    functionParallelism[userFunction] += numIncrease;
    SPDLOG_INFO("New parallelism for {} is {}",
                userFunction,
                functionParallelism[userFunction]);

    // If the state is partitioned, update the Hash method.
    // It is ok to use default construct function. Because before running, the
    // weight for each parallelism are the same.
    if (statePartitionBy.contains(userFunction)) {
        // Change the state hashing ring
        stateHashRing[userFunction] =
          std::make_shared<faabric::util::ConsistentHashRing>(
            functionParallelism[userFunction]);
    }
}

std::string StateAwareScheduler::scheduleStatelessMessageRB(
  std::string& userFunc,
  const HostMap& hostMap,
  const std::unique_ptr<Message>& msg)
{
    auto counter = getNextCounter(userFunc);
    int hostIdx = counter % hostMap.size();
    std::string host = faabric::util::getNthKey(hostMap, hostIdx);
    msg->set_messagetype(0);
    return host;
}

std::string StateAwareScheduler::scheduleStatelessMessageApportion(
  std::string& userFunc,
  const HostMap& hostMap,
  const std::unique_ptr<Message>& msg)
{
    std::string host = "unknown";
    std::string userFuncPar = userFunc + "_0";
    auto shceduledOperator =
      getScheduledOperatorOrThrow(scheduledOperatorsMap, userFunc);
    if (shceduledOperator.isCollocate) {
        // If the optsCollocateMap contains the userFunc. We will try to
        // collocate it with the state.
        std::string collocateFunc = shceduledOperator.collocateWith;
        auto parallelismInfo = getHashAndParallelismIndex(collocateFunc, *msg);
        std::string collocateUserFuncPar =
          collocateFunc + "_" + std::to_string(parallelismInfo.parallelismIdx);
        if (stateHost.find(collocateUserFuncPar) == stateHost.end()) {
            throw std::runtime_error("StateHost is not initialized");
        }
        std::string collocateHost = stateHost[collocateUserFuncPar];
        host = runtimeSummary.getHost(userFuncPar, collocateHost);
    } else {
        // Otherwise the request by using round robin.
        auto localCounter = getNextCounter(userFunc);
        host = runtimeSummary.getHost(userFuncPar, localCounter);
    }
    msg->set_messagetype(0);
    return host;
}

std::string StateAwareScheduler::scheduleStatelessMessageFaaSFlow(
  std::string& userFunc,
  const HostMap& hostMap,
  const std::unique_ptr<Message>& msg)
{
    std::string host = "unknown";
    std::string userFuncPar = userFunc + "_0";
    auto shceduledOperator =
      getScheduledOperatorOrThrow(scheduledOperatorsMap, userFunc);

    // Otherwise the request by using round robin.
    auto localCounter = getNextCounter(userFunc);
    host = runtimeSummary.getHost(userFuncPar, localCounter);

    msg->set_messagetype(0);
    return host;
}

std::string StateAwareScheduler::scheduleStatelessMessageLocal(
  std::string& userFunc,
  const HostMap& hostMap,
  const std::unique_ptr<Message>& msg)
{
    std::string host = localHost;
    msg->set_messagetype(0);
    return host;
}

HashAndParallelismInfo StateAwareScheduler::getHashAndParallelismIndex(
  const std::string& userFunction,
  const faabric::Message& msg)
{
    faabric::util::FullLock lock(scheduleMx);

    // For the partitioned stateful. Using key-partitioning.
    if (statePartitionBy.find(userFunction) != statePartitionBy.end()) {
        // get the input KEY.
        std::string inputString = msg.inputdata();
        std::vector<uint8_t> inputVec(inputString.begin(), inputString.end());
        size_t index = 0;
        std::map<std::string, std::string> inputData =
          faabric::util::deserializeMap(inputVec, index);
        std::string keyData = inputData[statePartitionBy[userFunction]];
        SPDLOG_TRACE("UserFunction {}'s input data: {}", userFunction, keyData);
        // If the HashRing is not initialized, throw error.
        if (stateHashRing.find(userFunction) == stateHashRing.end()) {
            SPDLOG_ERROR("HashRing for {} is not initialized", userFunction);
            throw std::runtime_error("HashRing for " + userFunction +
                                     " is not initialized");
        }
        // Hashing the keyData and set the partition index.
        std::vector<uint8_t> keyDataVec = faabric::util::stringToBytes(keyData);
        auto hashAndNode =
          stateHashRing[userFunction]->getHashAndNode(keyDataVec);
        std::size_t hash = hashAndNode.first;
        int parallelismIdx = hashAndNode.second;
        getNextCounter(userFunction);
        SPDLOG_TRACE("UserFunction {}'s hash {} and parallelismIdx {}",
                     userFunction,
                     hash,
                     parallelismIdx);
        return { 2, hash, parallelismIdx };
    }
    // Otherwise, use shuffle data-partitioning.
    auto localCounter = getNextCounter(userFunction);
    return {
        1, 0, static_cast<int>(localCounter % functionParallelism[userFunction])
    };
}

std::string StateAwareScheduler::scheduleStatefulMessage(
  std::string& userFunc,
  const std::unique_ptr<Message>& msg)
{
    auto parallelismInfo = getHashAndParallelismIndex(userFunc, *msg);
    std::string userFuncPar =
      userFunc + "_" + std::to_string(parallelismInfo.parallelismIdx);
    if (stateHost.find(userFuncPar) == stateHost.end()) {
        throw std::runtime_error("StateHost is not initialized");
    }
    std::string host = stateHost[userFuncPar];
    // Register the parallelismIdx to it.
    // If Scheduling failed, the next scheduling will overwrite it.
    msg->set_messagetype(parallelismInfo.messageType);
    msg->set_hash(parallelismInfo.hash);
    msg->set_parallelismid(parallelismInfo.parallelismIdx);
    return host;
}

std::string StateAwareScheduler::scheduleMessage(
  const HostMap& hostMap,
  const std::unique_ptr<faabric::Message>& msg)
{
    if (msg->user().empty() || msg->function().empty()) {
        throw std::runtime_error("User or function is empty");
    }
    std::string userFunc = msg->user() + "_" + msg->function();
    std::string host = "unknown";
    // Stateful or partitioned stateful function
    if (functionParallelism.contains(userFunc)) {
        // TODO - get parallelism is not thread safe now
        host = scheduleStatefulMessage(userFunc, msg);
    }
    // stateless operator
    else {
        if (scheduleMode == 5) {
            host = scheduleStatelessMessageApportion(userFunc, hostMap, msg);
        } else if (scheduleMode == 3 || scheduleMode == 7) {
            host = scheduleStatelessMessageFaaSFlow(userFunc, hostMap, msg);
        } else {
            host = scheduleStatelessMessageRB(userFunc, hostMap, msg);
        }
    }
    if (host == "unknown") {
        throw std::runtime_error("Host is unknown");
    }
    if (host == localHost) {
        msg->set_isscheduledlocally(true);
    } else {
        msg->set_isscheduledlocally(false);
    }
    return host;
}

std::vector<std::string> StateAwareScheduler::scheduleMessagesBatch(
  const HostMap& hostMap,
  const std::vector<std::unique_ptr<faabric::Message>>& msgs)
{
    if (hostMap.empty()) {
        SPDLOG_ERROR("Host map is empty, cannot schedule messages");
        throw std::runtime_error("Host map is empty");
    }
    std::vector<std::string> hosts;
    hosts.resize(msgs.size());
    for (int i = 0; i < msgs.size(); i++) {
        hosts[i] = scheduleMessage(hostMap, msgs[i]);
    }
    return hosts;
}

// TODO - before repartition. no in flight request.
bool StateAwareScheduler::repartitionParitionedState(
  std::string userFunction,
  std::shared_ptr<std::map<std::string, std::string>> oldStateHost)
{
    SPDLOG_DEBUG("Repartitioning state for {}", userFunction);
    // Select the new hosts and their parallelismIdx for this function.
    std::map<std::string, std::string> newFilteredStateHost;
    for (const auto& [userFuncParIdx, host] : stateHost) {
        if (userFuncParIdx.find(userFunction + "_") == std::string::npos) {
            continue;
        }
        newFilteredStateHost.insert({ userFuncParIdx, host });
    }
    // For all the old State Host, notify the new parallelism.
    std::vector<uint8_t> tmpStateHost =
      faabric::util::serializeMapBinary(newFilteredStateHost);
    std::string newFilteredStateHostStr(tmpStateHost.begin(),
                                        tmpStateHost.end());
    for (const auto& [userFuncParIdx, host] : *oldStateHost) {
        // If the userFuncParallelism is not the userFunction, ignore it.
        if (userFuncParIdx.find(userFunction + "_") == std::string::npos) {
            continue;
        }
        // Inform the new parallelism to the old state server.
        auto [user, function, parallelismId] =
          faabric::util::splitUserFuncPar(userFuncParIdx);
        state::FunctionStateClient cli(
          user, function, std::stoi(parallelismId), host);
        cli.rePartitionState(newFilteredStateHostStr);
    }
    // Send the new parallelism to state server, along with the new map.
    for (const auto& [userFuncParIdx, host] : newFilteredStateHost) {
        // Inform the new parallelism to the old state server.
        auto [user, function, parallelismId] =
          faabric::util::splitUserFuncPar(userFuncParIdx);
        state::FunctionStateClient cli(
          user, function, std::stoi(parallelismId), host);
        cli.combineParState();
    }
    // Wait until get the response from all state server.
    return true;
}

const std::map<std::string, FunctionStateInfo>
StateAwareScheduler::getStateInfo()
{
    SPDLOG_DEBUG("Getting state information");
    std::map<std::string, FunctionStateInfo> stateInfo;
    for (const auto& [func, host] : funcStateRegMap) {
        FunctionStateInfo info;
        info.functionName = func;
        info.partitionBy = std::get<0>(host);
        info.stateKey = std::get<1>(host);
        info.parallelism = functionParallelism[func];
        // State Location Information
        for (const auto& [userFuncPar, host] : stateHost) {
            if (userFuncPar.find(func + "_") == std::string::npos) {
                continue;
            }
            std::size_t pos = userFuncPar.rfind('_');
            if (pos != std::string::npos && pos + 1 < userFuncPar.size()) {
                // Extract the substring after the underscore.
                std::string parIdxStr = userFuncPar.substr(pos + 1);
                // Convert the substring to an integer.
                int parIdx = std::stoi(parIdxStr);
                info.stateHost[parIdx] = host;
            } else {
                SPDLOG_ERROR(
                  "Invalid format when finding host of state parallelism.");
            }
        }
        stateInfo[func] = std::move(info);
    }
    return stateInfo;
}

void StateAwareScheduler::updateApp(
  const std::map<std::string, long>& nodeWorkloads,
  const std::map<std::string, std::map<std::string, int>> edgeWeightMap)
{
    if (!application) {
        SPDLOG_WARN("Scheduler: No application registered");
        return;
    }

    for (const auto& [nodeName, workload] : nodeWorkloads) {
        if (!application->getNodes().contains(nodeName)) {
            SPDLOG_WARN("Scheduler: Node {} not found in the application",
                        nodeName);
            continue;
        }
        application->getNodes().at(nodeName)->processedTuples = workload;
        SPDLOG_DEBUG("Scheduler: Node {} processed {} tuples",
                     nodeName,
                     application->getNodes().at(nodeName)->processedTuples);
    }

    application->updateConnectionsWithWeight(edgeWeightMap);
}

void StateAwareScheduler::groupNodesHelper(
  const std::string& nodeName,
  std::vector<NodeGroup>& groups,
  std::unordered_set<std::string>& visited)
{
    if (visited.count(nodeName)) {
        return;
    }

    std::vector<std::shared_ptr<Node>> currentGroup;
    std::string currentPartition = NONE_STRING;

    // Start a stack for DFS
    std::vector<std::string> stack;
    stack.push_back(nodeName);

    while (!stack.empty()) {
        std::string current = stack.back();
        stack.pop_back();

        if (visited.count(current)) {
            continue;
        }

        auto node = application->getNodes().at(current);
        currentGroup.push_back(node);
        visited.insert(current);

        if (node->type == PARTITIONED_STATEFUL) {
            currentPartition = node->partitionBy;
        }

        // Find all successors
        auto connIt = application->getConnections().find(current);
        if (connIt != application->getConnections().end()) {
            for (const auto& succName : connIt->second) {
                auto succ = application->getNodes().at(succName);
                bool canJoin = false;

                if (node->type != STATEFUL && succ->type == STATELESS) {
                    canJoin = true;
                }
                if (node->type != STATEFUL &&
                    succ->type == PARTITIONED_STATEFUL &&
                    (currentPartition == NONE_STRING ||
                     currentPartition == succ->partitionBy)) {
                    canJoin = true;
                }

                if (canJoin && !visited.count(succName)) {
                    // Joinable and not visited: add to this group and DFS next
                    stack.push_back(succName);
                } else if (!visited.count(succName)) {
                    // Not joinable: start as a new group
                    groupNodesHelper(succName, groups, visited);
                }
            }
        }
    }

    // Add the group if not empty
    if (!currentGroup.empty()) {
        groups.emplace_back(currentGroup, currentPartition);
    }
}

void StateAwareScheduler::groupNodesStrictHelper(
  const std::string& nodeName,
  std::vector<NodeGroup>& groups,
  std::unordered_set<std::string>& visited)
{
    if (visited.count(nodeName)) {
        return;
    }

    std::vector<std::shared_ptr<Node>> currentGroup;
    std::string currentPartition = NONE_STRING;

    // Start a stack for DFS
    std::vector<std::string> stack;
    stack.push_back(nodeName);

    while (!stack.empty()) {
        std::string current = stack.back();
        stack.pop_back();

        if (visited.count(current)) {
            continue;
        }

        auto node = application->getNodes().at(current);
        currentGroup.push_back(node);
        visited.insert(current);

        if (node->type == PARTITIONED_STATEFUL) {
            currentPartition = node->partitionBy;
        }

        // Find all successors
        auto connIt = application->getConnections().find(current);
        if (connIt != application->getConnections().end()) {
            for (const auto& succName : connIt->second) {
                auto succ = application->getNodes().at(succName);
                bool canJoin = false;

                if (succ->type == STATELESS) {
                    canJoin = true;
                }
                if (succ->type == PARTITIONED_STATEFUL) {
                    std::string succPartition = succ->partitionBy;
                    bool violate = false;

                    if (currentPartition != NONE_STRING &&
                        currentPartition != succPartition) {
                        violate = true;
                    }
                    for (auto node : currentGroup) {
                        if (!node->inputFeilds.contains(succPartition)) {
                            violate = true;
                        }
                    }
                    if (!violate) {
                        canJoin = true;
                    }
                }

                if (node->type == STATEFUL) {
                    canJoin = false;
                }

                if (canJoin && !visited.count(succName)) {
                    // Joinable and not visited: add to this group and DFS next
                    stack.push_back(succName);
                } else if (!visited.count(succName)) {
                    // Not joinable: start as a new group
                    groupNodesStrictHelper(succName, groups, visited);
                }
            }
        }
    }

    // Add the group if not empty
    if (!currentGroup.empty()) {
        groups.emplace_back(currentGroup, currentPartition);
    }
}

void StateAwareScheduler::groupNodesLooseHelper(
  const std::string& nodeName,
  std::vector<NodeGroup>& groups,
  std::unordered_set<std::string>& visited)
{
    if (visited.count(nodeName)) {
        return;
    }

    std::vector<std::shared_ptr<Node>> currentGroup;
    std::string currentPartition = NONE_STRING;

    // Start a stack for DFS
    std::vector<std::string> stack;
    stack.push_back(nodeName);

    while (!stack.empty()) {
        std::string current = stack.back();
        stack.pop_back();

        if (visited.count(current)) {
            continue;
        }

        auto node = application->getNodes().at(current);
        currentGroup.push_back(node);
        visited.insert(current);

        if (node->type == PARTITIONED_STATEFUL) {
            currentPartition = node->partitionBy;
        }

        // Find all successors
        auto connIt = application->getConnections().find(current);
        if (connIt != application->getConnections().end()) {
            for (const auto& succName : connIt->second) {
                auto succ = application->getNodes().at(succName);
                bool canJoin = false;

                if (succ->type == STATELESS) {
                    canJoin = true;
                }
                if (succ->type == PARTITIONED_STATEFUL) {
                    canJoin = true;
                }

                if (node->type == STATEFUL) {
                    canJoin = false;
                }

                if (canJoin && !visited.count(succName)) {
                    // Joinable and not visited: add to this group and DFS next
                    stack.push_back(succName);
                } else if (!visited.count(succName)) {
                    // Not joinable: start as a new group
                    groupNodesLooseHelper(succName, groups, visited);
                }
            }
        }
    }

    // Add the group if not empty
    if (!currentGroup.empty()) {
        groups.emplace_back(currentGroup, currentPartition);
    }
}

bool StateAwareScheduler::nodeCollocation(
  const std::string& current,
  const std::string& partitionKey,
  const std::unordered_set<std::string>& groupNodeNames)
{
    if (!groupNodeNames.contains(current)) {
        return false;
    }
    auto currentNode = application->getNodes().at(current);
    if (currentNode->type == STATELESS &&
        currentNode->inputFeilds.contains(partitionKey)) {
        return true;
    }
    return false;
}

void StateAwareScheduler::collectCollocation(
  const std::string& current,
  const std::string& psName,
  const std::string& partitionKey,
  std::map<std::string, std::string>& collocateMap,
  const std::unordered_set<std::string>& groupNodeNames)
{
    // If the current node is not collocated with the partition key, return.
    if (!nodeCollocation(current, partitionKey, groupNodeNames)) {
        return;
    }

    auto sources = application->getSource(current);
    for (const auto& srcNode : sources) {
        collectCollocation(
          srcNode->name, psName, partitionKey, collocateMap, groupNodeNames);
    }

    collocateMap[current] = psName;
}

std::map<std::string, ScheduledOperator>
StateAwareScheduler::buildScheduledOperatorsForGroup(
  int groupId,
  const std::vector<std::shared_ptr<Node>>& group,
  const std::map<std::string, std::string>& newOptsCollocateMap,
  const std::map<std::string, std::map<std::string, double>>&
    newStatelessReqWeight,
  const std::map<std::string, std::map<int, double>>& newParStateReqWeight)
  const
{
    std::map<std::string, ScheduledOperator> scheduledOperatorsGroup;
    // If the source of the group is not in the same group, it is the head.
    for (const auto& node : group) {
        // Initialize the information.
        std::string userFunction = node->name;
        auto type = node->type;
        bool isCollocate = false;
        std::string collocatewith = "None";
        int parallelism = 1;
        // IP to weight distribution.
        std::map<std::string, double> weightDist;
        // State instance id to IP Mapping. Only used for stateful
        // operators.
        std::map<int, std::string> parallelismDist;

        if (newOptsCollocateMap.contains(userFunction)) {
            isCollocate = true;
            collocatewith = newOptsCollocateMap.at(userFunction);
        }
        if (functionParallelism.contains(userFunction)) {
            parallelism = functionParallelism.at(userFunction);
        }
        if (type == STATELESS &&
            newStatelessReqWeight.contains(userFunction + "_0")) {
            weightDist = newStatelessReqWeight.at(userFunction + "_0");
        }
        if (type == PARTITIONED_STATEFUL) {
            for (int i = 0; i < parallelism; ++i) {
                std::string userFuncPar =
                  userFunction + "_" + std::to_string(i);
                if (!stateHost.contains(userFuncPar)) {
                    throw std::runtime_error("State host for " + userFuncPar +
                                             " not found");
                }
                auto assignedIp = stateHost.at(userFuncPar);
                parallelismDist[i] = assignedIp;
                if (newParStateReqWeight.contains(userFunction)) {
                    if (newParStateReqWeight.at(userFunction).contains(i)) {
                        weightDist[assignedIp] =
                          newParStateReqWeight.at(userFunction).at(i);
                    } else {
                        SPDLOG_ERROR(
                          "Parallelism {} not found for {}", i, userFunction);
                        throw std::runtime_error(
                          "Parallelism " + std::to_string(i) +
                          " not found for " + userFunction);
                    }
                }
            }
        }
        if (type == STATEFUL) {
            auto instReqRes =
              node->reqResource / static_cast<double>(node->parallelism);
            for (int i = 0; i < parallelism; ++i) {
                std::string userFuncPar =
                  userFunction + "_" + std::to_string(i);
                if (!stateHost.contains(userFuncPar)) {
                    throw std::runtime_error("State host for " + userFuncPar +
                                             " not found");
                }
                auto assignedIp = stateHost.at(userFuncPar);
                parallelismDist[i] = assignedIp;
                // Assuming the weight distribution for each stateful
                // parallelism instance is the same
                weightDist[assignedIp] += instReqRes;
            }
        }

        auto sop = ScheduledOperator(*node,
                                     groupId,
                                     isCollocate,
                                     collocatewith,
                                     parallelism,
                                     weightDist,
                                     parallelismDist,
                                     LocalStatelessOperatorType::UNKNOWN);

        scheduledOperatorsGroup.emplace(node->name, sop);
    }
    return scheduledOperatorsGroup;
}

/***
 * The steps of reschedule App:
 * 1. Calculate the workload of each operator, which is based on the executed
 * requests number.
 * 2. Group the operators into groups. TODO - combine the partitioned stateful
 * with same attribute.
 * 3. Map groups to hosts.
 * 4. Arrange the states accordingly.
 * ***/
void StateAwareScheduler::rescheduleApp(const HostMap& hostMap)
{
    if (scheduleMode == 3 || scheduleMode == 7) {
        rescheduleAppFaaSFlow(hostMap);
        return;
    }

    SPDLOG_INFO("StateAwareScheduler: Reschedule the application according to "
                "the metrics");

    scheduledOperatorsMap.clear();

    //--------------------------------------------------------------------------
    // 1. Calculate the workload of each operator.
    //--------------------------------------------------------------------------

    if (!application) {
        SPDLOG_WARN("No application registered");
        return;
    }
    auto& appNodes = application->getNodes();
    if (appNodes.empty()) {
        SPDLOG_WARN("No nodes recorded in the application");
        return;
    }

    // TODO - scale the number of hosts.
    application->quantiseResources(hostMap.size(), scheduleMode);
    application->showConnections();

    //--------------------------------------------------------------------------
    // 2. Partition the application into sub-groups.
    //--------------------------------------------------------------------------

    std::vector<NodeGroup> groups;
    std::unordered_set<std::string> visited;

    if (scheduleMode == 5) {
        for (const auto& inputNode : application->getInputNodes()) {
            groupNodesStrictHelper(inputNode, groups, visited);
        }
    } else {
        for (const auto& inputNode : application->getInputNodes()) {
            groupNodesHelper(inputNode, groups, visited);
        }
    }

    {
        std::stringstream ss;
        for (size_t i = 0; i < groups.size(); ++i) {
            const auto& [nodes, partition] = groups[i];

            if (i > 0) {
                ss << "; ";
            }

            ss << "Group " << i << "(partition=\"" << partition << "\"): [";

            bool first = true;
            for (const auto& nodePtr : nodes) {
                if (!first)
                    ss << ", ";
                ss << nodePtr->name;
                first = false;
            }
            ss << "]";
        }

        SPDLOG_INFO("All groups:\n {}", ss.str());
    }
    //--------------------------------------------------------------------------
    // 3. Map groups to hosts.
    //--------------------------------------------------------------------------

    std::map<std::string, double> workerRemaining;
    for (const auto& [ip, host] : hostMap) {
        workerRemaining[ip] = 1.0;
    }

    // For each group, how much resource is allocated to each worker. The order
    // of groups is the same as groups variable.
    // MAP <ip, allocated resource>
    std::vector<std::map<std::string, double>> groupAllocations;

    for (size_t groupIndex = 0; groupIndex < groups.size(); ++groupIndex) {
        double groupReqResource = 0.0;
        for (const auto& node : std::get<0>(groups[groupIndex])) {
            groupReqResource += node->reqResource;
        }

        // Map of this group resource distribution (IP -> allocated resource).
        std::map<std::string, double> groupAllocation;
        // Greedily fill the available workers.
        for (auto& [ip, available] : workerRemaining) {
            if (available <= 0)
                continue;
            if (groupReqResource <= 0)
                break;

            if (available >= groupReqResource) {
                // If the available resource on worker is enough.
                groupAllocation[ip] = groupReqResource;
                available -= groupReqResource;
                groupReqResource = 0;
                break;
            } else {
                // Otherwise (not enough).
                groupAllocation[ip] = available;
                groupReqResource -= available;
                available = 0;
            }
        }

        // 1e-9 is rounding up tolerance.
        if (groupReqResource > 1e-9) {
            SPDLOG_WARN("Not enough resources to assign group {}", groupIndex);
            throw std::runtime_error("Insufficient resources across workers");
        }

        groupAllocations.push_back(groupAllocation);
    }

    //--------------------------------------------------------------------------
    // 4. Arrange the states accordingly.
    // We change the parallelism of partitioned stateful operators to the number
    // of workers in its group. We don't change the parallelism of the stateful
    // function.
    //--------------------------------------------------------------------------

    std::map<std::string, std::string> newStateHost;
    std::map<std::string, int> newFunctionParallelism;
    // MAP <USER_FUNC_PARALLELISM, MAP<IP, proportion>>
    std::map<std::string, std::map<std::string, double>> newStatelessReqWeight;
    // MAP <USER_FUNC, MAP<PARALLELISM_IDX, proportion>>
    std::map<std::string, std::map<int, double>> newParStateReqWeight;

    // For each group, statistics its states and assigns states to workers.
    for (size_t groupIndex = 0; groupIndex < groups.size(); ++groupIndex) {
        const auto& group = std::get<0>(groups[groupIndex]);
        auto groupAllocation = groupAllocations[groupIndex];

        // If the group only has one stateful operator. Assgin it.
        if (group.size() == 1 && group[0]->type == STATEFUL) {
            auto node = group[0];
            std::string UserFunc = node->name;
            // We don't change the parallelism of the stateful function.
            int para = node->parallelism;
            newFunctionParallelism[UserFunc] = para;
            for (int i = 0; i < para; ++i) {
                // Find the worker with the maximum available resource in this
                // group.
                std::string bestWorker;
                double bestAvail = -1.0;
                for (const auto& [ip, value] : groupAllocation) {
                    if (value > bestAvail) {
                        bestAvail = value;
                        bestWorker = ip;
                    }
                }
                newStateHost[UserFunc + "_" + std::to_string(i)] = bestWorker;
                // Update the available resource for the selected worker.
                double reqRes = (node->reqResource) / para;
                groupAllocation[bestWorker] =
                  std::max(0.0, groupAllocation[bestWorker] - reqRes);
            }
            continue;
        }

        // Otherwise, it contains partitioned stateful and stateless operators
        // For partitioned stateful, we assign it to every workers,
        // (who has higher than 10% resource).

        // Sanity check: if the group has stateful operator, throw error.
        for (const auto& node : group) {
            if (node->type == STATEFUL) {
                SPDLOG_ERROR("Group {} contains multiple operators with "
                             "stateful operator {}, which is not "
                             "supported in rescheduling",
                             groupIndex,
                             node->name);
            }
        }

        // Do assign the stateless and partitioned stateful operators.
        double groupResource = 0;
        for (const auto& node : group) {
            groupResource += node->reqResource;
        }

        for (const auto& node : group) {
            std::string userFunc = node->name;
            if (node->type == STATELESS) {
                double nodeReqResource = node->reqResource;
                double scale = nodeReqResource / groupResource;
                std::map<std::string, double> nodeAllocation;
                for (const auto& [ip, groupWeight] : groupAllocation) {
                    double nodeAlloc = groupWeight * scale;
                    if (nodeAlloc > 0) {
                        nodeAllocation[ip] = nodeAlloc;
                    }
                }
                newStatelessReqWeight[userFunc + "_0"] = nodeAllocation;
            }
            if (node->type == PARTITIONED_STATEFUL) {
                int index = 0;
                newFunctionParallelism[userFunc] = groupAllocation.size();
                double nodeReqResource = node->reqResource;
                double scale = nodeReqResource / groupResource;
                for (const auto& [ip, groupWeight] : groupAllocation) {
                    std::string userFuncPar =
                      userFunc + "_" + std::to_string(index);
                    newParStateReqWeight[userFunc][index] = groupWeight * scale;
                    newStateHost[userFuncPar] = ip;
                    index++;
                }
            }
        }
    }

    //--------------------------------------------------------------------------
    // 5. Collocate stateless operators with partitioned stateful operators.
    // For each operator, we try to assign the stateless operators with states
    // being acted up by the following partitioned stateful operators.
    //--------------------------------------------------------------------------
    std::map<std::string, std::string> newOptsCollocateMap;
    for (const auto& group : groups) {
        const auto& groupNodes = std::get<0>(group);

        std::unordered_set<std::string> groupNodeNames;
        for (const auto& node : groupNodes) {
            groupNodeNames.insert(node->name);
        }

        const auto& groupPartition = std::get<1>(group);
        if (groupPartition == NONE_STRING) {
            continue;
        }
        // Fetch the partitioned stateful operators.
        std::vector<std::shared_ptr<Node>> psNodes;
        for (const auto& node : groupNodes) {
            if (node->type == PARTITIONED_STATEFUL) {
                psNodes.push_back(node);
            }
        }
        // For the preceding stateless operators, we try to collocate them with
        // this partitioned stateful operator.
        if (psNodes.empty()) {
            continue;
        }

        for (const auto& psNode : psNodes) {
            const auto& psName = psNode->name;
            auto psNodePartition = psNode->partitionBy;
            auto sources = application->getSource(psName);
            for (const auto& srcNode : sources) {
                collectCollocation(srcNode->name,
                                   psName,
                                   psNodePartition,
                                   newOptsCollocateMap,
                                   groupNodeNames);
            }
        }
    }

    // Update the state host and parallelism info.
    stateHost = newStateHost;
    functionParallelism = newFunctionParallelism;

    // Build the scheduledOperatorsMap
    scheduledOperatorsMap.clear();
    for (int groupId = 0; groupId < groups.size(); ++groupId) {
        auto grpMap =
          buildScheduledOperatorsForGroup(groupId,
                                          std::get<0>(groups[groupId]),
                                          newOptsCollocateMap,
                                          newStatelessReqWeight,
                                          newParStateReqWeight);
        scheduledOperatorsMap.insert(grpMap.begin(), grpMap.end());
    }

    // Init the runtime summary.
    runtimeSummary.initScheduledOperators(
      *application, scheduledOperatorsMap, true, scheduleMode);

    // Initialize the State Information for stateful and partitioned stateful
    // operators.
    stateHashRing.clear();

    redis::Redis& redis = redis::Redis::getState();
    redis.flushAll();
    for (const auto& [stateName, ip] : stateHost) {
        registerStateToRedis(stateName, ip);
    }

    for (const auto& [userFunction, partitionBy] : statePartitionBy) {
        if (!functionParallelism.contains(userFunction)) {
            SPDLOG_ERROR("Function {} has no parallelism", userFunction);
            throw std::runtime_error("Function parallelism not found");
        }
        auto weightDist = newParStateReqWeight[userFunction];
        stateHashRing[userFunction] =
          std::make_shared<faabric::util::ConsistentHashRing>(weightDist);
    }

    printScheduleInfomation();
}

NodeGroup& groupContainingNode(std::vector<NodeGroup>& groups,
                               const std::string& nodeName)
{
    for (auto& group : groups) {
        const auto& nodeVector = std::get<0>(group);
        for (const auto& nodePtr : nodeVector) {
            if (nodePtr->name == nodeName) {
                return group;
            }
        }
    }

    SPDLOG_ERROR("Node {} not found in any group", nodeName);
    throw std::runtime_error("Node not found in any group");
}

double getRequireResource(const NodeGroup& group)
{
    double totalResource = 0;
    const auto& nodeVector = std::get<0>(group);
    for (const auto& nodePtr : nodeVector) {
        totalResource += nodePtr->reqResource;
    }
    return totalResource;
}

/**
 * @brief Performs the main greedy grouping algorithm for FaaSFlow scheduling.
 *
 * This function initializes each node as its own group, assigns it to a worker,
 * and then iteratively merges groups based on connection weights and resource
 * constraints until no more merges are possible.
 *
 * @param hostMap A map of available hosts (workers).
 * @param appNodes A map of all nodes in the application.
 * @return A tuple containing:
 * - The final vector of merged NodeGroups.
 * - The final vector of group allocations.
 * - The final map of remaining resources on each worker.
 */
std::tuple<std::vector<NodeGroup>,
           std::vector<std::map<std::string, double>>,
           std::map<std::string, double>>
StateAwareScheduler::groupNodesGreedily(const HostMap& hostMap)
{
    // --- Initial State Setup ---
    auto& appNodes = application->getNodes();

    std::vector<NodeGroup> groups;
    std::vector<std::map<std::string, double>> groupAllocations;
    std::map<std::string, double> workerRemaining;

    for (const auto& [ip, host] : hostMap) {
        workerRemaining[ip] = 1.0;
    }

    // --- Initial Group Creation and Placement (Done ONCE) ---
    // Each node starts as its own group, assigned to the best available worker.
    for (const auto& [nodeName, node] : appNodes) {
        groups.push_back({ { node }, NONE_STRING });

        // Find the worker with maximum available resource.
        std::string bestWorker;
        double bestAvail = std::numeric_limits<double>::lowest();
        for (const auto& [ip, value] : workerRemaining) {
            if (value > bestAvail) {
                bestAvail = value;
                bestWorker = ip;
            }
        }

        groupAllocations.push_back({ { bestWorker, node->reqResource } });
        workerRemaining[bestWorker] -= node->reqResource;
    }

    // --- Iterative Merging Loop ---
    bool merged = true;
    while (merged) {
        merged = false;
        auto connectionsWithWeight = application->getConnectionsWithWeight();

        for (const auto& conn : connectionsWithWeight) {
            auto& inputGroup = groupContainingNode(groups, conn.input);
            auto& outputGroup = groupContainingNode(groups, conn.output);

            if (&inputGroup == &outputGroup) {
                continue;
            }

            double inputResource = getRequireResource(inputGroup);
            double outputResource = getRequireResource(outputGroup);

            SPDLOG_DEBUG("Checking merge for node {} ({} group resources) and "
                         "node {} ({} group resources)",
                         conn.input,
                         inputResource,
                         conn.output,
                         outputResource);
            if (inputResource + outputResource > 1.0) {
                continue;
            }

            // If a merge is possible, set the flag and perform the merge.
            merged = true;

            // Find indices and ensure index1 < index2
            int index1 = -1, index2 = -1;
            for (size_t i = 0; i < groups.size(); ++i) {
                if (&groups[i] == &inputGroup)
                    index1 = i;
                if (&groups[i] == &outputGroup)
                    index2 = i;
            }
            if (index1 > index2)
                std::swap(index1, index2);

            // Free up resources from original groups
            for (const auto& [workerIp, resource] : groupAllocations[index2]) {
                workerRemaining[workerIp] += resource;
            }
            for (const auto& [workerIp, resource] : groupAllocations[index1]) {
                workerRemaining[workerIp] += resource;
            }

            // Perform the merge of nodes and allocations
            auto& target_nodes = std::get<0>(groups[index1]);
            auto& source_nodes = std::get<0>(groups[index2]);
            target_nodes.insert(target_nodes.end(),
                                std::make_move_iterator(source_nodes.begin()),
                                std::make_move_iterator(source_nodes.end()));
            groups.erase(groups.begin() + index2);
            groupAllocations.erase(groupAllocations.begin() + index2);

            NodeGroup& mergedGroup = groups[index1];

            // Find a new worker for the merged group
            double mergedResource = getRequireResource(mergedGroup);
            std::string bestWorker;
            double bestAvail = std::numeric_limits<double>::lowest();
            for (const auto& [ip, avail] : workerRemaining) {
                if (avail >= mergedResource) {
                    bestWorker = ip;
                    break;
                }
                if (avail > bestAvail) {
                    bestAvail = avail;
                    bestWorker = ip;
                }
            }

            // Assign the merged group to the new worker and update state
            workerRemaining[bestWorker] -= mergedResource;
            groupAllocations[index1] = { { bestWorker, mergedResource } };
        }
    }

    if (groups.empty()) {
        SPDLOG_ERROR("No groups formed after greedy grouping");
        throw std::runtime_error("No groups formed after greedy grouping");
    }
    return { groups, groupAllocations, workerRemaining };
}

void StateAwareScheduler::rescheduleAppFaaSFlow(const HostMap& hostMap)
{
    SPDLOG_INFO("Rescheduling the application in FaaSFlow Mode");

    scheduledOperatorsMap.clear();

    //--------------------------------------------------------------------------
    // 1. Calculate the workload of each operator.
    //--------------------------------------------------------------------------

    if (!application) {
        SPDLOG_WARN("No application registered");
        return;
    }
    auto& appNodes = application->getNodes();
    if (appNodes.empty()) {
        SPDLOG_WARN("No nodes recorded in the application");
        return;
    }

    application->quantiseResources(hostMap.size(), scheduleMode);
    application->showConnections();

    //--------------------------------------------------------------------------
    // 2. Assign groups to nodes Randomly
    //--------------------------------------------------------------------------

    auto [groups, groupAllocations, workerRemaining] =
      groupNodesGreedily(hostMap);

    {
        std::stringstream ss;
        for (size_t i = 0; i < groups.size(); ++i) {
            const auto& [nodes, partition] = groups[i];

            if (i > 0) {
                ss << "; ";
            }

            ss << "Group " << i << "(partition=\"" << partition << "\"): [";

            bool first = true;
            for (const auto& nodePtr : nodes) {
                if (!first)
                    ss << ", ";
                ss << nodePtr->name;
                first = false;
            }
            ss << "]";
        }

        SPDLOG_INFO("All groups:\n {}", ss.str());
    }

    // --------------------------------------------------------------------------
    // 3. Allocate the unused workers to the groups.
    //---------------------------------------------------------------------------
    for (const auto& [ip, host] : hostMap) {
        // Skip workers that have already been assigned some workload.
        // We are only interested in workers that are completely free.
        if (workerRemaining.at(ip) != 1.0) {
            continue;
        }

        // Find the group with the most nodes. This will be our target.
        int targetIdx = -1;
        double maxResource = std::numeric_limits<double>::lowest();
        for (int i = 0; i < groupAllocations.size(); ++i) {
            const auto& groupAllocation = groupAllocations[i];
            for (const auto& [workerIp, resource] : groupAllocation) {
                if (resource >= maxResource) {
                    maxResource = resource;
                    targetIdx = i;
                }
            }
        }

        if (targetIdx != -1) {
            // Update the groupAllocations and workerRemaining.
            double totalResource = 0;
            for (const auto& [_, resource] : groupAllocations[targetIdx]) {
                totalResource += resource;
            }
            groupAllocations[targetIdx][ip] = 0.0;
            double newEvenShare =
              totalResource / groupAllocations[targetIdx].size();

            for (auto& [workerIp, allocatedResource] :
                 groupAllocations[targetIdx]) {
                workerRemaining[workerIp] += allocatedResource;
                // Then, assign the new even share and deduct it
                allocatedResource = newEvenShare;
                workerRemaining[workerIp] -= newEvenShare;
            }
        }
    }

    //--------------------------------------------------------------------------
    // 4. Arrange the states accordingly.
    //--------------------------------------------------------------------------
    std::map<std::string, std::string> newStateHost;
    std::map<std::string, int> newFunctionParallelism;
    // MAP <USER_FUNC_PARALLELISM, MAP<IP, proportion>>
    std::map<std::string, std::map<std::string, double>> newStatelessReqWeight;
    // MAP <USER_FUNC, MAP<PARALLELISM_IDX, proportion>>
    std::map<std::string, std::map<int, double>> newParStateReqWeight;

    for (size_t groupIndex = 0; groupIndex < groups.size(); ++groupIndex) {
        const auto& group = std::get<0>(groups[groupIndex]);
        auto groupAllocation = groupAllocations[groupIndex];

        double groupResource = 0;
        for (const auto& node : group) {
            groupResource += node->reqResource;
        }

        for (const auto& node : group) {
            std::string userFunc = node->name;
            if (node->type == STATELESS) {
                // STATELESS: Assign it to all workers evenly.
                double nodeReqResource = node->reqResource;
                double scale = nodeReqResource / groupResource;
                std::map<std::string, double> nodeAllocation;
                for (const auto& [ip, groupWeight] : groupAllocation) {
                    double nodeAlloc = groupWeight * scale;
                    if (nodeAlloc > 0) {
                        nodeAllocation[ip] = nodeAlloc;
                    }
                }
                newStatelessReqWeight[userFunc + "_0"] = nodeAllocation;
            } else if (node->type == PARTITIONED_STATEFUL) {
                // P_STATEFUL: Assign it to all workers evenly.
                int index = 0;
                newFunctionParallelism[userFunc] = groupAllocation.size();
                double nodeReqResource = node->reqResource;
                double scale = nodeReqResource / groupResource;
                for (const auto& [ip, groupWeight] : groupAllocation) {
                    std::string userFuncPar =
                      userFunc + "_" + std::to_string(index);
                    newParStateReqWeight[userFunc][index] = groupWeight * scale;
                    newStateHost[userFuncPar] = ip;
                    index++;
                }
            } else if (node->type == STATEFUL) {
                // STATEFUL: we assign it to workers with round-robin.
                int para = node->parallelism;
                newFunctionParallelism[userFunc] = para;
                for (int i = 0; i < para; ++i) {
                    int workerRB = i % groupAllocation.size();
                    std::string worker =
                      faabric::util::getNthKey(groupAllocation, workerRB);
                    newStateHost[userFunc + "_" + std::to_string(i)] = worker;
                }
            }
        }
    }

    //--------------------------------------------------------------------------
    // 5. Build the scheduledOperatorsMap.
    //--------------------------------------------------------------------------

    // Update the state host and parallelism info.
    stateHost = newStateHost;
    functionParallelism = newFunctionParallelism;

    // Build the scheduledOperatorsMap
    scheduledOperatorsMap.clear();
    for (int groupId = 0; groupId < groups.size(); ++groupId) {
        auto grpMap =
          buildScheduledOperatorsForGroup(groupId,
                                          std::get<0>(groups[groupId]),
                                          {},
                                          newStatelessReqWeight,
                                          newParStateReqWeight);
        scheduledOperatorsMap.insert(grpMap.begin(), grpMap.end());
    }

    // Init the runtime summary.
    runtimeSummary.initScheduledOperators(
      *application, scheduledOperatorsMap, true, scheduleMode);

    // Initialize the State Information for stateful and partitioned stateful
    // operators.
    stateHashRing.clear();

    redis::Redis& redis = redis::Redis::getState();
    redis.flushAll();
    for (const auto& [stateName, ip] : stateHost) {
        registerStateToRedis(stateName, ip);
    }

    for (const auto& [userFunction, partitionBy] : statePartitionBy) {
        if (!functionParallelism.contains(userFunction)) {
            SPDLOG_ERROR("Function {} has no parallelism", userFunction);
            throw std::runtime_error("Function parallelism not found");
        }
        auto weightDist = newParStateReqWeight[userFunction];
        stateHashRing[userFunction] =
          std::make_shared<faabric::util::ConsistentHashRing>(weightDist);
    }

    printScheduleInfomation();
}

void StateAwareScheduler::printScheduleInfomation() const
{
    SPDLOG_INFO("Scheduler: Scheduled Operators Map:");
    printScheduledOperatorsMap(scheduledOperatorsMap);
    // 1) Function parallelism
    {
        std::ostringstream ss;
        bool first = true;
        for (const auto& [func, par] : functionParallelism) {
            if (!first)
                ss << ", ";
            first = false;
            ss << func << "=" << par;
        }
        SPDLOG_INFO("FunctionParallelism: {}", ss.str());
    }

    // 2) State host assignments
    {
        std::ostringstream ss;
        bool first = true;
        for (const auto& [funcPar, host] : stateHost) {
            if (!first)
                ss << ", ";
            first = false;
            ss << funcPar << "->" << host;
        }
        SPDLOG_INFO("StateHostMap: {}", ss.str());
    }

    {
        std::ostringstream ss;
        bool first = true;
        for (const auto& [func, partitionBy] : statePartitionBy) {
            if (!first)
                ss << ", ";
            first = false;
            ss << func << "->" << partitionBy;
        }
        SPDLOG_INFO("StatePartitionBy: {}", ss.str());
    }

    {
        std::ostringstream ss;
        bool first = true;
        for (const auto& [func, ring] : stateHashRing) {
            if (!first)
                ss << ", ";
            first = false;
            ss << func << "->(" << ring->nodeWeightsToString() << ")";
        }
        SPDLOG_INFO("stateHashRing: {}", ss.str());
    }
}

void StateAwareScheduler::runtimeDistTune(
  const std::map<std::string, std::map<std::string, int>>& observeDistMap)
{
    if (!runtimeReconfig) {
        return;
    }
    // We only schedule when the schedule mode is 5.
    if (scheduleMode != 5) {
        return;
    }
    runtimeSummary.requestDistTune(observeDistMap);
}

void StateAwareScheduler::setScheduleMode(int mode)
{
    scheduleMode = mode;
}

void StateAwareScheduler::setRuntimeReconfig(bool value)
{
    runtimeReconfig = value;
}

void StateAwareScheduler::setAlpha(double value)
{
    runtimeSummary.setAlpha(value);
}

void StateAwareScheduler::resetScheduler()
{
    SPDLOG_INFO("Flushing state information");
    stateRbCounter.store(0);

    if (isplanner) {
        redis::Redis& redis = redis::Redis::getState();
        redis.flushAll();
    }
    functionParallelism.clear();
    counterTable.clear();
    scheduledOperatorsMap.clear();
    stateHost.clear();
    stateHashRing.clear();
    statePartitionBy.clear();
    funcStateRegMap.clear();
    runtimeSummary.reset();
}

} // namespace faabric::batch_scheduler