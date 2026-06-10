#include <faabric/batch-scheduler/SchedulingDecision.h>
#include <faabric/batch-scheduler/StateAwareScheduler.h>
#include <faabric/state/FunctionStateClient.h>
#include <faabric/util/batch.h>
#include <faabric/util/logging.h>
// #include <faabric/util/map.h>
#include <faabric/util/serialization.h>
#include <faabric/util/string_tools.h>

#include <algorithm>
#include <cmath>
#include <functional>
#include <limits>
#include <sstream>

#define MAIN_KEY_PREFIX "main_"

namespace faabric::batch_scheduler {

// Shared contiguous Binpack bin-fill helper (defined below). Lays
// (operator, reqResource) pairs onto a tape of unit-capacity slots.
static std::map<std::string, std::map<int, double>> binpackTapeFill(
  const std::vector<std::pair<std::string, double>>& ordered,
  int numSlots);

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

// std::string StateAwareScheduler::scheduleStatelessMessageRBHost(
//   std::string& userFunc,
//   const HostMap& hostMap,
//   const std::unique_ptr<Message>& msg)
// {
//     auto counter = getNextCounter(userFunc);
//     int hostIdx = counter % hostMap.size();
//     std::string host = faabric::util::getNthKey(hostMap, hostIdx);
//     msg->set_messagetype(0);
//     return host;
// }

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

std::string StateAwareScheduler::scheduleStatelessMessageRoundRobin(
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

std::string StateAwareScheduler::scheduleMessage(const HostMap& hostMap,
                                                 const faabric::Message& msg)
{
    auto* nonConstMsg = const_cast<faabric::Message*>(&msg);
    std::unique_ptr<faabric::Message> tempWrapper(nonConstMsg);
    std::string host = scheduleMessage(hostMap, tempWrapper);
    tempWrapper.release();

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
        if (scheduleMode == 0 || scheduleMode == 5 || scheduleMode == 11) {
            host = scheduleStatelessMessageApportion(userFunc, hostMap, msg);
        } else {
            // when scheduleMode is 3, 7, we use round robin
            host = scheduleStatelessMessageRoundRobin(userFunc, hostMap, msg);
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

void StateAwareScheduler::scheduleApp(const HostMap& hostMap)
{

    //--------------------------------------------------------------------------
    // 1. Calculate the workload of each operator.
    //--------------------------------------------------------------------------

    scheduledOperatorsMap.clear();

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

    if (scheduleMode == 3 || scheduleMode == 7) {
        rescheduleAppFaaSFlow(hostMap);
        return;
    }

    if (scheduleMode == 0) {
        rescheduleAppBinpack(hostMap);
        return;
    }

    if (scheduleMode == 11) {
        rescheduleAppStepConf(hostMap);
        return;
    }

    SPDLOG_INFO("StateAwareScheduler: Reschedule the application according to "
                "the metrics");

    //--------------------------------------------------------------------------
    // 2. Partition the application into sub-groups.
    //--------------------------------------------------------------------------

    std::vector<NodeGroup> groups;
    std::unordered_set<std::string> visited;

    for (const auto& inputNode : application->getInputNodes()) {
        groupNodesStrictHelper(inputNode, groups, visited);
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

/***
 * The steps of reschedule App:
 * 1. Calculate the workload of each operator, which is based on the executed
 * requests number.
 * 2. Group the operators into groups. TODO - combine the partitioned stateful
 * with same attribute.
 * 3. Map groups to hosts.
 * 4. Arrange the states accordingly.
 * ***/
void StateAwareScheduler::rescheduleApp(
  const HostMap& hostMap,
  const faabric::planner::ApplicationMetrics* metrics)
{

    //--------------------------------------------------------------------------
    // 1. Calculate the workload of each operator.
    //--------------------------------------------------------------------------

    scheduledOperatorsMap.clear();

    if (!application) {
        SPDLOG_WARN("No application registered");
        return;
    }
    auto& appNodes = application->getNodes();
    if (appNodes.empty()) {
        SPDLOG_WARN("No nodes recorded in the application");
        return;
    }

    // For Binpack, weight each operator's chained calls by their estimated CPU
    // cost so remote-heavy operators get more workers. The coefficient is the
    // cluster-average per-call cost (alpha·localShare + beta·(1-localShare))
    // normalised by t_e, derived from the live metrics. Other modes ignore it.
    double chainedCostCoeff = 0.0;
    if (scheduleMode == 0 && metrics != nullptr) {
        chainedCostCoeff =
          computeChainedCostCoeff(metrics->getScalingSignals(kChainedCostWindowSec));
    }

    // TODO - scale the number of hosts.
    application->quantiseResources(hostMap.size(), scheduleMode, chainedCostCoeff);
    application->showConnections();

    if (scheduleMode == 7) {
        rescheduleAppFaaSFlow(hostMap);
        return;
    }

    if (scheduleMode == 3) {
        rescheduleAppFaaSFlowAdaptive(hostMap, metrics);
        return;
    }

    if (scheduleMode == 0) {
        rescheduleAppBinpack(hostMap);
        return;
    }

    if (scheduleMode == 11) {
        rescheduleAppStepConf(hostMap);
        return;
    }

    SPDLOG_INFO("StateAwareScheduler: Reschedule the application according to "
                "the metrics");

    //--------------------------------------------------------------------------
    // 2. Partition the application into sub-groups.
    //--------------------------------------------------------------------------

    std::vector<NodeGroup> groups;
    std::unordered_set<std::string> visited;

    for (const auto& inputNode : application->getInputNodes()) {
        groupNodesStrictHelper(inputNode, groups, visited);
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
    // Each node starts as its own group. If reqResource > 1.0 the load is
    // spread across multiple workers (cap 1.0 per worker).
    for (const auto& [nodeName, node] : appNodes) {
        groups.push_back({ { node }, NONE_STRING });

        std::map<std::string, double> nodeAllocation;
        double remaining = node->reqResource;

        while (remaining > 1e-9) {
            // Find the worker with most available capacity not yet used by
            // this node.
            std::string bestWorker;
            double bestAvail = std::numeric_limits<double>::lowest();
            for (const auto& [ip, avail] : workerRemaining) {
                if (nodeAllocation.count(ip))
                    continue;
                if (avail > bestAvail) {
                    bestAvail = avail;
                    bestWorker = ip;
                }
            }
            if (bestWorker.empty())
                break;

            double allocation = std::min(1.0, remaining);
            nodeAllocation[bestWorker] = allocation;
            workerRemaining[bestWorker] -= allocation;
            remaining -= allocation;
        }

        groupAllocations.push_back(std::move(nodeAllocation));
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

std::tuple<std::vector<NodeGroup>,
           std::vector<std::map<std::string, double>>,
           std::map<std::string, double>>
StateAwareScheduler::groupNodesTopo(const HostMap& hostMap)
{
    SPDLOG_INFO("Maping nodes to operators based on topology");
    if (hostMap.empty()) {
        SPDLOG_ERROR("Host map is empty, cannot schedule.");
        throw std::runtime_error("Host map is empty");
    }
    auto appNodes = application->getNodesDFSOrder();

    std::vector<NodeGroup> groups;
    std::vector<std::map<std::string, double>> groupAllocations;
    std::map<std::string, double> workerRemaining;

    for (const auto& [ip, host] : hostMap) {
        workerRemaining[ip] = 1.0;
    }

    // Contiguous Binpack layout via the shared helper, keyed by slot index;
    // map each slot index back to its IP (hostMap iteration order).
    std::vector<std::string> ipByIdx;
    ipByIdx.reserve(hostMap.size());
    for (const auto& [ip, host] : hostMap) {
        ipByIdx.push_back(ip);
    }

    std::vector<std::pair<std::string, double>> ordered;
    ordered.reserve(appNodes.size());
    for (const auto& [nodeName, node] : appNodes) {
        ordered.emplace_back(nodeName, node->reqResource);
    }

    auto placementByIdx =
      binpackTapeFill(ordered, static_cast<int>(hostMap.size()));

    // Rebuild the per-node IP-keyed allocations and the leftover capacity.
    for (const auto& [nodeName, node] : appNodes) {
        std::map<std::string, double> currentNodeAllocation;
        auto pit = placementByIdx.find(nodeName);
        if (pit != placementByIdx.end()) {
            for (const auto& [idx, amount] : pit->second) {
                const std::string& ip = ipByIdx[idx];
                currentNodeAllocation[ip] += amount;
                workerRemaining[ip] -= amount;
            }
        }
        groups.push_back({ { node }, NONE_STRING });
        groupAllocations.push_back(currentNodeAllocation);
    }
    if (groups.empty()) {
        SPDLOG_ERROR("No groups formed after topo grouping");
        throw std::runtime_error("No groups formed after topo grouping");
    }

    SPDLOG_INFO("--- Final Topo Grouping Results (Total Groups: {}) ---",
                groups.size());

    for (size_t i = 0; i < groups.size(); ++i) {
        const auto& group = groups[i];
        const auto& allocation = groupAllocations[i];

        auto& nodes = std::get<0>(group);
        if (nodes.empty()) {
            SPDLOG_WARN("Group {} has no nodes", i + 1);
            continue;
        }
        // Currently, one node per group
        std::string nodeName = nodes[0]->name;
        SPDLOG_INFO("Group {}: Node=[{}]", i + 1, nodeName);
        SPDLOG_INFO("  -> Worker Allocations: {}", allocation);
    }
    return { groups, groupAllocations, workerRemaining };
}

// Greedy contiguous bin-fill — the deterministic core of the Binpack layout.
// Lay each (key, resource) in `ordered` onto a tape of `numSlots`
// unit-capacity slots, the slot cursor persisting across keys so that adjacent
// operators land on overlapping slots. Returns key -> { slotIndex -> amount
// placed on that slot }. Mirrors the fill loop in groupNodesTopo, including the
// floating-point-dust recovery onto the last slot. Used by both groupNodesTopo
// (which maps slot index -> IP) and predictBinpackLocalShare.
static std::map<std::string, std::map<int, double>> binpackTapeFill(
  const std::vector<std::pair<std::string, double>>& ordered,
  int numSlots)
{
    std::map<std::string, std::map<int, double>> placement;
    if (numSlots <= 0) {
        return placement;
    }

    std::vector<double> slotRemaining(numSlots, 1.0);
    const double TOLERANCE = 1e-9;
    int it = 0;
    for (const auto& [key, resource] : ordered) {
        double remainReq = resource;
        while (remainReq > TOLERANCE) {
            if (it >= numSlots) {
                // Floating-point dust: assign to the last slot.
                placement[key][numSlots - 1] += remainReq;
                slotRemaining[numSlots - 1] -= remainReq;
                remainReq = 0.0;
                break;
            }
            double avail = slotRemaining[it];
            if (avail > TOLERANCE) {
                if (avail >= remainReq) {
                    placement[key][it] += remainReq;
                    slotRemaining[it] -= remainReq;
                    remainReq = 0.0;
                } else {
                    placement[key][it] += avail;
                    remainReq -= avail;
                    slotRemaining[it] = 0.0;
                    ++it;
                }
            } else {
                ++it;
            }
        }
    }
    return placement;
}

std::map<std::string, std::map<int, double>>
StateAwareScheduler::computeBinpackPlacement(int numHosts) const
{
    std::map<std::string, std::map<int, double>> placement;
    if (!application || numHosts <= 0) {
        return placement;
    }

    auto& nodes = application->getNodes();
    if (nodes.empty()) {
        return placement;
    }

    //--------------------------------------------------------------------------
    // Step 1: read each operator's preWorkload — the N-independent share the
    // Binpack quantiser uses, computed at the last reschedule. Floored to 1.0
    // to match computePreWorkloads (and to stay well-defined before the first
    // reschedule, where it degrades to a uniform split). Read-only.
    //--------------------------------------------------------------------------
    std::map<std::string, double> preWorkloads;
    for (const auto& [name, node] : nodes) {
        double pw = node->preWorkload;
        if (pw < 1.0) {
            pw = 1.0;
        }
        preWorkloads[name] = pw;
    }

    //--------------------------------------------------------------------------
    // Step 2: quantise to per-operator reqResource at 0.1-worker granularity,
    // reusing the exact same routine the real Binpack scheduler runs.
    //--------------------------------------------------------------------------
    std::map<std::string, double> reqResource;
    try {
        reqResource =
          Application::quantiseFromPreWorkloads(preWorkloads, numHosts);
    } catch (const std::exception& e) {
        // quantiseFromPreWorkloads throws if it cannot rebalance to 0.1 units.
        // This is only a prediction, so degrade gracefully to the fallback.
        SPDLOG_WARN("computeBinpackPlacement: quantisation failed: {}",
                    e.what());
        return {};
    }

    //--------------------------------------------------------------------------
    // Step 3: lay operators out contiguously on a tape of `numHosts` workers in
    // DFS order, reusing the shared binpackTapeFill helper (the deterministic
    // Binpack layout). placement[op] = { workerIdx -> fraction of op }.
    //--------------------------------------------------------------------------
    std::vector<std::pair<std::string, double>> ordered;
    {
        std::set<std::string> visited;
        const auto& conns = application->getConnections();
        std::function<void(const std::string&)> visit =
          [&](const std::string& n) {
              visited.insert(n);
              auto rIt = reqResource.find(n);
              if (rIt != reqResource.end()) {
                  ordered.emplace_back(n, rIt->second);
              }
              auto cIt = conns.find(n);
              if (cIt != conns.end()) {
                  for (const auto& succ : cIt->second) {
                      if (!visited.count(succ)) {
                          visit(succ);
                      }
                  }
              }
          };
        for (const auto& input : application->getInputNodes()) {
            if (!visited.count(input)) {
                visit(input);
            }
        }
    }

    placement = binpackTapeFill(ordered, numHosts);

    // Normalise each operator's per-worker amounts into fractions summing to 1.
    for (auto& [name, wmap] : placement) {
        double sum = 0.0;
        for (const auto& [w, amt] : wmap) {
            sum += amt;
        }
        if (sum > 0.0) {
            for (auto& [w, amt] : wmap) {
                amt /= sum;
            }
        }
    }

    return placement;
}

double StateAwareScheduler::predictBinpackLocalShare(int numHosts) const
{
    auto placement = computeBinpackPlacement(numHosts);
    if (placement.empty()) {
        return -1.0;
    }

    //--------------------------------------------------------------------------
    // Weighted local share over chained edges. For an edge A->B the
    // source/destination hosts are approximately independent (round-robin /
    // apportion for stateless, hash for partitioned stateful), so the local
    // probability is Σ_w A_w·B_w. Weight each edge by its observed chained call
    // count.
    //--------------------------------------------------------------------------
    auto weightedEdges = application->getConnectionsWithWeight();
    double weightedLocal = 0.0;
    double weightedTotal = 0.0;
    for (const auto& edge : weightedEdges) {
        if (edge.weight <= 0) {
            continue;
        }
        auto aIt = placement.find(edge.input);
        auto bIt = placement.find(edge.output);
        if (aIt == placement.end() || bIt == placement.end()) {
            continue;
        }
        double localFrac = 0.0;
        for (const auto& [w, aFrac] : aIt->second) {
            auto bw = bIt->second.find(w);
            if (bw != bIt->second.end()) {
                localFrac += aFrac * bw->second;
            }
        }
        weightedLocal += edge.weight * localFrac;
        weightedTotal += edge.weight;
    }

    if (weightedTotal <= 0.0) {
        return -1.0;
    }
    return weightedLocal / weightedTotal;
}

std::vector<double> StateAwareScheduler::predictBinpackWorkerLoads(
  int numHosts,
  double totalLoad,
  double tE,
  double alpha,
  double beta,
  double chainedRatio) const
{
    auto placement = computeBinpackPlacement(numHosts);
    if (placement.empty() || numHosts <= 0 || totalLoad <= 0.0) {
        return {};
    }

    //--------------------------------------------------------------------------
    // Per-operator processing share == preWorkload share, floored to 1.0 to
    // match computeBinpackPlacement. An operator's absolute rate is then
    // totalLoad·share, so the per-worker process rates sum back to totalLoad.
    //--------------------------------------------------------------------------
    auto& nodes = application->getNodes();
    std::map<std::string, double> preWorkloads;
    double preWorkloadSum = 0.0;
    for (const auto& [name, node] : nodes) {
        double pw = node->preWorkload < 1.0 ? 1.0 : node->preWorkload;
        preWorkloads[name] = pw;
        preWorkloadSum += pw;
    }
    if (preWorkloadSum <= 0.0) {
        return {};
    }

    std::vector<double> workerLoads(numHosts, 0.0);

    //--------------------------------------------------------------------------
    // Process cost: each operator's absolute rate (totalLoad · its preWorkload
    // share) attributed to workers by its placement fractions, times tE.
    //--------------------------------------------------------------------------
    for (const auto& [name, wmap] : placement) {
        auto pwIt = preWorkloads.find(name);
        if (pwIt == preWorkloads.end()) {
            continue;
        }
        double rateOp = totalLoad * (pwIt->second / preWorkloadSum);
        for (const auto& [w, frac] : wmap) {
            if (w >= 0 && w < numHosts) {
                workerLoads[w] += rateOp * frac * tE;
            }
        }
    }

    //--------------------------------------------------------------------------
    // Chained-call cost: the total chained calls/s (chainedRatio · totalLoad)
    // split across edges by their observed weight, then attributed to the
    // worker running the source operator (share A_w). Each such call is local
    // with probability B_w (destination also on w) and remote otherwise.
    //--------------------------------------------------------------------------
    auto weightedEdges = application->getConnectionsWithWeight();
    double weightTotal = 0.0;
    for (const auto& edge : weightedEdges) {
        if (edge.weight > 0) {
            weightTotal += edge.weight;
        }
    }
    if (weightTotal > 0.0 && chainedRatio > 0.0) {
        double totalChainedPerSec = chainedRatio * totalLoad;
        for (const auto& edge : weightedEdges) {
            if (edge.weight <= 0) {
                continue;
            }
            auto aIt = placement.find(edge.input);
            auto bIt = placement.find(edge.output);
            if (aIt == placement.end() || bIt == placement.end()) {
                continue;
            }
            double edgeCallsPerSec =
              totalChainedPerSec * (edge.weight / weightTotal);
            for (const auto& [w, aFrac] : aIt->second) {
                if (w < 0 || w >= numHosts) {
                    continue;
                }
                double bFrac = 0.0;
                auto bw = bIt->second.find(w);
                if (bw != bIt->second.end()) {
                    bFrac = bw->second;
                }
                double callsOnW = edgeCallsPerSec * aFrac;
                workerLoads[w] += callsOnW * bFrac * alpha;        // local
                workerLoads[w] += callsOnW * (1.0 - bFrac) * beta; // remote
            }
        }
    }

    return workerLoads;
}

double StateAwareScheduler::computeChainedCostCoeff(
  const faabric::planner::ApplicationMetrics::ScalingSignals& signals)
{
    double tE = signals.avgExecTime;
    double R = signals.avgChainedRatio;
    if (tE <= 0.0 || R <= 0.0) {
        return 0.0;
    }

    // Cluster-average fraction of chained calls that stay host-local.
    double localShare = signals.avgLocalChainedRatio / R;
    localShare = std::clamp(localShare, 0.0, 1.0);

    // Blended per-call cost (us), then normalise by t_e so the coefficient is
    // in units of processed tuples (the same unit as Node::processedTuples).
    double perCallCost =
      signals.alpha * localShare + signals.beta * (1.0 - localShare);
    if (perCallCost <= 0.0) {
        return 0.0;
    }
    return perCallCost / tE;
}

void StateAwareScheduler::rescheduleAppBinpack(const HostMap& hostMap)
{
    SPDLOG_INFO("Rescheduling the application in Binpack Mode");
    // 1. Map and assign operators to hosts based on topology.
    auto [groups, groupAllocations, workerRemaining] = groupNodesTopo(hostMap);

    // 2. Arrange the states accordingly.
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
    // 3. Greedy IP remapping per group to minimise state migration.
    //    Since all workers are homogeneous, we can permute IPs within each
    //    group without affecting load distribution.
    //    Score priority: (1) state partition locality (+2 per match),
    //                    (2) identity preference / operator locality (+1).
    //--------------------------------------------------------------------------
    for (size_t gi = 0; gi < groups.size(); ++gi) {
        const auto& groupAllocation = groupAllocations[gi];
        if (groupAllocation.size() <= 1)
            continue;

        std::set<std::string> ipSet;
        for (const auto& [ip, _] : groupAllocation)
            ipSet.insert(ip);

        // scoreMx[newIp][oldIp] = benefit of the bijection φ(newIp)=oldIp.
        // Priority 1 (+1000 per match): stateful state partition stays on its
        // old host. Priority 2 (up to ~999): stateless traffic overlap
        // min(newW,oldW)*10 per op. The multiplier gap ensures state always
        // dominates stateless in the ranking.
        std::map<std::string, std::map<std::string, int>> scoreMx;
        for (const auto& newIp : ipSet)
            for (const auto& oldIp : ipSet)
                scoreMx[newIp][oldIp] = 0;

        // Priority 1: stateful state locality.
        for (const auto& [key, newIp] : newStateHost) {
            if (!ipSet.count(newIp))
                continue;
            auto it = stateHost.find(key);
            if (it != stateHost.end() && ipSet.count(it->second))
                scoreMx[newIp][it->second] += 1000;
        }

        // Priority 2: stateless operator locality.
        // For φ(newIp)=oldIp, score += min(new_weight[newIp],
        // old_weight[oldIp]) * 10 per stateless operator. This favours mappings
        // where the physical host oldIp continues to handle a similar
        // proportion of stateless traffic.
        for (const auto& [funcPar, wMap] : newStatelessReqWeight) {
            bool inGroup =
              std::any_of(wMap.begin(), wMap.end(), [&ipSet](const auto& kv) {
                  return ipSet.count(kv.first) > 0;
              });
            if (!inGroup)
                continue;
            std::string func = funcPar.substr(0, funcPar.rfind('_'));
            auto sopIt = scheduledOperatorsMap.find(func);
            if (sopIt == scheduledOperatorsMap.end())
                continue;
            for (const auto& [newIp, newW] : wMap) {
                if (!ipSet.count(newIp))
                    continue;
                for (const auto& oldIp : ipSet) {
                    double oldW = 0;
                    auto wit = sopIt->second.weightDist.find(oldIp);
                    if (wit != sopIt->second.weightDist.end())
                        oldW = wit->second;
                    scoreMx[newIp][oldIp] +=
                      static_cast<int>(std::min(newW, oldW) * 10);
                }
            }
        }

        // Greedy bipartite matching: maximise Σ scoreMx[newIp][φ(newIp)].
        std::vector<std::tuple<int, std::string, std::string>> cands;
        cands.reserve(ipSet.size() * ipSet.size());
        for (const auto& newIp : ipSet)
            for (const auto& oldIp : ipSet)
                cands.emplace_back(scoreMx[newIp][oldIp], newIp, oldIp);
        std::sort(cands.rbegin(), cands.rend());

        std::map<std::string, std::string> ipRemap;
        std::set<std::string> freeNew = ipSet;
        std::set<std::string> freeOld = ipSet;

        for (const auto& [s, newIp, oldIp] : cands) {
            if (freeNew.count(newIp) && freeOld.count(oldIp)) {
                ipRemap[newIp] = oldIp;
                freeNew.erase(newIp);
                freeOld.erase(oldIp);
            }
        }
        for (const auto& newIp : freeNew) {
            ipRemap[newIp] = *freeOld.begin();
            freeOld.erase(freeOld.begin());
        }

        // Skip groups where the mapping is the identity.
        bool changed = false;
        for (const auto& [k, v] : ipRemap)
            if (k != v) {
                changed = true;
                break;
            }
        if (!changed)
            continue;

        for ([[maybe_unused]] const auto& [k, v] : ipRemap)
            SPDLOG_DEBUG("Binpack group {}: remap {} -> {}", gi, k, v);

        // Apply to newStateHost.
        for (auto& [key, ip] : newStateHost)
            if (ipRemap.count(ip))
                ip = ipRemap.at(ip);

        // Apply to newStatelessReqWeight (IP-keyed weight maps).
        for (auto& [funcPar, wMap] : newStatelessReqWeight) {
            bool inGroup =
              std::any_of(wMap.begin(), wMap.end(), [&ipSet](const auto& kv) {
                  return ipSet.count(kv.first);
              });
            if (!inGroup)
                continue;
            std::map<std::string, double> remapped;
            for (const auto& [ip, w] : wMap)
                remapped[ipRemap.count(ip) ? ipRemap.at(ip) : ip] = w;
            wMap = std::move(remapped);
        }
    }

    //--------------------------------------------------------------------------
    // 4. Build the scheduledOperatorsMap.
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

void StateAwareScheduler::rescheduleAppFaaSFlow(const HostMap& hostMap)
{
    SPDLOG_INFO("Rescheduling the application in FaaSFlow Mode");

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

void StateAwareScheduler::rescheduleAppFaaSFlowAdaptive(
  const HostMap& hostMap,
  const faabric::planner::ApplicationMetrics* metrics)
{
    SPDLOG_INFO("Rescheduling the application in FaaSFlow Adaptive Mode");

    const int numHosts = static_cast<int>(hostMap.size());

    double avgInputRate = 0.0;
    if (metrics) {
        auto signals = metrics->getScalingSignals(3);
        avgInputRate = signals.avgInputRate;
        SPDLOG_INFO("Adaptive: avgInputRate(3s)={:.1f}", avgInputRate);
    }

    int usedHostCount =
      metrics ? metrics->computeAdaptiveHostCount(avgInputRate, numHosts)
              : numHosts;
    SPDLOG_INFO("Adaptive: usedHostCount={}", usedHostCount);

    // Distribute reqResource proportionally to each operator's processedTuples.
    if (avgInputRate > 0.0) {
        double totalRequired = static_cast<double>(usedHostCount);
        double totalProcessed = 0.0;
        for (const auto& [name, node] : application->getNodes())
            totalProcessed +=
              static_cast<double>(std::max(1L, node->processedTuples));
        for (auto& [name, node] : application->getNodes()) {
            double frac =
              static_cast<double>(std::max(1L, node->processedTuples)) /
              totalProcessed;
            node->reqResource = frac * totalRequired;
        }
    }

    //--------------------------------------------------------------------------
    // Step 3: Build a limited host map with only usedHostCount workers.
    //--------------------------------------------------------------------------
    HostMap limitedHostMap;
    {
        int cnt = 0;
        for (const auto& [ip, host] : hostMap) {
            if (cnt >= usedHostCount)
                break;
            limitedHostMap[ip] = host;
            ++cnt;
        }
    }

    //--------------------------------------------------------------------------
    // Step 4: Greedy grouping on the limited host map.
    //--------------------------------------------------------------------------
    auto [groups, groupAllocations, workerRemaining] =
      groupNodesGreedily(limitedHostMap);

    {
        std::stringstream ss;
        for (size_t i = 0; i < groups.size(); ++i) {
            const auto& [nodes, partition] = groups[i];
            if (i > 0)
                ss << "; ";
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
        SPDLOG_INFO("Adaptive groups:\n {}", ss.str());
    }

    //--------------------------------------------------------------------------
    // Step 5: Allocate any unused workers (within limitedHostMap) to groups.
    //--------------------------------------------------------------------------
    for (const auto& [ip, host] : limitedHostMap) {
        if (workerRemaining.at(ip) != 1.0)
            continue;

        int targetIdx = -1;
        double maxResource = std::numeric_limits<double>::lowest();
        for (int i = 0; i < static_cast<int>(groupAllocations.size()); ++i) {
            for (const auto& [workerIp, resource] : groupAllocations[i]) {
                if (resource >= maxResource) {
                    maxResource = resource;
                    targetIdx = i;
                }
            }
        }

        if (targetIdx != -1) {
            double totalResource = 0;
            for (const auto& [_, resource] : groupAllocations[targetIdx])
                totalResource += resource;
            groupAllocations[targetIdx][ip] = 0.0;
            double newEvenShare =
              totalResource / groupAllocations[targetIdx].size();
            for (auto& [workerIp, allocatedResource] :
                 groupAllocations[targetIdx]) {
                workerRemaining[workerIp] += allocatedResource;
                allocatedResource = newEvenShare;
                workerRemaining[workerIp] -= newEvenShare;
            }
        }
    }

    //--------------------------------------------------------------------------
    // Step 6: Arrange states.
    //--------------------------------------------------------------------------
    std::map<std::string, std::string> newStateHost;
    std::map<std::string, int> newFunctionParallelism;
    std::map<std::string, std::map<std::string, double>> newStatelessReqWeight;
    std::map<std::string, std::map<int, double>> newParStateReqWeight;

    for (size_t groupIndex = 0; groupIndex < groups.size(); ++groupIndex) {
        const auto& group = std::get<0>(groups[groupIndex]);
        auto groupAllocation = groupAllocations[groupIndex];

        double groupResource = 0;
        for (const auto& node : group)
            groupResource += node->reqResource;

        for (const auto& node : group) {
            std::string userFunc = node->name;
            if (node->type == STATELESS) {
                double scale = node->reqResource / groupResource;
                std::map<std::string, double> nodeAllocation;
                for (const auto& [ip, groupWeight] : groupAllocation) {
                    double nodeAlloc = groupWeight * scale;
                    if (nodeAlloc > 0)
                        nodeAllocation[ip] = nodeAlloc;
                }
                newStatelessReqWeight[userFunc + "_0"] = nodeAllocation;
            } else if (node->type == PARTITIONED_STATEFUL) {
                int index = 0;
                newFunctionParallelism[userFunc] = groupAllocation.size();
                double scale = node->reqResource / groupResource;
                for (const auto& [ip, groupWeight] : groupAllocation) {
                    std::string userFuncPar =
                      userFunc + "_" + std::to_string(index);
                    newParStateReqWeight[userFunc][index] = groupWeight * scale;
                    newStateHost[userFuncPar] = ip;
                    ++index;
                }
            } else if (node->type == STATEFUL) {
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
    // Step 7: Build scheduledOperatorsMap and commit.
    //--------------------------------------------------------------------------
    stateHost = newStateHost;
    functionParallelism = newFunctionParallelism;

    scheduledOperatorsMap.clear();
    for (int groupId = 0; groupId < static_cast<int>(groups.size());
         ++groupId) {
        auto grpMap =
          buildScheduledOperatorsForGroup(groupId,
                                          std::get<0>(groups[groupId]),
                                          {},
                                          newStatelessReqWeight,
                                          newParStateReqWeight);
        scheduledOperatorsMap.insert(grpMap.begin(), grpMap.end());
    }

    runtimeSummary.initScheduledOperators(
      *application, scheduledOperatorsMap, true, scheduleMode);

    stateHashRing.clear();

    redis::Redis& redis = redis::Redis::getState();
    redis.flushAll();
    for (const auto& [stateName, ip] : stateHost)
        registerStateToRedis(stateName, ip);

    for (const auto& [userFunction, partitionBy] : statePartitionBy) {
        if (functionParallelism.find(userFunction) ==
            functionParallelism.end()) {
            SPDLOG_ERROR("Function {} has no parallelism", userFunction);
            throw std::runtime_error("Function parallelism not found");
        }
        auto weightDist = newParStateReqWeight[userFunction];
        stateHashRing[userFunction] =
          std::make_shared<faabric::util::ConsistentHashRing>(weightDist);
    }

    printScheduleInfomation();
}

void StateAwareScheduler::rescheduleAppStepConf(const HostMap& hostMap)
{
    SPDLOG_INFO("Rescheduling the application in StepConf Mode");

    // DoP is already set by quantiseResources(): di ∝ αi (same as Binpack).

    //--------------------------------------------------------------------------
    // Step 1: Critical-path DP on the operator DAG.
    //   critWeight[v] = reqResource[v] + max(critWeight[pred] for pred of v)
    // We traverse in DFS topological order, which guarantees predecessors are
    // processed before any successor.
    //--------------------------------------------------------------------------
    auto appNodes = application->getNodesDFSOrder();

    std::map<std::string, double> critWeight;
    std::map<std::string, std::string> critPred;

    for (const auto& [nodeName, node] : appNodes) {
        double maxPredW = 0.0;
        std::string bestPred;
        for (const auto& predNode : application->getSource(nodeName)) {
            auto it = critWeight.find(predNode->name);
            if (it != critWeight.end() && it->second > maxPredW) {
                maxPredW = it->second;
                bestPred = predNode->name;
            }
        }
        critWeight[nodeName] = node->reqResource + maxPredW;
        critPred[nodeName] = bestPred;
    }

    // Find the sink with the maximum critical weight.
    std::string sinkName;
    double maxCrit = -1.0;
    for (const auto& [name, w] : critWeight) {
        if (w > maxCrit) {
            maxCrit = w;
            sinkName = name;
        }
    }

    // Reconstruct critical path (sink → source, stored in reverse so we can
    // mark members quickly with a set).
    std::unordered_set<std::string> critSet;
    {
        std::string cur = sinkName;
        while (!cur.empty()) {
            critSet.insert(cur);
            cur = critPred.count(cur) ? critPred.at(cur) : "";
        }
    }

    SPDLOG_INFO("StepConf: critical path length={:.3f}, {} nodes",
                maxCrit,
                critSet.size());

    //--------------------------------------------------------------------------
    // Step 2: Reorder operators — critical path first, then the rest.
    //   Both sub-lists maintain their original DFS topological order.
    //--------------------------------------------------------------------------
    std::vector<std::pair<std::string, std::shared_ptr<Node>>> orderedNodes;
    orderedNodes.reserve(appNodes.size());

    // Pass 1: critical path nodes in topo order.
    for (const auto& [name, node] : appNodes) {
        if (critSet.count(name))
            orderedNodes.emplace_back(name, node);
    }
    // Pass 2: non-critical nodes in topo order.
    for (const auto& [name, node] : appNodes) {
        if (!critSet.count(name))
            orderedNodes.emplace_back(name, node);
    }

    //--------------------------------------------------------------------------
    // Step 3: Sequential bin-packing in critical-path-first order.
    //   Identical fill logic to groupNodesTopo(), but operating on
    //   orderedNodes.
    //--------------------------------------------------------------------------
    std::vector<NodeGroup> groups;
    std::vector<std::map<std::string, double>> groupAllocations;
    std::map<std::string, double> workerRemaining;

    for (const auto& [ip, host] : hostMap)
        workerRemaining[ip] = 1.0;

    auto it = hostMap.begin();
    const double TOLERANCE = 1e-9;

    for (const auto& [nodeName, node] : orderedNodes) {
        std::map<std::string, double> currentNodeAllocation;
        double remainReq = node->reqResource;

        while (remainReq > TOLERANCE) {
            if (it == hostMap.end()) {
                // Floating-point dust: absorb into the last host.
                std::string lastIp = hostMap.rbegin()->first;
                SPDLOG_WARN(
                  "StepConf: FP drift {:.2e} -> {}", remainReq, lastIp);
                currentNodeAllocation[lastIp] += remainReq;
                workerRemaining[lastIp] -= remainReq;
                remainReq = 0.0;
                continue;
            }
            std::string ip = it->first;
            double avail = workerRemaining[ip];
            if (avail > TOLERANCE) {
                if (avail >= remainReq) {
                    currentNodeAllocation[ip] += remainReq;
                    workerRemaining[ip] -= remainReq;
                    remainReq = 0.0;
                } else {
                    currentNodeAllocation[ip] += avail;
                    remainReq -= avail;
                    workerRemaining[ip] = 0.0;
                    ++it;
                }
            } else {
                ++it;
            }
        }
        groups.push_back({ { node }, NONE_STRING });
        groupAllocations.push_back(currentNodeAllocation);
    }

    //--------------------------------------------------------------------------
    // Step 4: Assign state hosts (same logic as rescheduleAppBinpack Step 2).
    //--------------------------------------------------------------------------
    std::map<std::string, std::string> newStateHost;
    std::map<std::string, int> newFunctionParallelism;
    std::map<std::string, std::map<std::string, double>> newStatelessReqWeight;
    std::map<std::string, std::map<int, double>> newParStateReqWeight;

    for (size_t gi = 0; gi < groups.size(); ++gi) {
        const auto& group = std::get<0>(groups[gi]);
        auto groupAllocation = groupAllocations[gi];

        double groupResource = 0;
        for (const auto& n : group)
            groupResource += n->reqResource;

        for (const auto& node : group) {
            std::string userFunc = node->name;
            if (node->type == STATELESS) {
                double nodeReqResource = node->reqResource;
                double scale = nodeReqResource / groupResource;
                std::map<std::string, double> nodeAllocation;
                for (const auto& [ip, groupWeight] : groupAllocation) {
                    double alloc = groupWeight * scale;
                    if (alloc > 0)
                        nodeAllocation[ip] = alloc;
                }
                newStatelessReqWeight[userFunc + "_0"] = nodeAllocation;
            } else if (node->type == PARTITIONED_STATEFUL) {
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
    // Step 5: Greedy IP remapping per group to minimise state migration.
    //   Copied verbatim from rescheduleAppBinpack Step 3.
    //--------------------------------------------------------------------------
    for (size_t gi = 0; gi < groups.size(); ++gi) {
        const auto& groupAllocation = groupAllocations[gi];
        if (groupAllocation.size() <= 1)
            continue;

        std::set<std::string> ipSet;
        for (const auto& [ip, _] : groupAllocation)
            ipSet.insert(ip);

        std::map<std::string, std::map<std::string, int>> scoreMx;
        for (const auto& newIp : ipSet)
            for (const auto& oldIp : ipSet)
                scoreMx[newIp][oldIp] = 0;

        // Priority 1: stateful state locality (+1000 per partition match).
        for (const auto& [key, newIp] : newStateHost) {
            if (!ipSet.count(newIp))
                continue;
            auto sit = stateHost.find(key);
            if (sit != stateHost.end() && ipSet.count(sit->second))
                scoreMx[newIp][sit->second] += 1000;
        }

        // Priority 2: stateless operator locality.
        for (const auto& [funcPar, wMap] : newStatelessReqWeight) {
            bool inGroup =
              std::any_of(wMap.begin(), wMap.end(), [&ipSet](const auto& kv) {
                  return ipSet.count(kv.first) > 0;
              });
            if (!inGroup)
                continue;
            std::string func = funcPar.substr(0, funcPar.rfind('_'));
            auto sopIt = scheduledOperatorsMap.find(func);
            if (sopIt == scheduledOperatorsMap.end())
                continue;
            for (const auto& [newIp, newW] : wMap) {
                if (!ipSet.count(newIp))
                    continue;
                for (const auto& oldIp : ipSet) {
                    double oldW = 0;
                    auto wit = sopIt->second.weightDist.find(oldIp);
                    if (wit != sopIt->second.weightDist.end())
                        oldW = wit->second;
                    scoreMx[newIp][oldIp] +=
                      static_cast<int>(std::min(newW, oldW) * 10);
                }
            }
        }

        // Greedy bipartite matching.
        std::vector<std::tuple<int, std::string, std::string>> cands;
        cands.reserve(ipSet.size() * ipSet.size());
        for (const auto& newIp : ipSet)
            for (const auto& oldIp : ipSet)
                cands.emplace_back(scoreMx[newIp][oldIp], newIp, oldIp);
        std::sort(cands.rbegin(), cands.rend());

        std::map<std::string, std::string> ipRemap;
        std::set<std::string> freeNew = ipSet;
        std::set<std::string> freeOld = ipSet;
        for (const auto& [s, newIp, oldIp] : cands) {
            if (freeNew.count(newIp) && freeOld.count(oldIp)) {
                ipRemap[newIp] = oldIp;
                freeNew.erase(newIp);
                freeOld.erase(oldIp);
            }
        }
        for (const auto& nip : freeNew) {
            ipRemap[nip] = *freeOld.begin();
            freeOld.erase(freeOld.begin());
        }

        bool changed = false;
        for (const auto& [k, v] : ipRemap)
            if (k != v) {
                changed = true;
                break;
            }
        if (!changed)
            continue;

        for ([[maybe_unused]] const auto& [k, v] : ipRemap)
            SPDLOG_DEBUG("StepConf group {}: remap {} -> {}", gi, k, v);

        for (auto& [key, ip] : newStateHost)
            if (ipRemap.count(ip))
                ip = ipRemap.at(ip);

        for (auto& [funcPar, wMap] : newStatelessReqWeight) {
            bool inGroup =
              std::any_of(wMap.begin(), wMap.end(), [&ipSet](const auto& kv) {
                  return ipSet.count(kv.first);
              });
            if (!inGroup)
                continue;
            std::map<std::string, double> remapped;
            for (const auto& [ip, w] : wMap)
                remapped[ipRemap.count(ip) ? ipRemap.at(ip) : ip] = w;
            wMap = std::move(remapped);
        }
    }

    //--------------------------------------------------------------------------
    // Step 6: Commit and publish.
    //--------------------------------------------------------------------------
    stateHost = newStateHost;
    functionParallelism = newFunctionParallelism;

    scheduledOperatorsMap.clear();
    for (int gi = 0; gi < (int)groups.size(); ++gi) {
        auto grpMap = buildScheduledOperatorsForGroup(gi,
                                                      std::get<0>(groups[gi]),
                                                      {},
                                                      newStatelessReqWeight,
                                                      newParStateReqWeight);
        scheduledOperatorsMap.insert(grpMap.begin(), grpMap.end());
    }

    runtimeSummary.initScheduledOperators(
      *application, scheduledOperatorsMap, true, scheduleMode);

    stateHashRing.clear();
    redis::Redis& redis = redis::Redis::getState();
    redis.flushAll();
    for (const auto& [stateName, ip] : stateHost)
        registerStateToRedis(stateName, ip);

    for (const auto& [userFunction, partitionBy] : statePartitionBy) {
        if (!functionParallelism.count(userFunction)) {
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

bool StateAwareScheduler::getRuntimeReconfig() const
{
    return runtimeReconfig;
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