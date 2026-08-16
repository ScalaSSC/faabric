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
        if (scheduleMode == 0 || scheduleMode == 5 ||  scheduleMode == 11) {
            host = scheduleStatelessMessageApportion(userFunc, hostMap, msg);
        } else {
            // when scheduleMode is 3, 4, 7, we use round robin
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

    if (isClastPlacement && (scheduleMode == 3 || scheduleMode == 4)) {
        application->quantiseResources(hostMap.size(), 0);
        rescheduleAppBinpack(hostMap);
        return;
    }

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
    faabric::planner::ApplicationMetrics::ScalingSignals signals;
    bool haveSignals = metrics != nullptr;
    if (haveSignals) {
        signals = metrics->getScalingSignals(kChainedCostWindowSec);
    }
    // The CLAST-placement hybrid lays modes 3/4 out with the mode-0 packer, so
    // it needs the mode-0 chained-call pricing too.
    bool clastPlacement =
      isClastPlacement && (scheduleMode == 3 || scheduleMode == 4);
    double chainedCostCoeff = 0.0;
    if ((scheduleMode == 0 || clastPlacement) && haveSignals) {
        chainedCostCoeff = computeChainedCostCoeff(signals);
    }

    // TODO - scale the number of hosts.
    application->quantiseResources(hostMap.size(), scheduleMode, chainedCostCoeff);
    application->showConnections();

    if (clastPlacement) {
        rescheduleAppClastPlacement(
          hostMap, metrics, haveSignals ? &signals : nullptr, chainedCostCoeff);
        return;
    }

    if (scheduleMode == 7) {
        rescheduleAppFaaSFlow(hostMap);
        return;
    }

    if (scheduleMode == 3) {
        rescheduleAppFaaSFlowAdaptive(hostMap, metrics);
        return;
    }

    if (scheduleMode == 4) {
        rescheduleAppNonlinear(hostMap, metrics);
        return;
    }

    if (scheduleMode == 0) {
        rescheduleAppBinpack(
          hostMap, haveSignals ? &signals : nullptr, metrics);
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

namespace {

// One chained edge a->b carrying `calls` chained calls per second (absolute).
struct CapEdge
{
    std::string a;
    std::string b;
    double calls;
};

// Exact per-worker CPU load (us/s) for a placement expressed as ABSOLUTE
// processing load: effProc[op][ip] = op's processing cost (us/s) landing on
// worker ip. This is the four-term CoeffEstimator capacity model:
//   L_k = Σ_op effProc[op][k]
//       + alpha·localCalls_k + beta·remoteCalls_k + gamma·#remoteDestHosts_k
// The per-worker fraction of op is recovered as effProc[op][k]/procDemand[op],
// so this works for PARTIAL placements (an operator only partly placed, or a
// not-yet-placed operator represented by its coarse destination hint). For edge
// a->b a call leaving worker k is local with probability frac_b(k) (destination
// routed proportionally to b's weight) and remote otherwise; gamma is charged
// once per distinct remote destination worker k fans out to.
std::map<std::string, double> capEvalWorkerLoadsProc(
  const std::map<std::string, std::map<std::string, double>>& effProc,
  const std::vector<std::string>& ips,
  const std::map<std::string, double>& procDemand,
  const std::vector<CapEdge>& edges,
  double alpha,
  double beta,
  double gamma)
{
    std::map<std::string, double> load;
    for (const auto& ip : ips) {
        load[ip] = 0.0;
    }

    // Processing cost (effProc is already absolute us/s).
    for (const auto& [op, m] : effProc) {
        for (const auto& [ip, v] : m) {
            load[ip] += v;
        }
    }

    // Chained-call cost + distinct remote dest hosts per sender worker.
    std::map<std::string, std::set<std::string>> remoteDestHosts;
    for (const auto& e : edges) {
        auto ia = effProc.find(e.a);
        if (ia == effProc.end()) {
            continue;
        }
        auto da = procDemand.find(e.a);
        if (da == procDemand.end() || da->second <= 0.0) {
            continue;
        }
        auto ib = effProc.find(e.b);
        double db = 0.0;
        if (ib != effProc.end()) {
            auto it = procDemand.find(e.b);
            if (it != procDemand.end()) {
                db = it->second;
            }
        }
        for (const auto& [ipA, vA] : ia->second) {
            double fracA = vA / da->second;
            if (fracA <= 0.0) {
                continue;
            }
            double bFrac = 0.0;
            if (ib != effProc.end() && db > 0.0) {
                auto f = ib->second.find(ipA);
                if (f != ib->second.end()) {
                    bFrac = f->second / db;
                }
            }
            double callsOnA = e.calls * fracA;
            load[ipA] += callsOnA * bFrac * alpha;         // local
            load[ipA] += callsOnA * (1.0 - bFrac) * beta;  // remote
            if (ib != effProc.end() && db > 0.0) {
                for (const auto& [ipB, vB] : ib->second) {
                    if (ipB != ipA && vB > 0.0) {
                        remoteDestHosts[ipA].insert(ipB);
                    }
                }
            }
        }
    }
    if (gamma > 0.0) {
        for (const auto& [ip, hosts] : remoteDestHosts) {
            load[ip] += gamma * static_cast<double>(hosts.size());
        }
    }

    return load;
}

} // namespace

std::tuple<std::vector<NodeGroup>,
           std::vector<std::map<std::string, double>>,
           std::map<std::string, double>>
StateAwareScheduler::groupNodesCapacity(
  const HostMap& hostMap,
  const faabric::planner::ApplicationMetrics::ScalingSignals& sig)
{
    SPDLOG_INFO("Mapping nodes to workers with capacity-aware open-ended "
                "Binpack (mode 0)");

    // Deterministic worker order; workers are opened from this list on demand.
    std::vector<std::string> allIps;
    allIps.reserve(hostMap.size());
    for (const auto& [ip, host] : hostMap) {
        allIps.push_back(ip);
    }
    const int maxHosts = static_cast<int>(allIps.size());

    // ---- Cost model (us, us/s) from the live estimator -----------------------
    const double W = sig.coeffC;
    const double tE = sig.avgExecTime;
    const double alpha = std::max(0.0, sig.alpha);
    const double beta = std::max(0.0, sig.beta);
    const double gamma = std::max(0.0, sig.gamma);
    const double R = std::max(0.0, sig.avgChainedRatio);
    const double budget = W * capacityHeadroom;
    const double EPS = 1e-9;

    // ---- Per-operator absolute processing demand (us/s) ----------------------
    // rate_o = totalLoad · (processedTuples_o / Σ processedTuples), with
    // totalLoad = avgInputRate · (Σ processedTuples / Σ input processedTuples)
    // (= avgInputRate scaled to total processing across the DAG). procDemand =
    // rate_o · t_e. This mirrors the units used by predictBinpackWorkerLoads, so
    // L_k is directly comparable to the budget W. Processing share is taken from
    // raw processedTuples (NOT the chained-weighted preWorkload) because chained
    // cost is modelled explicitly here and must not be double-counted.
    auto& nodes = application->getNodes();
    double totalTuples = 0.0;
    double inputTuples = 0.0;
    for (const auto& [name, node] : nodes) {
        totalTuples += static_cast<double>(std::max<long>(1, node->processedTuples));
    }
    for (const auto& inName : application->getInputNodes()) {
        auto it = nodes.find(inName);
        if (it != nodes.end()) {
            inputTuples +=
              static_cast<double>(std::max<long>(1, it->second->processedTuples));
        }
    }
    double totalLoad = sig.avgInputRate;
    if (inputTuples > 0.0) {
        totalLoad = sig.avgInputRate * (totalTuples / inputTuples);
    }

    std::map<std::string, double> procDemand;
    std::map<std::string, double> procShare;
    for (const auto& [name, node] : nodes) {
        double share =
          static_cast<double>(std::max<long>(1, node->processedTuples)) /
          totalTuples;
        procShare[name] = share;
        procDemand[name] = totalLoad * share * tE;
    }

    // ---- Chained edges with absolute call rates (calls/s) --------------------
    auto edgesW = application->getConnectionsWithWeight();
    double sumW = 0.0;
    for (const auto& e : edgesW) {
        if (e.weight > 0) {
            sumW += e.weight;
        }
    }
    const double totalChained = R * totalLoad;
    std::vector<CapEdge> edges;
    std::map<std::string, double> edgeShare;
    for (const auto& e : edgesW) {
        if (e.weight <= 0) {
            continue;
        }
        double calls = sumW > 0.0 ? totalChained * (e.weight / sumW) : 0.0;
        edgeShare[e.input + "->" + e.output] =
          sumW > 0.0 ? e.weight / sumW : 0.0;
        edges.push_back({ e.input, e.output, calls });
    }

    // DFS order, used only to emit one group per operator (see below).
    auto topo = application->getNodesDFSOrder();

    // Outgoing-edge index: op -> indices of edges leaving it.
    std::map<std::string, std::vector<int>> succEdges;
    for (int i = 0; i < static_cast<int>(edges.size()); ++i) {
        succEdges[edges[i].a].push_back(i);
    }

    // Reverse-topological order (postorder DFS = successors before the node), so
    // that when we place an operator ALL its chained destinations are already
    // placed. This removes the placement<->locality circular dependency: an
    // operator's outgoing chained cost is exact at decision time, and its
    // incoming edges are charged to predecessors' workers when those are placed
    // later. No coarse / destination-hint stage is needed.
    //
    // At each node we recurse into successors in descending chained-weight order,
    // so the heaviest chain is explored (and therefore laid out) first and most
    // contiguously — it gets the nearest leftover capacity of its successor and
    // thus the best chance of a local edge.
    std::vector<std::string> revTopo;
    {
        std::set<std::string> vis;
        const auto& conns = application->getConnections();
        std::function<void(const std::string&)> post =
          [&](const std::string& n) {
              vis.insert(n);
              // Unique successors of n with their chained-call weight (0 if the
              // edge was filtered out as zero-weight above).
              std::map<std::string, double> succCalls;
              auto sit = succEdges.find(n);
              if (sit != succEdges.end()) {
                  for (int ei : sit->second) {
                      succCalls[edges[ei].b] = edges[ei].calls;
                  }
              }
              auto cit = conns.find(n);
              if (cit != conns.end()) {
                  for (const auto& s : cit->second) {
                      succCalls.emplace(s, 0.0);
                  }
              }
              // Heaviest-traffic successor first.
              std::vector<std::pair<double, std::string>> order;
              order.reserve(succCalls.size());
              for (const auto& [s, c] : succCalls) {
                  order.emplace_back(c, s);
              }
              std::sort(order.rbegin(), order.rend());
              for (const auto& [c, s] : order) {
                  if (!vis.count(s)) {
                      post(s);
                  }
              }
              revTopo.push_back(n);
          };
        for (const auto& in : application->getInputNodes()) {
            if (!vis.count(in)) {
                post(in);
            }
        }
        for (const auto& [name, node] : application->getNodes()) {
            if (!vis.count(name)) {
                post(name);
            }
        }
    }

    // ---- Backward single-pass placement (sink-first) -------------------------
    // Place operators in reverse-topological order so that when an operator is
    // placed all its chained destinations are already fixed; capEvalWorkerLoadsProc
    // is then exact. runBackward(scale) runs this pass with the projected load
    // scaled by `scale` (i.e. at input rate scale·lambda_p): it fills every
    // operator with the analytic max-fill and opens workers on demand, returning
    // the placement, the workers used, and whether it saturated (ran out of
    // workers, or an operator's fan-out alone exceeds the budget).
    auto runBackward =
      [&](double scale)
      -> std::tuple<std::map<std::string, std::map<std::string, double>>,
                    std::vector<std::string>,
                    bool> {
        std::map<std::string, double> D; // demand scaled to scale·lambda_p
        for (const auto& [o, d] : procDemand) {
            D[o] = d * scale;
        }
        std::vector<CapEdge> E = edges; // chained rates scaled likewise
        for (auto& e : E) {
            e.calls *= scale;
        }

        std::map<std::string, std::map<std::string, double>> placeAbs;
        std::vector<std::string> opened;
        int nextHostIdx = 0;
        auto openWorker = [&]() -> std::string {
            if (nextHostIdx >= maxHosts) {
                return "";
            }
            std::string ip = allIps[nextHostIdx++];
            opened.push_back(ip);
            return ip;
        };
        openWorker();

        bool saturated = false;
        for (const auto& name : revTopo) {
            auto dit = D.find(name);
            if (dit == D.end() || dit->second <= 0.0) {
                continue;
            }
            double demand = dit->second;

            // Successors are all placed (reverse-topo): destOfO = the workers any
            // successor occupies = o's potential remote destinations.
            std::set<std::string> destOfO;
            for (int ei : succEdges[name]) {
                auto bit = placeAbs.find(E[ei].b);
                if (bit == placeAbs.end()) {
                    continue;
                }
                for (const auto& [ip, v] : bit->second) {
                    if (v > 0.0) {
                        destOfO.insert(ip);
                    }
                }
            }

            // Base load (everything placed so far, excluding o) and each worker's
            // current remote destination set (for the gamma step).
            auto baseLoads =
              capEvalWorkerLoadsProc(placeAbs, opened, D, E, alpha, beta, gamma);
            std::map<std::string, std::set<std::string>> remoteSets;
            if (gamma > 0.0) {
                for (const auto& e : E) {
                    auto ia = placeAbs.find(e.a);
                    if (ia == placeAbs.end()) {
                        continue;
                    }
                    auto ib = placeAbs.find(e.b);
                    if (ib == placeAbs.end()) {
                        continue;
                    }
                    for (const auto& [ipA, vA] : ia->second) {
                        if (vA <= 0.0) {
                            continue;
                        }
                        for (const auto& [ipB, vB] : ib->second) {
                            if (ipB != ipA && vB > 0.0) {
                                remoteSets[ipA].insert(ipB);
                            }
                        }
                    }
                }
            }

            // Per-worker quantities for placing o on k (constant in the amount p):
            //   aff(k) = Σ_{o->b} c_ob (β-α) sh_b(k),  sh_b(k)=Φ[b][k]/D_b
            //   m_k    = 1 + (1/D_o) Σ_{o->b} c_ob[α sh_b(k)+β(1-sh_b(k))]
            //   γ·G_k  = γ · #new remote dest workers k opens for o
            // Max fit: p_max = (budget - baseLoad_k - γ·G_k) / m_k.
            auto affOf = [&](const std::string& k) {
                double a = 0.0;
                for (int ei : succEdges[name]) {
                    const auto& e = E[ei];
                    auto bit = placeAbs.find(e.b);
                    double db = D.count(e.b) ? D.at(e.b) : 0.0;
                    if (bit == placeAbs.end() || db <= 0.0) {
                        continue;
                    }
                    auto f = bit->second.find(k);
                    if (f != bit->second.end()) {
                        a += (f->second / db) * e.calls * (beta - alpha);
                    }
                }
                return a;
            };
            auto slopeOf = [&](const std::string& k) {
                double m = 1.0;
                for (int ei : succEdges[name]) {
                    const auto& e = E[ei];
                    auto bit = placeAbs.find(e.b);
                    double db = D.count(e.b) ? D.at(e.b) : 0.0;
                    if (bit == placeAbs.end() || db <= 0.0) {
                        continue;
                    }
                    double sh = 0.0;
                    auto f = bit->second.find(k);
                    if (f != bit->second.end()) {
                        sh = f->second / db;
                    }
                    m += (e.calls / demand) * (alpha * sh + beta * (1.0 - sh));
                }
                return m;
            };
            auto gammaStep = [&](const std::string& k) {
                if (gamma <= 0.0) {
                    return 0.0;
                }
                auto rit = remoteSets.find(k);
                int g = 0;
                for (const auto& d : destOfO) {
                    if (d == k) {
                        continue;
                    }
                    if (rit == remoteSets.end() || !rit->second.count(d)) {
                        ++g;
                    }
                }
                return gamma * static_cast<double>(g);
            };
            auto maxFit = [&](const std::string& k, double rem) {
                double avail = budget - baseLoads[k] - gammaStep(k);
                if (avail <= EPS) {
                    return 0.0;
                }
                return std::min(rem, std::max(0.0, avail / slopeOf(k)));
            };

            double rem = demand;
            // Fill opened workers, highest locality first (fills the leftover
            // capacity of successors' workers -> local edges).
            std::vector<std::pair<double, std::string>> order;
            order.reserve(opened.size());
            for (const auto& ip : opened) {
                order.emplace_back(affOf(ip), ip);
            }
            std::sort(order.rbegin(), order.rend());
            for (const auto& [a, ip] : order) {
                if (rem <= EPS) {
                    break;
                }
                double p = maxFit(ip, rem);
                if (p > EPS) {
                    placeAbs[name][ip] += p;
                    rem -= p;
                }
            }
            // Open new workers for the remainder.
            while (rem > EPS) {
                std::string fresh = openWorker();
                if (fresh.empty()) {
                    saturated = true;
                    break;
                }
                baseLoads[fresh] = 0.0;
                double p = maxFit(fresh, rem);
                if (p <= EPS) {
                    saturated = true;
                    break;
                }
                placeAbs[name][fresh] += p;
                rem -= p;
            }
            if (saturated) {
                break;
            }
        }
        return { std::move(placeAbs), std::move(opened), saturated };
    };

    // Place at the requested rate. If the cluster is saturated, binary-search the
    // largest sustainable rate whose backward placement fits. Upper bound = the
    // communication-free capacity ceiling (single-worker throughput × workers =
    // maxHosts·budget/ΣD, as a fraction of the requested rate), capped at the
    // requested rate; stop at a coarse tolerance (100 req/s). The planner then
    // admits input at the found rate; the surplus queues/sheds.
    auto initial = runBackward(1.0);
    auto placeAbs = std::move(std::get<0>(initial));
    auto opened = std::move(std::get<1>(initial));
    bool saturated = std::get<2>(initial);
    double lambdaMult = -1.0; // < 0 means "not saturated"

    // Under sustained overload, reuse the cached max-rate placement from a
    // previous saturation search: it only depends on the capacity model, not
    // on the requested rate, so it stays valid while the model inputs are
    // within tolerance (10% on coefficients, 0.05 on DAG shares, identical
    // host set).
    bool reusedSatCache = false;
    if (saturated && capSatCache.valid) {
        auto relClose = [](double a, double b) {
            double m = std::max({ std::fabs(a), std::fabs(b), 1e-9 });
            return std::fabs(a - b) / m <= 0.1;
        };
        auto mapClose = [](const std::map<std::string, double>& a,
                           const std::map<std::string, double>& b) {
            if (a.size() != b.size()) {
                return false;
            }
            for (const auto& [k, v] : a) {
                auto it = b.find(k);
                if (it == b.end() || std::fabs(v - it->second) > 0.05) {
                    return false;
                }
            }
            return true;
        };
        bool usable =
          capSatCache.hostIps == allIps && relClose(capSatCache.W, W) &&
          relClose(capSatCache.tE, tE) && relClose(capSatCache.alpha, alpha) &&
          relClose(capSatCache.beta, beta) &&
          relClose(capSatCache.gamma, gamma) &&
          relClose(capSatCache.chainedRatio, R) &&
          mapClose(capSatCache.procShare, procShare) &&
          mapClose(capSatCache.edgeShare, edgeShare) &&
          sig.avgInputRate > 0.0 &&
          capSatCache.sustainedAbsRate <= sig.avgInputRate;
        if (usable) {
            placeAbs = capSatCache.placeAbs;
            opened = capSatCache.opened;
            lambdaMult =
              std::min(1.0, capSatCache.sustainedAbsRate / sig.avgInputRate);
            reusedSatCache = true;
            SPDLOG_WARN(
              "Capacity Binpack: cluster SATURATED at requested rate; reusing "
              "cached max-rate placement (~{:.2f}x of requested, ~{:.1f} "
              "req/s) on {} workers",
              lambdaMult,
              capSatCache.sustainedAbsRate,
              opened.size());
        }
    }

    if (saturated && !reusedSatCache) {
        double totalD = 0.0;
        for (const auto& [o, d] : procDemand) {
            totalD += d;
        }
        double hi =
          totalD > 0.0 ? std::min(1.0, maxHosts * budget / totalD) : 1.0;
        double lo = 0.0;
        // Convert the absolute search tolerance (req/s, tunable via
        // resetParameter("capacity_search_tol")) into a scale fraction. Clamp
        // it below the search interval width: an absolute tolerance wider
        // than [lo, hi] (low input rates) would otherwise skip the search
        // entirely and keep the saturated partial placement.
        double tolScale = sig.avgInputRate > 0.0
                            ? capacitySearchTolRate / sig.avgInputRate
                            : 0.01;
        tolScale = std::clamp(tolScale, 1e-4, 0.05);
        std::map<std::string, std::map<std::string, double>> bestPlace;
        std::vector<std::string> bestOpened;
        double bestScale = 0.0;
        while (hi - lo > tolScale) {
            double mid = 0.5 * (lo + hi);
            auto probe = runBackward(mid);
            if (!std::get<2>(probe)) { // fits
                bestPlace = std::move(std::get<0>(probe));
                bestOpened = std::move(std::get<1>(probe));
                bestScale = mid;
                lo = mid;
            } else {
                hi = mid;
            }
        }
        if (!bestPlace.empty()) {
            placeAbs = std::move(bestPlace);
            opened = std::move(bestOpened);
            lambdaMult = bestScale;
            // Refresh the saturation cache with this search result.
            capSatCache.valid = true;
            capSatCache.hostIps = allIps;
            capSatCache.W = W;
            capSatCache.tE = tE;
            capSatCache.alpha = alpha;
            capSatCache.beta = beta;
            capSatCache.gamma = gamma;
            capSatCache.chainedRatio = R;
            capSatCache.procShare = procShare;
            capSatCache.edgeShare = edgeShare;
            capSatCache.placeAbs = placeAbs;
            capSatCache.opened = opened;
            capSatCache.sustainedAbsRate = lambdaMult * sig.avgInputRate;
        }
        SPDLOG_WARN(
          "Capacity Binpack: cluster SATURATED at requested rate; scheduling "
          "for max sustainable rate (~{:.2f}x of requested, ~{:.1f} req/s) on "
          "{} workers",
          lambdaMult,
          lambdaMult > 0.0 ? lambdaMult * sig.avgInputRate : 0.0,
          opened.size());
    }

    // ---- Emit one single-node group per operator (groupNodesTopo shape) -------
    std::vector<NodeGroup> groups;
    std::vector<std::map<std::string, double>> groupAllocations;
    std::map<std::string, double> workerRemaining;
    for (const auto& ip : allIps) {
        workerRemaining[ip] = 1.0;
    }
    // Evaluate final loads in the same units as placeAbs: when the placement
    // was scaled down to the max sustainable rate, scale the demand/call
    // totals likewise, so capEvalWorkerLoadsProc recovers true placement
    // fractions (bFrac is a probability and must not shrink with the rate).
    // finalLoads is then the per-worker load at the ADMITTED rate.
    std::map<std::string, double> evalDemand = procDemand;
    std::vector<CapEdge> evalEdges = edges;
    if (lambdaMult > 0.0) {
        for (auto& [op, d] : evalDemand) {
            d *= lambdaMult;
        }
        for (auto& e : evalEdges) {
            e.calls *= lambdaMult;
        }
    }
    auto finalLoads = capEvalWorkerLoadsProc(
      placeAbs, opened, evalDemand, evalEdges, alpha, beta, gamma);
    for (const auto& ip : opened) {
        workerRemaining[ip] = std::max(0.0, 1.0 - finalLoads[ip] / W);
    }

    // Normalise each operator's absolute placement into fractions (Σ_ip = 1).
    for (const auto& [name, node] : topo) {
        std::map<std::string, double> alloc;
        auto it = placeAbs.find(name);
        if (it != placeAbs.end()) {
            double tot = 0.0;
            for (const auto& [ip, v] : it->second) {
                tot += v;
            }
            if (tot > 0.0) {
                for (const auto& [ip, v] : it->second) {
                    alloc[ip] = v / tot;
                }
            }
        }
        if (alloc.empty()) {
            // Fallback: pin to the first opened worker so downstream always has
            // a placement (should only happen for zero-demand operators).
            alloc[opened.front()] = 1.0;
        }
        groups.push_back({ { node }, NONE_STRING });
        groupAllocations.push_back(alloc);
    }

    SPDLOG_INFO("Capacity Binpack (backward): opened {}/{} workers, max worker "
                "load {:.0f}us/s (budget {:.0f}us/s){}",
                opened.size(),
                maxHosts,
                [&]() {
                    double m = 0.0;
                    for (const auto& ip : opened)
                        m = std::max(m, finalLoads[ip]);
                    return m;
                }(),
                budget,
                saturated ? " [SATURATED]" : "");

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
  double gamma,
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
    // Distinct remote dest workers each source worker fans out to. A fixed
    // gamma cost is charged once per entry below (per-host overhead, not
    // per-call), so it is independent of how many calls each edge carries.
    std::vector<std::set<int>> remoteDestHosts(numHosts);
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
                if (w < 0 || w >= numHosts || aFrac <= 0.0) {
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

                // Record every distinct remote destination worker (w' != w
                // with a non-zero placement fraction) this source worker w
                // sends to, for the per-host gamma cost below.
                for (const auto& [destW, destFrac] : bIt->second) {
                    if (destW != w && destW >= 0 && destW < numHosts &&
                        destFrac > 0.0) {
                        remoteDestHosts[w].insert(destW);
                    }
                }
            }
        }
    }

    // Fixed per-destination-host overhead: gamma us per distinct remote worker.
    if (gamma > 0.0) {
        for (int w = 0; w < numHosts; ++w) {
            workerLoads[w] += gamma * static_cast<double>(remoteDestHosts[w].size());
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

void StateAwareScheduler::rescheduleAppBinpack(
  const HostMap& hostMap,
  const faabric::planner::ApplicationMetrics::ScalingSignals* signals,
  const faabric::planner::ApplicationMetrics* metrics)
{
    SPDLOG_INFO("Rescheduling the application in Binpack Mode");
    // 1. Map and assign operators to hosts.
    //    Use the capacity-aware open-ended packer once the estimator has warmed
    //    up (valid C / exec time / input rate); otherwise fall back to the
    //    legacy topology tape layout (e.g. on the first schedule).
    bool useCapacity = signals != nullptr && signals->coeffC > 0.0 &&
                       signals->avgExecTime > 0.0 && signals->avgInputRate > 0.0;
    auto [groups, groupAllocations, workerRemaining] =
      useCapacity ? groupNodesCapacity(hostMap, *signals)
                  : groupNodesTopo(hostMap);

    commitGroupAllocations(groups, groupAllocations);
    if (signals != nullptr && scheduleMode == 0) {
        applyExecutorDist(*signals, metrics);
    }
}

std::map<std::string, double> StateAwareScheduler::perOperatorExecTime(
  const faabric::planner::ApplicationMetrics* metrics) const
{
    std::map<std::string, double> execTime;
    if (metrics == nullptr) {
        return execTime;
    }

    // snapshot() carries a running average and a cumulative count per
    // instance, so folding parallelism instances back into their operator is a
    // count-weighted mean.
    std::map<std::string, double> weightedSum;
    std::map<std::string, long> counts;
    for (const auto& [instanceName, snap] :
         metrics->getInstanceLifecycleSnapshots()) {
        if (snap.count <= 0 || snap.workerExecTime <= 0.0) {
            continue;
        }
        std::string opName;
        try {
            auto parts = faabric::util::splitUserFuncPar(instanceName);
            opName = std::get<0>(parts) + "_" + std::get<1>(parts);
        } catch (const std::invalid_argument&) {
            SPDLOG_WARN("Executor budget: cannot parse instance name {}",
                        instanceName);
            continue;
        }
        weightedSum[opName] += snap.workerExecTime * (double)snap.count;
        counts[opName] += snap.count;
    }

    for (const auto& [opName, sum] : weightedSum) {
        long n = counts.at(opName);
        if (n > 0) {
            execTime[opName] = sum / (double)n;
        }
    }
    return execTime;
}

void StateAwareScheduler::applyExecutorDist(
  const faabric::planner::ApplicationMetrics::ScalingSignals& sig,
  const faabric::planner::ApplicationMetrics* metrics)
{
    // --- 1. Cluster-wide budget -------------------------------------------
    // E_new = E_old * (inputRate / throughput). The ratio is the calibration:
    // E_old executors sustained `throughput` req/s, so serving `inputRate`
    // req/s needs that many times more. When input and throughput match the
    // budget holds steady.
    if (sig.avgTotalExecutors <= 0.0 || sig.avgThroughput <= 0.0 ||
        sig.avgInputRate <= 0.0) {
        SPDLOG_DEBUG("Executor budget: signals not usable yet (executors={:.1f}"
                     ", throughput={:.1f}, inputRate={:.1f}); leaving "
                     "executorDist empty",
                     sig.avgTotalExecutors,
                     sig.avgThroughput,
                     sig.avgInputRate);
        return;
    }

    double totalExecutors =
      sig.avgTotalExecutors * (sig.avgInputRate / sig.avgThroughput);
    double growthCeiling = sig.avgTotalExecutors * execBudgetGrowthCap;
    if (totalExecutors > growthCeiling) {
        SPDLOG_WARN("Executor budget: {:.1f} capped to {:.1f} by the growth cap"
                    " ({:.1f}x of {:.1f} running)",
                    totalExecutors,
                    growthCeiling,
                    execBudgetGrowthCap,
                    sig.avgTotalExecutors);
        totalExecutors = growthCeiling;
    }

    // --- 2. Split across operators by CPU demand --------------------------
    // demand_o = processedTuples_o * t_e,o. Only the ratio between operators
    // matters, so both factors may stay in their raw cumulative units.
    // reqResource is deliberately NOT used here: it is quantised to 0.1-worker
    // steps and normalised to sum to numHosts, which discards exactly the
    // magnitude information this split needs.
    auto execTime = perOperatorExecTime(metrics);

    std::map<std::string, double> demand;
    double totalDemand = 0.0;
    bool haveExecTime = !execTime.empty();
    for (const auto& [name, sop] : scheduledOperatorsMap) {
        double tuples = static_cast<double>(std::max<long>(
          1, sop.node.processedTuples));
        auto it = execTime.find(name);
        if (haveExecTime && it == execTime.end()) {
            // An operator with no completed request yet has no measured
            // latency. Falling back to the cluster average for it alone keeps
            // the split defined without dragging every other operator onto the
            // average too.
            demand[name] = tuples * std::max(1.0, sig.avgExecTime);
        } else if (haveExecTime) {
            demand[name] = tuples * it->second;
        } else {
            // No per-operator latency at all: degrade to the tuple-count-only
            // weighting, i.e. the pre-existing behaviour.
            demand[name] = tuples;
        }
        totalDemand += demand[name];
    }
    if (totalDemand <= 0.0) {
        SPDLOG_WARN("Executor budget: total demand is 0, skipping");
        return;
    }
    if (!haveExecTime) {
        SPDLOG_WARN("Executor budget: no per-operator exec latency yet, "
                    "splitting on processedTuples alone");
    }

    // --- 3. Split each operator across its workers by weightDist ----------
    // weightDist is normalised per operator here because the two Binpack
    // packers scale it differently: groupNodesTopo sums to reqResource (worker
    // fractions) while groupNodesCapacity sums to 1 (traffic fractions). Only
    // the relative split across hosts is meaningful either way.
    int grandTotal = 0;
    for (auto& [name, sop] : scheduledOperatorsMap) {
        sop.executorDist.clear();

        double opExecutors = totalExecutors * (demand.at(name) / totalDemand);

        double weightSum = 0.0;
        for (const auto& [ip, w] : sop.weightDist) {
            weightSum += w;
        }
        if (weightSum <= 0.0 || opExecutors <= 0.0) {
            // No placement (or no demand): give every host holding this
            // operator a single executor so it stays runnable.
            for (const auto& [ip, w] : sop.weightDist) {
                sop.executorDist[ip] = 1;
                grandTotal += 1;
            }
            continue;
        }

        for (const auto& [ip, w] : sop.weightDist) {
            // Ceiling per the agreed rule: a host holding any share of an
            // operator always gets at least one executor, and the cluster-wide
            // total is allowed to overshoot the budget as a result.
            int n = static_cast<int>(std::ceil(opExecutors * (w / weightSum)));
            if (n < 1) {
                n = 1;
            }
            sop.executorDist[ip] = n;
            grandTotal += n;
        }
    }

    SPDLOG_INFO("Executor budget: {:.1f} req/s in / {:.1f} req/s out over "
                "{:.1f} executors -> budget {:.1f}, {} after per-worker ceiling",
                sig.avgInputRate,
                sig.avgThroughput,
                sig.avgTotalExecutors,
                totalExecutors,
                grandTotal);

    for ([[maybe_unused]] const auto& [name, sop] : scheduledOperatorsMap) {
        SPDLOG_INFO("Executor budget: {} demand={:.0f} ({:.1f}% of total, "
                    "tuples={}, execTime={:.1f}us) -> {}",
                    name,
                    demand.at(name),
                    100.0 * demand.at(name) / totalDemand,
                    sop.node.processedTuples,
                    execTime.count(name) ? execTime.at(name) : 0.0,
                    sop.executorDist);
    }
}

void StateAwareScheduler::commitGroupAllocations(
  const std::vector<NodeGroup>& groups,
  const std::vector<std::map<std::string, double>>& groupAllocations)
{
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

//--------------------------------------------------------------------------
// Schedule mode 4: scaling by non-linear performance model + soft-affinity
// placement (Zhang et al., HPCC'24).
//--------------------------------------------------------------------------

long StateAwareScheduler::predictNonlinearInstanceTotal(
  double inputRate,
  const faabric::planner::ApplicationMetrics::ScalingSignals& signals) const
{
    if (!application || inputRate <= 0.0) {
        return -1;
    }
    auto& nodes = application->getNodes();
    if (nodes.empty()) {
        return -1;
    }

    // Per-operator exec time from the last lifecycle snapshots (cumulative
    // averages, count-weighted across the operator's instances), falling
    // back to the cluster average.
    std::map<std::string, double> teNum;
    std::map<std::string, long> teDen;
    for (const auto& [inst, s] : nlLastSnap) {
        auto pos = inst.rfind('_');
        if (pos == std::string::npos || s.count <= 0 ||
            s.workerExecTime <= 0.0) {
            continue;
        }
        std::string op = inst.substr(0, pos);
        teNum[op] += s.workerExecTime * static_cast<double>(s.count);
        teDen[op] += s.count;
    }

    // Demand split, same derivation as rescheduleAppNonlinear but at the
    // hypothetical input rate.
    double totalTuples = 0.0;
    double inputTuples = 0.0;
    for (const auto& [name, node] : nodes) {
        totalTuples +=
          static_cast<double>(std::max<long>(1, node->processedTuples));
    }
    for (const auto& inName : application->getInputNodes()) {
        auto it = nodes.find(inName);
        if (it != nodes.end()) {
            inputTuples += static_cast<double>(
              std::max<long>(1, it->second->processedTuples));
        }
    }
    double totalLoad = inputRate;
    if (inputTuples > 0.0) {
        totalLoad = inputRate * (totalTuples / inputTuples);
    }

    long total = 0;
    for (const auto& [name, node] : nodes) {
        long p = 1;
        auto dit = teDen.find(name);
        double te = (dit != teDen.end() && dit->second > 0)
                      ? teNum.at(name) / static_cast<double>(dit->second)
                      : signals.avgExecTime;
        double share =
          static_cast<double>(std::max<long>(1, node->processedTuples)) /
          totalTuples;
        double rate = totalLoad * share;
        if (te > 0.0 && rate > 0.0) {
            double cInst = nlCpuBudgetPerExecutorUs / te; // records/s
            // r(p) is clamped to <= nlMaxOverheadRatio, so the smallest
            // satisfying p is bounded by the demand at worst-case overhead.
            // The demand is deliberately NOT capped by cluster capacity;
            // excess instances are overcommitted at placement time.
            long pMax = static_cast<long>(std::ceil(
                          rate / (cInst * (1.0 - nlMaxOverheadRatio) *
                                  capacityHeadroom))) +
                        1;
            for (; p < pMax; ++p) {
                double capEff = p * cInst * (1.0 - nlPredictR(name, p)) *
                                capacityHeadroom;
                if (capEff >= rate) {
                    break;
                }
            }
        }
        total += std::max<long>(1, p);
    }
    return total;
}

void StateAwareScheduler::nlCollectSamples(
  const std::map<std::string, faabric::planner::InstanceMetrics::Snapshot>&
    snaps)
{
    // Per-operator interval sums, weighted by each instance's message count.
    std::map<std::string, double> tsSum;
    std::map<std::string, double> teSum;
    std::map<std::string, long> msgCount;

    for (const auto& [inst, cur] : snaps) {
        auto pit = nlLastSnap.find(inst);
        if (pit == nlLastSnap.end()) {
            continue;
        }
        const auto& prev = pit->second;
        long d = cur.count - prev.count;
        if (d <= 0) {
            continue;
        }
        // Interval per-message average recovered from two cumulative
        // averages. dispatchTime only averages the non-negative samples
        // (no-NTP guard), so its delta is a slight approximation.
        auto delta = [&](double a2, double a1) {
            return (a2 * static_cast<double>(cur.count) -
                    a1 * static_cast<double>(prev.count)) /
                   static_cast<double>(d);
        };
        double ts = delta(cur.plannerQueueTime, prev.plannerQueueTime) +
                    delta(cur.plannerConsumeTime, prev.plannerConsumeTime) +
                    delta(cur.dispatchTime, prev.dispatchTime) +
                    delta(cur.workerQueueTime, prev.workerQueueTime);
        double te = delta(cur.workerExecTime, prev.workerExecTime);
        if (te <= 0.0) {
            continue;
        }
        auto pos = inst.rfind('_');
        if (pos == std::string::npos) {
            continue;
        }
        std::string op = inst.substr(0, pos);
        tsSum[op] += std::max(0.0, ts) * static_cast<double>(d);
        teSum[op] += te * static_cast<double>(d);
        msgCount[op] += d;
    }

    bool newSamples = false;
    for (const auto& [op, cnt] : msgCount) {
        if (cnt <= 0 || teSum[op] <= 0.0) {
            continue;
        }
        auto lit = nlLastParallelism.find(op);
        if (lit == nlLastParallelism.end() || lit->second <= 0) {
            // Interval parallelism unknown (operator appeared between two
            // reschedules); the sample cannot be tagged with a p.
            continue;
        }
        double r = tsSum[op] / (tsSum[op] + teSum[op]);
        auto& buf = nlSamples[op];
        buf.emplace_back(static_cast<double>(lit->second), r);
        if (buf.size() > 64) {
            buf.erase(buf.begin());
        }
        nlFits[op] = nlFitCurve(buf, nlMinFitSamples);
        newSamples = true;
        SPDLOG_INFO("Non-linear mode: sample op={} p={} r={:.4f} ({} msgs) -> "
                    "fit(k={:.3f}, b={:.3f}, valid={})",
                    op,
                    lit->second,
                    r,
                    cnt,
                    nlFits[op].k,
                    nlFits[op].b,
                    nlFits[op].valid);
    }

    if (newSamples) {
        std::vector<std::pair<double, double>> all;
        for (const auto& [op, buf] : nlSamples) {
            all.insert(all.end(), buf.begin(), buf.end());
        }
        nlGlobalFit = nlFitCurve(all, nlMinFitSamples);
    }

    nlLastSnap = snaps;
}

StateAwareScheduler::NlFit StateAwareScheduler::nlFitCurve(
  const std::vector<std::pair<double, double>>& samples,
  int minSamples)
{
    NlFit fit;
    if (static_cast<int>(samples.size()) < minSamples) {
        return fit;
    }
    // r(p) = b·(1 - e^{-kp}) is linear in b for fixed k, so grid-search k in
    // log space and solve b in closed form (least squares through the
    // implicit (0,0) pre-fit point).
    constexpr int kSteps = 40;
    constexpr double kMin = 0.01;
    constexpr double kMax = 5.0;
    double bestSse = std::numeric_limits<double>::max();
    for (int i = 0; i < kSteps; ++i) {
        double k = kMin * std::pow(kMax / kMin,
                                   static_cast<double>(i) / (kSteps - 1));
        double num = 0.0;
        double den = 0.0;
        for (const auto& [p, r] : samples) {
            double x = 1.0 - std::exp(-k * p);
            num += r * x;
            den += x * x;
        }
        if (den <= 0.0) {
            continue;
        }
        double b = std::clamp(num / den, 0.0, nlMaxOverheadRatio);
        if (b <= 0.0) {
            continue;
        }
        double sse = 0.0;
        for (const auto& [p, r] : samples) {
            double e = b * (1.0 - std::exp(-k * p)) - r;
            sse += e * e;
        }
        if (sse < bestSse) {
            bestSse = sse;
            fit.k = k;
            fit.b = b;
            fit.valid = true;
        }
    }
    return fit;
}

double StateAwareScheduler::nlPredictR(const std::string& op, double p) const
{
    if (p <= 0.0) {
        return 0.0;
    }
    const NlFit* f = nullptr;
    auto it = nlFits.find(op);
    if (it != nlFits.end() && it->second.valid) {
        f = &it->second;
    } else if (nlGlobalFit.valid) {
        f = &nlGlobalFit;
    }
    if (f == nullptr) {
        // Cold start: fall back to the prior curve until a fit is valid.
        return std::clamp(nlPriorB * (1.0 - std::exp(-nlPriorK * p)),
                          0.0,
                          nlMaxOverheadRatio);
    }
    return std::clamp(
      f->b * (1.0 - std::exp(-f->k * p)), 0.0, nlMaxOverheadRatio);
}

std::map<std::string, std::map<std::string, int>>
StateAwareScheduler::softAffinityPlace(
  const HostMap& hostMap,
  const std::vector<std::pair<std::string, int>>& orderedTargets)
{
    constexpr double EPS = 1e-9;

    std::map<std::string, int> slotsOf;
    std::map<std::string, int> used;
    for (const auto& [ip, host] : hostMap) {
        slotsOf[ip] = nlHostCapacity(host->slots);
        used[ip] = 0;
    }

    // Upstream edge index: op -> [(source op, chained weight)].
    std::map<std::string, std::vector<std::pair<std::string, int>>> upEdges;
    for (const auto& e : application->getConnectionsWithWeight()) {
        if (e.weight > 0) {
            upEdges[e.output].emplace_back(e.input, e.weight);
        }
    }

    std::map<std::string, std::map<std::string, int>> placement;
    long totalUsed = 0;
    bool warnedOvercommit = false;

    for (const auto& [op, n] : orderedTargets) {
        // S[ip]: data volume the upstream instances on ip send towards this
        // operator (edge weight split by the upstream's instance fractions).
        // Paper eq (9).
        std::map<std::string, double> S;
        auto ueIt = upEdges.find(op);
        if (ueIt != upEdges.end()) {
            for (const auto& [src, w] : ueIt->second) {
                auto pit = placement.find(src);
                if (pit == placement.end()) {
                    continue;
                }
                long nSrc = 0;
                for (const auto& [ip, c] : pit->second) {
                    nSrc += c;
                }
                if (nSrc <= 0) {
                    continue;
                }
                for (const auto& [ip, c] : pit->second) {
                    S[ip] += static_cast<double>(w) * c / nSrc;
                }
            }
        }
        double sumSInit = 0.0;
        for (const auto& [ip, v] : S) {
            sumSInit += v;
        }
        // Eq (11): every placed instance "consumes" S_avg of affinity volume.
        double sAvg = n > 0 ? sumSInit / n : 0.0;

        auto& B = placement[op];
        for (int i = 0; i < n; ++i) {
            double curSum = 0.0;
            for (const auto& [ip, v] : S) {
                curSum += v;
            }
            double denom = curSum - sAvg; // eq (10) denominator

            std::string best;
            double bestScore = std::numeric_limits<double>::lowest();
            for (const auto& [ip, cap] : slotsOf) {
                double data = 0.0;
                auto sit = S.find(ip);
                if (sit != S.end()) {
                    if (denom > EPS) {
                        data = (sit->second - sAvg) / denom; // eq (10)
                    } else if (curSum > EPS) {
                        data = std::max(0.0, sit->second) / curSum; // eq (9)
                    }
                }
                double balance =
                  totalUsed > 0
                    ? 1.0 - static_cast<double>(used[ip]) / totalUsed
                    : 1.0; // eq (12)
                double score = softAffinityWs * data + softAffinityWb * balance;
                // Full nodes only win when every node is full (overcommit).
                if (used[ip] >= cap) {
                    score -= 1e6;
                }
                if (best.empty() || score > bestScore) {
                    best = ip;
                    bestScore = score;
                }
            }
            if (bestScore < -1e5 && !warnedOvercommit) {
                SPDLOG_WARN("Soft affinity: all hosts full, overcommitting "
                            "instances beyond slot capacity");
                warnedOvercommit = true;
            }
            B[best]++;
            used[best]++;
            totalUsed++;
            auto sit = S.find(best);
            if (sit != S.end()) {
                sit->second -= sAvg; // paper's dynamic update after eq (10)
            }
        }
    }
    return placement;
}

StateAwareScheduler::NlSizing StateAwareScheduler::nlComputeSizing(
  const HostMap& hostMap,
  const faabric::planner::ApplicationMetrics* metrics)
{
    NlSizing sizing;
    if (!application) {
        return sizing;
    }

    //--------------------------------------------------------------------------
    // 1. Ingest fresh overhead samples and refresh the r(p) fits.
    //--------------------------------------------------------------------------
    std::map<std::string, faabric::planner::InstanceMetrics::Snapshot> snaps;
    faabric::planner::ApplicationMetrics::ScalingSignals signals;
    if (metrics != nullptr) {
        snaps = metrics->getInstanceLifecycleSnapshots();
        nlCollectSamples(snaps);
        signals = metrics->getScalingSignals(kChainedCostWindowSec);
    }

    //--------------------------------------------------------------------------
    // 2. Per-operator exec time (count-weighted cumulative average across the
    //    operator's instances), falling back to the cluster average.
    //--------------------------------------------------------------------------
    std::map<std::string, double> teNum;
    std::map<std::string, long> teDen;
    for (const auto& [inst, s] : snaps) {
        auto pos = inst.rfind('_');
        if (pos == std::string::npos || s.count <= 0 ||
            s.workerExecTime <= 0.0) {
            continue;
        }
        std::string op = inst.substr(0, pos);
        teNum[op] += s.workerExecTime * static_cast<double>(s.count);
        teDen[op] += s.count;
    }

    //--------------------------------------------------------------------------
    // 3. Per-operator demand (records/s): input rate scaled to total DAG
    //    processing, split by processedTuples share (same derivation as the
    //    capacity packer).
    //--------------------------------------------------------------------------
    auto& nodes = application->getNodes();
    double totalTuples = 0.0;
    double inputTuples = 0.0;
    for (const auto& [name, node] : nodes) {
        totalTuples +=
          static_cast<double>(std::max<long>(1, node->processedTuples));
    }
    for (const auto& inName : application->getInputNodes()) {
        auto it = nodes.find(inName);
        if (it != nodes.end()) {
            inputTuples += static_cast<double>(
              std::max<long>(1, it->second->processedTuples));
        }
    }
    double totalLoad = signals.avgInputRate;
    if (inputTuples > 0.0) {
        totalLoad = signals.avgInputRate * (totalTuples / inputTuples);
    }

    // Per-worker executor capacity: max_executors when configured (one
    // executor = one CPU), otherwise the host's reported slots.
    long totalCapacity = 0;
    for (const auto& [ip, host] : hostMap) {
        totalCapacity += nlHostCapacity(host->slots);
    }

    bool warm = signals.avgInputRate > 0.0;

    //--------------------------------------------------------------------------
    // 4. Target parallelism per operator: smallest p with
    //    p · c · (1 - r(p)) · headroom ≥ rate_o  (the paper's n* = n/(1-r)
    //    solved as a search, since capacity is monotone in p). Per-instance
    //    capacity c = nlCpuBudgetPerExecutorUs/t_e in records/s (one executor
    //    = one CPU, per the paper's experimental setup). The demand is NOT
    //    capped by cluster capacity — when it exceeds the available
    //    executors, softAffinityPlace overcommits beyond each worker's
    //    capacity.
    //--------------------------------------------------------------------------
    std::map<std::string, int>& target = sizing.target;
    for (const auto& [name, node] : nodes) {
        long p = 1;
        if (warm) {
            double te = teDen.count(name) && teDen[name] > 0
                          ? teNum[name] / teDen[name]
                          : signals.avgExecTime;
            double share =
              static_cast<double>(std::max<long>(1, node->processedTuples)) /
              totalTuples;
            double rate = totalLoad * share;
            if (te > 0.0 && rate > 0.0) {
                double cInst = nlCpuBudgetPerExecutorUs / te; // records/s
                // r(p) <= nlMaxOverheadRatio bounds the smallest satisfying
                // p by the demand at worst-case overhead.
                long pMax = static_cast<long>(std::ceil(
                              rate / (cInst * (1.0 - nlMaxOverheadRatio) *
                                      capacityHeadroom))) +
                            1;
                for (; p < pMax; ++p) {
                    double capEff = p * cInst * (1.0 - nlPredictR(name, p)) *
                                    capacityHeadroom;
                    if (capEff >= rate) {
                        break;
                    }
                }
            }
        }
        target[name] = static_cast<int>(std::max<long>(1, p));
    }

    for (const auto& [op, n] : target) {
        sizing.plannedInstances += n;
    }
    sizing.totalCapacity = totalCapacity;
    return sizing;
}

void StateAwareScheduler::rescheduleAppNonlinear(
  const HostMap& hostMap,
  const faabric::planner::ApplicationMetrics* metrics)
{
    SPDLOG_INFO("Rescheduling the application in Non-linear AutoTuning mode "
                "(mode 4)");
    if (!application || hostMap.empty()) {
        SPDLOG_WARN("Non-linear mode: no application or empty host map");
        return;
    }

    //--------------------------------------------------------------------------
    // 1-4. Refit r(p) and solve the per-operator instance demand.
    //--------------------------------------------------------------------------
    auto sizing = nlComputeSizing(hostMap, metrics);
    const auto& target = sizing.target;
    const long planned = sizing.plannedInstances;
    const long totalCapacity = sizing.totalCapacity;
    bool saturated = planned > totalCapacity;

    //--------------------------------------------------------------------------
    // 5. Topological order (upstream first, so Data_N sees placed sources),
    //    then soft-affinity placement.
    //--------------------------------------------------------------------------
    auto& nodes = application->getNodes();
    std::vector<std::pair<std::string, int>> orderedTargets;
    {
        std::set<std::string> vis;
        std::vector<std::string> post;
        const auto& conns = application->getConnections();
        std::function<void(const std::string&)> dfs =
          [&](const std::string& n) {
              vis.insert(n);
              auto cit = conns.find(n);
              if (cit != conns.end()) {
                  for (const auto& s : cit->second) {
                      if (!vis.count(s)) {
                          dfs(s);
                      }
                  }
              }
              post.push_back(n);
          };
        for (const auto& in : application->getInputNodes()) {
            if (!vis.count(in)) {
                dfs(in);
            }
        }
        for (const auto& [name, node] : nodes) {
            if (!vis.count(name)) {
                dfs(name);
            }
        }
        for (auto it = post.rbegin(); it != post.rend(); ++it) {
            if (target.count(*it)) {
                orderedTargets.emplace_back(*it, target.at(*it));
            }
        }
    }

    auto placement = softAffinityPlace(hostMap, orderedTargets);

    //--------------------------------------------------------------------------
    // 6. Emit one single-node group per operator (Binpack commit shape) and
    //    commit through the shared tail.
    //--------------------------------------------------------------------------
    std::vector<NodeGroup> groups;
    std::vector<std::map<std::string, double>> groupAllocations;
    auto topo = application->getNodesDFSOrder();
    for (const auto& [name, node] : topo) {
        std::map<std::string, double> alloc;
        auto pit = placement.find(name);
        int n = target.count(name) ? target.at(name) : 1;
        if (pit != placement.end() && n > 0) {
            for (const auto& [ip, c] : pit->second) {
                if (c > 0) {
                    alloc[ip] = static_cast<double>(c) / n;
                }
            }
        }
        if (alloc.empty()) {
            alloc[hostMap.begin()->first] = 1.0;
        }
        // With single-node groups reqResource only matters for the intra-group
        // scale factor (=1); the instance count is its natural value here.
        node->reqResource = static_cast<double>(n);
        groups.push_back({ { node }, NONE_STRING });
        groupAllocations.push_back(alloc);
    }

    commitGroupAllocations(groups, groupAllocations);

    // Remember the parallelism the next sampling interval runs with.
    nlLastParallelism = target;

    std::stringstream ss;
    for (const auto& [op, n] : target) {
        ss << op << "=" << n << " (r=" << nlPredictR(op, n) << ") ";
    }
    SPDLOG_INFO("Non-linear mode: total instances {}/{} executor capacity{} "
                "-> {}",
                planned,
                totalCapacity,
                saturated ? " [OVERCOMMITTED]" : "",
                ss.str());
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

//--------------------------------------------------------------------------
// CLAST-placement hybrid: worker count from mode 3 / 4, layout from mode 0.
//--------------------------------------------------------------------------

void StateAwareScheduler::rescheduleAppClastPlacement(
  const HostMap& hostMap,
  const faabric::planner::ApplicationMetrics* metrics,
  const faabric::planner::ApplicationMetrics::ScalingSignals* signals,
  double chainedCostCoeff)
{
    const int numHosts = static_cast<int>(hostMap.size());
    if (!application || numHosts == 0) {
        SPDLOG_WARN("CLAST placement: no application or empty host map");
        return;
    }

    //--------------------------------------------------------------------------
    // 1. Ask the base mode how many workers it thinks the application needs.
    //    This is the ONLY thing taken from mode 3 / 4 here.
    //--------------------------------------------------------------------------
    int targetHosts = numHosts;
    if (scheduleMode == 3) {
        double avgInputRate = 0.0;
        if (metrics != nullptr) {
            avgInputRate = metrics->getScalingSignals(3).avgInputRate;
            targetHosts =
              metrics->computeAdaptiveHostCount(avgInputRate, numHosts);
        }
        SPDLOG_INFO("CLAST placement (mode 3 sizing): avgInputRate(3s)={:.1f} "
                    "-> {} workers",
                    avgInputRate,
                    targetHosts);
    } else if (scheduleMode == 4) {
        // Same sizing rescheduleAppNonlinear runs on: Σ n*_o instances folded
        // to a host count by the per-worker executor capacity (1 executor =
        // 1 CPU). Running it here also keeps the r(p) fitting loop alive,
        // since nlComputeSizing ingests this interval's overhead samples.
        auto sizing = nlComputeSizing(hostMap, metrics);
        double capPerHost = 0.0;
        if (numHosts > 0 && sizing.totalCapacity > 0) {
            capPerHost =
              static_cast<double>(sizing.totalCapacity) / numHosts;
        }
        if (capPerHost > 0.0 && sizing.plannedInstances > 0) {
            targetHosts = static_cast<int>(std::ceil(
              static_cast<double>(sizing.plannedInstances) / capPerHost));
        }
        SPDLOG_INFO("CLAST placement (mode 4 sizing): {} instances / {:.1f} "
                    "per worker -> {} workers",
                    sizing.plannedInstances,
                    capPerHost,
                    targetHosts);
        // Remember the parallelism the next sampling interval is tagged with,
        // exactly as rescheduleAppNonlinear does.
        nlLastParallelism = sizing.target;
    }
    targetHosts = std::clamp(targetHosts, 1, numHosts);

    //--------------------------------------------------------------------------
    // 2. Restrict the packer to that many workers. groupNodesCapacity opens
    //    workers on demand and cannot be told "use N"; bounding its host set at
    //    N makes its saturation binary search solve the dual problem instead —
    //    the largest input rate whose placement fits in N workers — and place
    //    the operators at that rate.
    //--------------------------------------------------------------------------
    HostMap limitedHostMap;
    {
        int cnt = 0;
        for (const auto& [ip, host] : hostMap) {
            if (cnt >= targetHosts) {
                break;
            }
            limitedHostMap[ip] = host;
            ++cnt;
        }
    }

    // Re-quantise with the mode-0 weighting (chained calls priced by
    // chainedCostCoeff) over the restricted worker set: rescheduleApp()
    // quantised for the base mode and the full host map.
    application->quantiseResources(
      limitedHostMap.size(), 0, chainedCostCoeff);

    SPDLOG_INFO("CLAST placement: laying out the application on {}/{} workers "
                "with the mode-0 capacity packer",
                limitedHostMap.size(),
                numHosts);

    rescheduleAppBinpack(limitedHostMap, signals);
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
    capSatCache = CapacitySaturationCache{};
    runtimeSummary.reset();
}

} // namespace faabric::batch_scheduler