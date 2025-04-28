#include <faabric/batch-scheduler/SchedulingDecision.h>
#include <faabric/batch-scheduler/StateAwareScheduler.h>
#include <faabric/state/FunctionStateClient.h>
#include <faabric/util/batch.h>
#include <faabric/util/logging.h>
#include <faabric/util/serialization.h>
#include <faabric/util/string_tools.h>

#include <algorithm>

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

template<typename K, typename V>
K getNthKey(const std::map<K, V>& map, std::size_t n)
{
    if (n >= map.size()) {
        throw std::out_of_range("Index out of range");
    }

    auto it = map.begin();
    std::advance(it, n);
    return it->first;
}

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
void StateAwareScheduler::funcStateInitializer()
{
    maxParallelism = faabric::util::getSystemConfig().maxParallelism;
}

bool StateAwareScheduler::registerApp(
  std::unique_ptr<batch_scheduler::Application> app)
{
    SPDLOG_INFO("Planner received request to register application {}",
                app->getName());
    application = std::move(app);
    application->displayApplication();
    return true;
}

bool StateAwareScheduler::registerFunctionState(const std::string& userFunction,
                                                const std::string& partitionBy,
                                                const std::string& stateKey,
                                                const HostMap& hostMap)
{
    SPDLOG_INFO("Registering function state {} with partitioning by {} and "
                "state key {}",
                userFunction,
                partitionBy,
                stateKey);

    faabric::util::FullLock lock(scheduleMx);

    if (partitionBy == NONE_STRING || stateKey == NONE_STRING) {
        funcStateRegMap[userFunction] = std::make_tuple("", "");
    } else {
        funcStateRegMap[userFunction] = std::make_tuple(partitionBy, stateKey);
    }

    // Initialize the function state with parallelism 1, if not initialized.
    if (!functionParallelism.contains(userFunction)) {
        registerState(hostMap, userFunction);
        return true;
    }
    return false;
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
        functionCounter[userFunction]++;
        SPDLOG_TRACE("UserFunction {}'s hash {} and parallelismIdx {}",
                     userFunction,
                     hash,
                     parallelismIdx);
        return { 2, hash, parallelismIdx };
    }
    // Otherwise, use shuffle data-partitioning.
    return { 1,
             0,
             static_cast<int>(functionCounter[userFunction]++ %
                              functionParallelism[userFunction]) };
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

void StateAwareScheduler::registerState(const HostMap& hostMap,
                                        std::string userFunc,
                                        int parallelism)
{
    SPDLOG_INFO("Create func {} with parallelism {}", userFunc, parallelism);
    if (parallelism != 1) {
        SPDLOG_ERROR("Parallelism is not 1, it is not supported now");
        return;
    }
    functionParallelism[userFunc] = 1;
    functionCounter[userFunc] = 0;
    // The default parallelism is 1 and parallelism Idx is 0
    std::string funcParaId = userFunc + "_0";
    // Assign state to a host.
    int hostIdx =
      stateRbCounter.fetch_add(1, std::memory_order_relaxed) % hostMap.size();
    std::string host = getNthKey(hostMap, hostIdx);
    stateHost[funcParaId] = host;
    // If it is partitioned state, register it.
    std::string partitionBy = std::get<0>(funcStateRegMap[userFunc]);
    std::string stateKey = std::get<1>(funcStateRegMap[userFunc]);
    if (partitionBy != "" && stateKey != "") {
        statePartitionBy[userFunc] = partitionBy;
    }
    // Register the state to the host.
    registerStateToRedis(funcParaId, host);
}

std::string StateAwareScheduler::scheduleStatelessMessageRB(
  std::string& userFunc,
  const HostMap& hostMap,
  const std::unique_ptr<Message>& msg)
{
    int counter;
    {
        faabric::util::FullLock lock(scheduleMx);
        counter = functionCounter[userFunc]++;
    }
    int hostIdx = counter % hostMap.size();
    std::string host = getNthKey(hostMap, hostIdx);
    msg->set_messagetype(0);
    return host;
}

std::string StateAwareScheduler::scheduleStatelessMessageApportion(
  std::string& userFunc,
  const HostMap& hostMap,
  const std::unique_ptr<Message>& msg)
{
    std::string host = "unknown";
    if (optsCollocateMap.contains(userFunc)) {
        // If the optsCollocateMap contains the userFunc. We will try to
        // collocate it with the state.
        std::string& collocateFunc = optsCollocateMap[userFunc];
        auto parallelismInfo = getHashAndParallelismIndex(collocateFunc, *msg);
        std::string collocateUserFuncPar =
          collocateFunc + "_" + std::to_string(parallelismInfo.parallelismIdx);
        if (stateHost.find(collocateUserFuncPar) == stateHost.end()) {
            throw std::runtime_error("StateHost is not initialized");
        }
        std::string collocateHost = stateHost[collocateUserFuncPar];
        host = runtimeSummary.getHost(userFunc, collocateHost);
    } else {
        // Otherwise the request by using round robin.
        std::string userFuncPar = userFunc + "_0";
        std::shared_ptr<std::atomic_uint> counterPtr;
        {
            faabric::util::FullLock lock(counterMx);
            auto& slot = counterTable[userFuncPar];
            if (!slot) {
                slot = std::make_shared<std::atomic_uint>(0u);
            }
            counterPtr = slot;
        }
        unsigned int localCounter =
          counterPtr->fetch_add(1u, std::memory_order_relaxed);

        host = runtimeSummary.getHost(userFuncPar, localCounter);
    }
    msg->set_messagetype(0);
    return host;
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
        if (scheduleMode == 0) {
            // If schedule mode is 0 (The scheduler should dispatch stateless
            // messages in accordance with the expected proportions).
            host = scheduleStatelessMessageApportion(userFunc, hostMap, msg);
        } else {
            // If schedule mode is 1 or 2 (The scheduler dispatch stateless
            // messages in round-robin).
            host = scheduleStatelessMessageRB(userFunc, hostMap, msg);
        }
    }
    if (host == "unknown") {
        throw std::runtime_error("Host is unknown");
    }
    return host;
}

std::vector<std::string> StateAwareScheduler::scheduleMessagesBatch(
  const HostMap& hostMap,
  const std::vector<std::unique_ptr<faabric::Message>>& msgs)
{
    std::vector<std::string> hosts;
    hosts.resize(msgs.size());
    for (int i = 0; i < msgs.size(); i++) {
        hosts[i] = scheduleMessage(hostMap, msgs[i]);
    }
    return hosts;
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
  const std::map<std::string, long>& nodeWorkloads)
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
}

void StateAwareScheduler::updateReqDist()
{
    runtimeSummary.updateExpDist(statelessReqWeight);
    runtimeSummary.reallocateAll(true);
}

void StateAwareScheduler::reallocateSummaryDist(
  const std::map<std::string, std::map<std::string, int>>& sourceCountStats)
{
    runtimeSummary.updateSourceDist(sourceCountStats);
    runtimeSummary.reallocateAll(false);
}

void StateAwareScheduler::groupNodesHelper(
  const std::string& nodeName,
  std::vector<std::shared_ptr<Node>>& currentGroup,
  std::vector<std::vector<std::shared_ptr<Node>>>& groups,
  std::unordered_set<std::string>& visited)
{
    // If this node was already visited, skip it.
    if (visited.find(nodeName) != visited.end()) {
        return;
    }
    visited.insert(nodeName);
    currentGroup.push_back(application->getNodes().at(nodeName));

    // If this node has no successor, skip it.
    auto connIt = application->getConnections().find(nodeName);
    if (connIt == application->getConnections().end()) {
        return;
    }

    for (const auto& succName : connIt->second) {
        const std::shared_ptr<Node>& succNode =
          application->getNodes().at(succName);
        bool joinCurrent = false;
        if (succNode->type == STATELESS) {
            // For stateless nodes, continue in the same group.
            joinCurrent = true;
        }
        if (succNode->type == PARTITIONED_STATEFUL) {
            std::string partitionedKey = succNode->partitionBy;
            // For partitioned stateful nodes, check if all the previous
            // stateless nodes contains the same field key and partitioned
            // stateful opeartor contains the same partitioned attribute. Add it
            // in the current group.
            bool added = true;
            for (const auto& node : currentGroup) {
                if (node->type == STATEFUL) {
                    added = false;
                    break;
                } else if (node->type == STATELESS) {
                    auto inputFeilds = node->inputFeilds;
                    if (!inputFeilds.contains(partitionedKey)) {
                        added = false;
                        break;
                    }
                } else if (node->type == PARTITIONED_STATEFUL) {
                    if (node->partitionBy != partitionedKey) {
                        added = false;
                        break;
                    }
                } else {
                    // This should never happen. Since we
                    added = false;
                    break;
                }
            }
            if (added) {
                joinCurrent = true;
            }
        }
        if (joinCurrent) {
            groupNodesHelper(succName, currentGroup, groups, visited);
        } else { // Fallback. Else, start a new group.
            std::vector<std::shared_ptr<Node>> newGroup;
            groupNodesHelper(succName, newGroup, groups, visited);
            if (!newGroup.empty()) {
                groups.push_back(newGroup);
            }
        }
    }
}

void StateAwareScheduler::groupNodesHelper(
  const std::string& nodeName,
  std::vector<std::shared_ptr<Node>>& currentGroup,
  std::string& currentPartition,
  std::vector<NodeGroup>& groups,
  std::unordered_set<std::string>& visited)
{
    if (!visited.insert(nodeName).second) {
        return;
    }

    auto node = application->getNodes().at(nodeName);
    currentGroup.push_back(node);

    // If this node is a partitioned‐stateful, set (or re‐set) the key:
    if (node->type == PARTITIONED_STATEFUL) {
        currentPartition = node->partitionBy;
    }

    // no successors? finish this group:
    auto connIt = application->getConnections().find(nodeName);
    if (connIt == application->getConnections().end()) {
        // push (nodes, partitionKey) as one tuple:
        groups.emplace_back(currentGroup, currentPartition);
        return;
    }

    // for each successor, decide whether to stay in this group…
    for (const auto& succName : connIt->second) {
        auto succ = application->getNodes().at(succName);
        bool joinCurrent = false;

        if (succ->type == STATELESS) {
            joinCurrent = true;
        } else if (succ->type == PARTITIONED_STATEFUL) {
            // If local group has stateful nodes, start a new group:
            bool hasStateful = false;
            for (const auto& node : currentGroup) {
                if (node->type == STATEFUL) {
                    hasStateful = true;
                    break;
                }
            }
            if (!hasStateful) {
                if (currentPartition == NONE_STRING ||
                    currentPartition == succ->partitionBy) {
                    joinCurrent = true;
                }
            }
        }

        if (joinCurrent) {
            // recurse in same group (partition stays as‐is)
            groupNodesHelper(
              succName, currentGroup, currentPartition, groups, visited);
        } else {
            // close out the current group first:
            groups.emplace_back(currentGroup, currentPartition);

            // start a brand‐new group for this branch, with fresh state:
            std::vector<std::shared_ptr<Node>> newGroup;
            std::string newPartition = NONE_STRING;
            groupNodesHelper(succName, newGroup, newPartition, groups, visited);
        }
    }
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
    SPDLOG_INFO("StateAwareScheduler: Reschedule the application according to "
                "the metrics");

    // 1. Calculate the workload of each operator.
    if (!application) {
        SPDLOG_WARN("No application registered");
        return;
    }
    auto& appNodes = application->getNodes();
    if (appNodes.empty()) {
        SPDLOG_WARN("No nodes recorded in the application");
        return;
    }

    // Get the minimum processed tuples operator in the application.
    long minimizedInput = std::numeric_limits<long>::max();
    for (auto& [nodeName, node] : appNodes) {
        if (node->processedTuples < minimizedInput) {
            minimizedInput = node->processedTuples;
        }
    }

    if (minimizedInput == std::numeric_limits<long>::max() ||
        minimizedInput == 0) {
        SPDLOG_WARN("Minimized {} input is not valid", minimizedInput);
        return;
    }

    // Calculate the workload of each operator and total workload (number of
    // requests processed by operators)
    // Preworkload is the number of requests processed by each operator in the
    // last period.
    double totalPreWorkload = 0;
    for (auto& [nodeName, node] : appNodes) {
        node->preWorkload = std::round(
          static_cast<double>(node->processedTuples) / minimizedInput);

        // It should never be less than 1.0, just in case.
        if (node->preWorkload < 1.0) {
            node->preWorkload = 1.0;
        }
        totalPreWorkload += node->preWorkload;
    }

    // Update the resource required for each operator (number of workers).
    const int NUM_WORKERS = hostMap.size();

    struct Quantised
    {
        std::shared_ptr<Node> node; // pointer to the original node object
        int units;                  // 1 unit == 0.1 workers
        double frac;                // fractional part kept for tie‑breaks
    };

    const int TOTAL_UNITS = NUM_WORKERS * 10; // 0.1‑granularity budget
    std::vector<Quantised> bucket;
    bucket.reserve(appNodes.size());

    int usedUnits = 0;

    //--------------------------------------------------------------------------
    // step 1: convert each exact share to "baseUnits" (floor in 0.1 steps)
    //--------------------------------------------------------------------------
    for (auto& [name, n] : appNodes) {

        double exactShare = static_cast<double>(NUM_WORKERS) * n->preWorkload /
                            totalPreWorkload; // original
        double rawUnitsD = exactShare * 10.0; // 0.1 units
        int baseUnits = static_cast<int>(std::floor(rawUnitsD));

        if (baseUnits == 0) // enforce the 0.1 minimum
            baseUnits = 1;

        double frac = rawUnitsD - baseUnits; // 0 ≤ frac < 1

        bucket.push_back({ n, baseUnits, frac });
        usedUnits += baseUnits;
    }

    //--------------------------------------------------------------------------
    // step 2: distribute (+) or steal (–) leftover units
    //--------------------------------------------------------------------------

    // 2a. We have *too few* units → hand out leftovers to highest "frac"
    int remain = TOTAL_UNITS - usedUnits;
    if (remain > 0) {
        std::sort(bucket.begin(),
                  bucket.end(),
                  [](const Quantised& a, const Quantised& b) {
                      return a.frac > b.frac; // descending
                  });
        for (int i = 0; i < remain; ++i)
            bucket[i % bucket.size()].units += 1;
    }

    // 2b. We have *too many* units → take units from lowest "frac"
    else if (remain < 0) {
        remain = -remain; // units to remove   (> 0)

        // sort buckets by ascending fractional remainder (cheapest to cut)
        std::vector<Quantised*> order;
        order.reserve(bucket.size());
        for (auto& q : bucket)
            order.push_back(&q);

        std::sort(order.begin(),
                  order.end(),
                  [](const Quantised* a, const Quantised* b) {
                      return a->frac < b->frac;
                  });

        // round‑robin removal: at most one unit per bucket per sweep
        std::size_t i = 0;
        while (remain > 0) {
            Quantised* q = order[i];

            if (q->units > 1) { // keep ≥ 0.1 (1 unit) per bucket
                --q->units;
                --remain;
            }

            i = (i + 1) % order.size(); // next bucket in the cycle
            /* If we've looped back to the beginning and could not remove
               anything on this entire sweep, it means all buckets are at
               their minimum 0.1 already. */
            if (i == 0 && remain > 0) {
                SPDLOG_ERROR("Unable to rebalance to integer‑0.1 units");
                throw std::runtime_error("quantisation failed");
            }
        }
    }

    //--------------------------------------------------------------------------
    // step 3: write the quantised values back to each node
    //--------------------------------------------------------------------------
    for (auto& q : bucket)
        q.node->reqResource = q.units / 10.0; // 0.1‑granularity

    for (auto& [name, n] : appNodes) {
        SPDLOG_DEBUG(
          "Node {} requires {} resource units", name, n->reqResource);
    }

    application->showConnections();

    // 2. Group the operators into groups.
    // TODO - combine the partitioned stateful operaoters with the same
    // attribute.
    std::vector<NodeGroup> groups;
    std::unordered_set<std::string> visited;

    for (const auto& inputNode : application->getInputNodes()) {
        if (visited.count(inputNode))
            continue;

        std::vector<std::shared_ptr<Node>> startGroup;
        std::string startPartition = NONE_STRING;

        groupNodesHelper(
          inputNode, startGroup, startPartition, groups, visited);
    }

    // 3. Map groups to hosts.
    std::map<std::string, double> workerRemaining;
    for (const auto& [ip, host] : hostMap) {
        workerRemaining[ip] = 1.0;
    }

    // For each group, how much resource is allocated to his worker. The order
    // of groups is the same as groups variable.
    // MAP <ip, allocated resource>
    std::vector<std::map<std::string, double>> groupAllocations;

    // For each group, compute the total required resource and assign workers.
    for (size_t groupIndex = 0; groupIndex < groups.size(); ++groupIndex) {
        double groupReqResource = 0.0;
        for (const auto& node : std::get<0>(groups[groupIndex])) {
            groupReqResource += node->reqResource;
        }

        SPDLOG_INFO(
          "Group {} requires total resource: {}", groupIndex, groupReqResource);

        // Map of this group resource distribution (IP -> allocated resource).
        std::map<std::string, double> groupAllocation;
        // Greedily fill the group requirement from available workers.
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

        // Log the per-worker allocation for this group.
        SPDLOG_INFO("Group {} allocated resources:", groupIndex);
        for (const auto& [ip, allocated] : groupAllocation) {
            SPDLOG_INFO("  Worker {}: {} resource units", ip, allocated);
        }
    }

    // 4. Arrange the states accordingly.
    // We change the parallelism of partitioned stateful operators to the number
    // of functions. We don't change the parallelism of the stateful function.
    // For each group, we assign the state to allocated workers.

    std::map<std::string, std::string> newStateHost;
    std::map<std::string, int> newFuncPar;
    // MAP <USER_FUNC_PARALLELISM, MAP<IP, proportion>>
    std::map<std::string, std::map<std::string, int>> newStatelessReqWeight;
    // MAP <USER_FUNC, MAP<PARALLELISM_IDX, proportion>>
    std::map<std::string, std::map<int, int>> newParStateReqWeight;

    // For each group, statistics its states and assigns states to workers.
    for (size_t groupIndex = 0; groupIndex < groups.size(); ++groupIndex) {
        const auto& group = std::get<0>(groups[groupIndex]);
        auto groupAllocation = groupAllocations[groupIndex];

        // Assign the stateful states to workers at first, we assign the
        // stateful states at first in greedy method (with maximum avialble
        // resource).
        for (const auto& node : group) {
            if (node->type != STATEFUL) {
                continue;
            }
            std::string statefulName = node->name;
            // We don't change the parallelism of the stateful function.
            int para = node->parallelism;
            newFuncPar[statefulName] = para;
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
                newStateHost[statefulName + "_" + std::to_string(i)] =
                  bestWorker;
                // Update the available resource for the selected worker.
                double reqRes = (node->reqResource) / para;
                groupAllocation[bestWorker] =
                  std::max(0.0, groupAllocation[bestWorker] - reqRes);
            }
        }

        // Then, we assign all the partitioned stateful operators and stateless
        // operators. For partitioned stateful, we assign it to every workers,
        // (who has higher than 10% resource).
        // If this group doesn't has other type of operators, we skip it.
        int numOtherType = 0;
        for (const auto& node : group) {
            if (node->type == STATEFUL) {
                continue;
            }
            numOtherType++;
        }
        if (numOtherType == 0) {
            continue;
        }

        std::map<std::string, int> ipWeight;
        for (const auto& [ip, alloc] : groupAllocation) {
            ipWeight[ip] = std::lround(alloc * weightFactor);
        }
        // Do assign the stateless and partitioned stateful operators.
        for (const auto& node : group) {
            if (node->type == STATELESS) {
                // Stateless operator, assign it to all workers.
                std::string statefulName = node->name;
                newStatelessReqWeight[statefulName + "_0"] = ipWeight;
            }
            if (node->type == PARTITIONED_STATEFUL) {
                std::string statefulName = node->name;
                int index = 0;
                newFuncPar[statefulName] = ipWeight.size();
                for (const auto& [ip, weight] : ipWeight) {
                    std::string userFuncPar =
                      statefulName + "_" + std::to_string(index);
                    newParStateReqWeight[statefulName][index] = weight;
                    newStateHost[userFuncPar] = ip;
                    index++;
                }
            }
        }
    }

    // Since some stateless operators
    std::map<std::string, std::string> newOptsCollocateMap;
    std::map<std::string, std::string> newOptsCollocateHeadMap;
    for (const auto& group : groups) {
        const auto& groupNodes = std::get<0>(group);
        const auto& groupPartition = std::get<1>(group);
        if (groupPartition == NONE_STRING) {
            continue;
        }
        for (int i = 0; i < groupNodes.size(); i++) {
            const auto& node = groupNodes[i];
            // We only consider the stateless operators.
            if (node->type != STATELESS) {
                continue;
            }
            for (int j = i + 1; j < groupNodes.size(); j++) {
                const auto& rearNode = groupNodes[j];
                if (rearNode->type != PARTITIONED_STATEFUL) {
                    continue;
                }
                // If later nodes has partitioned stateful operator, we have to
                // mark it as collocate with a partitioned stateful operator.
                // If it is the head of the group or it only has stateful
                // operators before. It is the head.
                if (i == 0) {
                    newOptsCollocateHeadMap[node->name] = rearNode->name;
                    break;
                }
                // If i is connected by a stateful operator.
                else {
                    auto sourceNodes = application->getSource(node->name);
                    for (const auto& sourceNode : sourceNodes) {
                        if (sourceNode->type == STATEFUL) {
                            newOptsCollocateHeadMap[node->name] =
                              rearNode->name;
                            break;
                        }
                    }
                }
                newOptsCollocateMap[node->name] = rearNode->name;
                break;
            }
        }
    }

    // SHOW the new state allocation
    for (size_t groupIndex = 0; groupIndex < groups.size(); ++groupIndex) {
        SPDLOG_INFO("Group {} mapping:", groupIndex);
        const auto& group = std::get<0>(groups[groupIndex]);

        // Log the nodes in the current group.
        SPDLOG_INFO("  Nodes in group:");
        for (const auto& node : group) {
            SPDLOG_INFO("    Node: {} (Type: {})",
                        node->name,
                        nodeTypeToString(node->type));
        }

        // Log the worker allocation for this group.
        const auto& groupAllocation = groupAllocations[groupIndex];
        SPDLOG_INFO("  Worker allocations:");
        for (const auto& [ip, allocated] : groupAllocation) {
            SPDLOG_INFO("    Worker {}: {} resource units", ip, allocated);
        }
    }

    // Log the final state mapping info.
    SPDLOG_INFO("State mapping (state -> worker):");
    for (const auto& [stateName, workerIP] : newStateHost) {
        SPDLOG_INFO("  {} assigned to worker {}", stateName, workerIP);
    }

    // Log the share of stateless operators and partitioned stateful operators.
    SPDLOG_INFO("Stateless operators share:");
    for (const auto& [userFuncPar, hostShares] : newStatelessReqWeight) {
        for (const auto& [host, share] : hostShares) {
            // one flat line per <operator, host> pair
            SPDLOG_INFO("stateless_dist  op={}  host={}  share={}",
                        userFuncPar,
                        host,
                        share);
        }
    }
    SPDLOG_INFO("Partitioned stateful operators share:");
    for (const auto& [userFunc, indexShares] : newParStateReqWeight) {
        for (const auto& [index, share] : indexShares) {
            std::string userFuncPar = userFunc + "_" + std::to_string(index);

            // one flat line per <operator, host> pair
            SPDLOG_INFO(
              "partitioned_stateful_dist  op={}  inst={} host={} share={}",
              userFunc,
              userFuncPar,
              newStateHost[userFuncPar],
              share);
        }
    }

    // Update the state host and parallelism info.
    stateHost = newStateHost;
    functionParallelism = newFuncPar;
    statelessReqWeight = newStatelessReqWeight;
    parStateReqWeight = newParStateReqWeight;
    setOptsCollocateMap(newOptsCollocateMap);
    setOptsCollocateHeadMap(newOptsCollocateHeadMap);

    stateHashRing.clear();

    redis::Redis& redis = redis::Redis::getState();
    redis.flushAll();
    for (const auto& [stateName, ip] : newStateHost) {
        registerStateToRedis(stateName, ip);
    }

    for (const auto& [userFunction, partitionBy] : statePartitionBy) {
        if (!functionParallelism.contains(userFunction)) {
            SPDLOG_ERROR("Function {} has no parallelism", userFunction);
            throw std::runtime_error("Function parallelism not found");
        }
        auto weightDist = parStateReqWeight[userFunction];
        stateHashRing[userFunction] =
          std::make_shared<faabric::util::ConsistentHashRing>(weightDist);
    }
}

void StateAwareScheduler::setScheduleMode(int mode)
{
    scheduleMode = mode;
}

void StateAwareScheduler::resetScheduler()
{
    SPDLOG_INFO("Flushing state information");
    stateRbCounter.store(0);

    redis::Redis& redis = redis::Redis::getState();
    redis.flushAll();
    functionParallelism.clear();
    functionCounter.clear();
    stateHost.clear();
    stateHashRing.clear();
    statePartitionBy.clear();
    funcStateRegMap.clear();
    statelessReqWeight.clear();
    parStateReqWeight.clear();
    optsCollocateMap.clear();
}

} // namespace faabric::batch_scheduler
