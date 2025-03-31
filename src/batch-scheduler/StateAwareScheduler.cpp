#include <faabric/batch-scheduler/SchedulingDecision.h>
#include <faabric/batch-scheduler/StateAwareScheduler.h>
#include <faabric/state/FunctionStateClient.h>
#include <faabric/util/batch.h>
#include <faabric/util/logging.h>
#include <faabric/util/serialization.h>
#include <faabric/util/string_tools.h>

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

    if (partitionBy == "None" || stateKey == "None") {
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
        // If the HashRing is not initialized, initialize it.
        if (stateHashRing.find(userFunction) == stateHashRing.end()) {
            stateHashRing[userFunction] =
              std::make_shared<faabric::util::ConsistentHashRing>(
                functionParallelism[userFunction]);
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
             functionCounter[userFunction]++ %
               functionParallelism[userFunction] };
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
    int hostIdx = rbCounter++ % hostMap.size();
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

std::string StateAwareScheduler::scheduleMessage(
  const HostMap& hostMap,
  const std::unique_ptr<faabric::Message>& msg)
{
    if (msg->user().empty() || msg->function().empty()) {
        throw std::runtime_error("User or function is empty");
    }
    std::string userFunc = msg->user() + "_" + msg->function();
    std::string host = "unknown";
    // For function-state function, assign near state
    if (functionParallelism.contains(userFunc)) {
        // If function-state has not been initialized, initialize it.
        faabric::util::FullLock lock(scheduleMx);
        // TODO - get parallelism is not thread safe now
        auto parallelismInfo = getHashAndParallelismIndex(userFunc, *msg);
        lock.unlock();
        std::string userFuncPar =
          userFunc + "_" + std::to_string(parallelismInfo.parallelismIdx);
        if (stateHost.find(userFuncPar) == stateHost.end()) {
            throw std::runtime_error("StateHost is not initialized");
        }
        host = stateHost[userFuncPar];
        // Register the parallelismIdx to it.
        // If Scheduling failed, the next scheduling will overwrite it.
        msg->set_messagetype(parallelismInfo.messageType);
        msg->set_hash(parallelismInfo.hash);
        msg->set_parallelismid(parallelismInfo.parallelismIdx);
    }
    // Otherwise the request by using round robin.
    else {
        int hostIdx = atomicRbCounter.fetch_add(1, std::memory_order_relaxed) %
                      hostMap.size();
        host = getNthKey(hostMap, hostIdx);
        msg->set_messagetype(0);
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
        reduceFuncStatePar(
          userFunction, functionParallelism[userFunction] - newPar, hostMap);
        return false;
    }

    return false;
}

// TODO - change it to increase or decrease function parallelism. It should
// return the old stateHost instead of the true/false
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
    if (statePartitionBy.contains(userFunction)) {
        // Change the state hashing ring
        stateHashRing[userFunction] =
          std::make_shared<faabric::util::ConsistentHashRing>(
            functionParallelism[userFunction]);
    }
}

void StateAwareScheduler::reduceFuncStatePar(const std::string& userFunction,
                                             int numDecrease,
                                             const HostMap& hostMap)
{
    SPDLOG_INFO("Reduce {} parallelism for {}", numDecrease, userFunction);

    int currentPar = functionParallelism[userFunction];
    int startIdx = currentPar - numDecrease;
    int endIdx = currentPar; // Exclusive

    for (int idx = startIdx; idx < endIdx; ++idx) {
        std::string userFunctionIdx = userFunction + "_" + std::to_string(idx);
        auto it = stateHost.find(userFunctionIdx);
        if (it != stateHost.end()) {
            stateHost.erase(it);
            deleteStateFromRedis(userFunctionIdx);
        } else {
            SPDLOG_WARN("No stateHost mapping found for {}", userFunctionIdx);
        }
    }

    functionParallelism[userFunction] -= numDecrease;
    SPDLOG_INFO("New parallelism for {} is {}",
                userFunction,
                functionParallelism[userFunction]);
    // If the state is partitioned, update the Hash method.
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
        if (succNode->type == STATELESS) {
            // For stateless nodes, continue in the same group.
            groupNodesHelper(succName, currentGroup, groups, visited);
        } else {
            // Else, start a new group.
            std::vector<std::shared_ptr<Node>> newGroup;
            groupNodesHelper(succName, newGroup, groups, visited);
            if (!newGroup.empty()) {
                groups.push_back(newGroup);
            }
        }
    }
}

void StateAwareScheduler::rescheduleApp(const HostMap& hostMap)
{
    SPDLOG_INFO("StateAwareScheduler: Reschedule the application according to "
                "the metrics");

    // Calculate the workload of each operator (node).
    if (!application) {
        SPDLOG_WARN("No application registered");
        return;
    }
    auto& appNodes = application->getNodes();
    if (appNodes.empty()) {
        SPDLOG_WARN("No nodes recorded in the application");
        return;
    }

    double totalPreWorkload = 0;
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

    for (auto& [nodeName, node] : appNodes) {
        node->preWorkload = std::round(
          static_cast<double>(node->processedTuples) / minimizedInput);

        // If no preworkload for any node, we pass this rescheduling.
        if (node->preWorkload <= 1.0) {
            node->preWorkload = 1.0;
        }
        totalPreWorkload += node->preWorkload;
    }

    // Update the resource required for each operator (number of workers).
    double totalResource = 0.0;
    for (auto& [nodeName, node] : appNodes) {
        node->reqResource =
          hostMap.size() * (node->preWorkload / totalPreWorkload);
        totalResource += node->reqResource;
    }
    if (totalResource != hostMap.size()) {
        SPDLOG_WARN("Total resource is not equal to the number of hosts");
        throw std::runtime_error("Total resource is not equal to the number of "
                                 "hosts");
        return;
    }

    // Combine operators into groups.
    std::vector<std::vector<std::shared_ptr<Node>>> groups;
    std::unordered_set<std::string> visited;
    for (const auto& inputNode : application->getInputNodes()) {
        if (visited.find(inputNode) != visited.end()) {
            continue;
        }
        std::vector<std::shared_ptr<Node>> currentGroup;
        groupNodesHelper(inputNode, currentGroup, groups, visited);
        if (!currentGroup.empty()) {
            groups.push_back(currentGroup);
        }
    }

    // Map groups to hosts.
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
        for (const auto& node : groups[groupIndex]) {
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

        if (groupReqResource > 0) {
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

    // Arrange the states accordingly.
    // We change the parallelism of partitioned stateful operators to the number
    // of functions. We don't change the parallelism of the stateful function.
    // For each group, we assign the state to allocated workers.

    std::map<std::string, double> workerAvailRes;
    for (const auto& [ip, host] : hostMap) {
        workerAvailRes[ip] = 1.0;
    }

    std::map<std::string, std::string> newStateHost;
    std::map<std::string, int> newFuncPar;

    // For each group, statistics its states and assigns states to workers.
    for (size_t groupIndex = 0; groupIndex < groups.size(); ++groupIndex) {
        const auto& group = groups[groupIndex];
        const auto& groupAllocation = groupAllocations[groupIndex];

        // MAP <operator, parallelism>
        std::map<std::string, int> statesMap;
        std::map<std::string, int> parStatesMap;

        for (const auto& node : group) {
            if (node->type == STATELESS) {
                continue;
            } else if (node->type == STATEFUL) {
                // We don't change the parallelism of the stateful function.
                int para = node->parallelism;
                statesMap[node->name] = para;
                newFuncPar[node->name] = para;
            } else if (node->type == PARTITIONED_STATEFUL) {
                // We change the parallelism of partitioned stateful operators
                // to the number of workers (allocated to this group).
                int para = groupAllocation.size();
                parStatesMap[node->name] = para;
                newFuncPar[node->name] = para;
            } else {
                SPDLOG_WARN("Unknown node type: {}", node->type);
                throw std::runtime_error("Unknown node type");
            }
        }

        // Assign the states to workers. For stateful, we assign it to
        // workers with avilable resource. For partitioned stateful, we
        // assign it to every workers.
        // Assign partitioned stateful to every worker.
        for (const auto& [parState, para] : parStatesMap) {
            auto node = appNodes[parState];
            for (int i = 0; i < para; ++i) {
                auto ip = getNthKey(groupAllocation, i);
                newStateHost[parState + "_" + std::to_string(i)] = ip;
                // Update the available resource.
                double reqRes = (node->reqResource) / para;
                workerAvailRes[ip] -= reqRes;
            }
        }

        // Assign stateful to workers with most available resource.
        for (const auto& [statefulName, para] : statesMap) {
            auto node = appNodes[statefulName];
            for (int i = 0; i < para; ++i) {
                // Find the worker with the maximum available resource in this
                // group.
                std::string bestWorker;
                double bestAvail = -1.0;
                for (const auto& [ip, value] : groupAllocation) {
                    if (workerAvailRes[ip] > bestAvail) {
                        bestAvail = workerAvailRes[ip];
                        bestWorker = ip;
                    }
                }
                newStateHost[statefulName + "_" + std::to_string(i)] =
                  bestWorker;
                // Update the available resource for the selected worker.
                double reqRes = (node->reqResource) / para;
                workerAvailRes[bestWorker] -= reqRes;
            }
        }
    }

    // SHOW the new state allocation
    for (size_t groupIndex = 0; groupIndex < groups.size(); ++groupIndex) {
        SPDLOG_INFO("Group {} mapping:", groupIndex);
        const auto& group = groups[groupIndex];

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

    // Update the state host and parallelism info.
    stateHost = newStateHost;
    functionParallelism = newFuncPar;
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
        stateHashRing[userFunction] =
          std::make_shared<faabric::util::ConsistentHashRing>(
            functionParallelism[userFunction]);
    }
}

void StateAwareScheduler::resetScheduler()
{
    SPDLOG_INFO("Flushing state information");
    rbCounter = 0;
    atomicRbCounter.store(0);

    redis::Redis& redis = redis::Redis::getState();
    redis.flushAll();
    functionParallelism.clear();
    functionCounter.clear();
    stateHost.clear();
    stateHashRing.clear();
    statePartitionBy.clear();
    funcStateRegMap.clear();
}

} // namespace faabric::batch_scheduler
