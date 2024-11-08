#include <faabric/batch-scheduler/SchedulingDecision.h>
#include <faabric/batch-scheduler/StateAwareScheduler.h>
#include <faabric/state/FunctionStateClient.h>
#include <faabric/util/batch.h>
#include <faabric/util/logging.h>
#include <faabric/util/serialization.h>
#include <faabric/util/string_tools.h>

#define MAIN_KEY_PREFIX "main_"

namespace faabric::batch_scheduler {
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

void StateAwareScheduler::registerFunctionState(const std::string& userFunction,
                                                const std::string& partitionBy,
                                                const std::string& stateKey)
{
    if (partitionBy == "None" || stateKey == "None") {
        SPDLOG_INFO("Registering function state {} with no partitioning",
                    userFunction);
        funcStateRegMap[userFunction] = std::make_tuple("", "");
    } else {
        SPDLOG_INFO("Registering function state {} with partitioning by {} and "
                    "state key {}",
                    userFunction,
                    partitionBy,
                    stateKey);
        funcStateRegMap[userFunction] = std::make_tuple(partitionBy, stateKey);
    }
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

bool registerStateToHost(const std::string& userFunctionParIdx,
                         const std::string& host,
                         const std::string& partitionBy,
                         const std::string stateKey)
{
    SPDLOG_INFO(
      "Registering state {} to host {}, partition by: {} and stateKey : {}",
      userFunctionParIdx,
      host,
      partitionBy,
      stateKey);
    // Update Redis Information
    redis::Redis& redis = redis::Redis::getState();
    std::string mainKey = MAIN_KEY_PREFIX + userFunctionParIdx;
    std::vector<uint8_t> mainIPBytes = faabric::util::stringToBytes(host);
    redis.set(mainKey, mainIPBytes);
    // Send Create information in the host.
    auto [user, function, parallelismId] =
      faabric::util::splitUserFuncPar(userFunctionParIdx);
    state::FunctionStateClient cli(
      user, function, std::stoi(parallelismId), host);
    cli.createState(stateKey);
    return true;
}

void StateAwareScheduler::initializeState(const HostMap& hostMap,
                                          std::string userFunc,
                                          int parallelism)
{
    SPDLOG_INFO("Initializing function state for {} with parallelism {}",
                userFunc,
                parallelism);
    if (parallelism == 1) {
        // Default parallelism is 1.
        functionParallelism[userFunc] = 1;
        functionCounter[userFunc] = 0;
        // The default parallelism Id is 0
        std::string funcParaId = userFunc + "_0";
        int hostIdx = rbCounter++ % hostMap.size();
        std::string host = getNthKey(hostMap, hostIdx);
        stateHost[funcParaId] = host;
        // If the State is partitionedState, register it.
        std::string partitionBy = std::get<0>(funcStateRegMap[userFunc]);
        std::string stateKey = std::get<1>(funcStateRegMap[userFunc]);
        if (partitionBy != "" && stateKey != "") {
            statePartitionBy[userFunc] = partitionBy;
        }
        // Register the state to the host.
        registerStateToHost(funcParaId, host, partitionBy, stateKey);
    } else {
        // Otherwise initialize it. (We reset all the related stateinfo now)
        functionParallelism[userFunc] = 0;
        functionCounter[userFunc] = 0;
        std::string partitionBy = std::get<0>(funcStateRegMap[userFunc]);
        std::string stateKey = std::get<1>(funcStateRegMap[userFunc]);
        if (partitionBy != "" && stateKey != "") {
            statePartitionBy[userFunc] = partitionBy;
        }
        // Initialize the StateHost and stateHashRing
        increaseFunctionParallelism(parallelism, userFunc, hostMap);
    }
}

// The BinPack's scheduler decision algorithm is very simple. It first sorts
// hosts (i.e. bins) in a specific order (depending on the scheduling type),
// and then starts filling bins from begining to end, until it runs out of
// messages to schedule
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
    if (funcStateRegMap.contains(userFunc)) {
        // If function-state has not been initialized, initialize it.
        faabric::util::FullLock lock(scheduleMx);
        if (!functionParallelism.contains(userFunc)) {
            initializeState(hostMap, userFunc);
        }
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

// TODO - change it to increase or decrease function parallelism. It should
// return the old stateHost instead of the true/false
void StateAwareScheduler::increaseFunctionParallelism(
  int numIncrease,
  const std::string& userFunction,
  const HostMap& hostMap)
{
    SPDLOG_INFO("Increasing {} parallelism for {}", numIncrease, userFunction);
    // Double check if the function exists
    if (functionParallelism.find(userFunction) == functionParallelism.end()) {
        SPDLOG_ERROR("Function {} does not exist as function-state function",
                     userFunction);
        return;
    }
    // Construct the userFunctionIdx for the new parallelism level
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
        registerStateToHost(userFunction + "_" + std::to_string(idx),
                            minHost,
                            partitionBy,
                            stateKey);
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

void StateAwareScheduler::flushStateInfo()
{
    SPDLOG_INFO("Flushing state information");
    rbCounter = 0;
    atomicRbCounter.store(0);

    functionParallelism.clear();
    functionCounter.clear();
    stateHost.clear();
    stateHashRing.clear();
    statePartitionBy.clear();
    funcStateRegMap.clear();
}

} // namespace faabric::batch_scheduler
