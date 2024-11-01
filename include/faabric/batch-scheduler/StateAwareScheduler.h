#pragma once

#include <faabric/batch-scheduler/BatchScheduler.h>
#include <faabric/planner/FunctionMetrics.h>
#include <faabric/util/config.h>
#include <faabric/util/hash.h>

#include <map>
#include <set>
#include <shared_mutex>
#include <string>
#include <tuple>

namespace faabric::batch_scheduler {

struct HashAndParallelismInfo
{
    int messageType;
    size_t hash;
    int parallelismIdx;
};

class StateAwareScheduler final : public BatchScheduler
{
  public:
    StateAwareScheduler()
    {
        // Initialization code here
        funcStateInitializer();
    }

    std::string scheduleMessage(const HostMap& hostMap,
                                const std::unique_ptr<Message>& msg);

    std::vector<std::string> scheduleMessagesBatch(
      const HostMap& hostMap,
      const std::vector<std::unique_ptr<faabric::Message>>& msgs);

    std::shared_ptr<SchedulingDecision> makeSchedulingDecision(
      HostMap& hostMap,
      const InFlightReqs& inFlightReqs,
      std::shared_ptr<faabric::BatchExecuteRequest> req) override;

    // the following functions are public only for tests.
    std::shared_ptr<std::map<std::string, std::string>>
    increaseFunctionParallelism(int numIncrease,
                                const std::string& userFunction,
                                const HostMap& hostMap);

    bool repartitionParitionedState(
      std::string userFunction,
      std::shared_ptr<std::map<std::string, std::string>> oldStateHost);

    void flushStateInfo();

    const std::map<std::string, int>& getFunctionParallelismMap() const
    {
        return functionParallelism;
    }

    void registerFunctionState(const std::string& userFunction,
                               const std::string& partitionBy,
                               const std::string& stateKey);

  private:
    bool isFirstDecisionBetter(
      std::shared_ptr<SchedulingDecision> decisionA,
      std::shared_ptr<SchedulingDecision> decisionB) override;

    std::vector<Host> getSortedHosts(
      HostMap& hostMap,
      const InFlightReqs& inFlightReqs,
      std::shared_ptr<faabric::BatchExecuteRequest> req,
      const DecisionType& decisionType) override;

    // The counter used for round robin scheduling.
    int rbCounter = 0;
    std::atomic<unsigned int> atomicRbCounter{ 0 };

    // scheduler lock
    std::shared_mutex scheduleMx;

    /***
     * The following maps are used to store the state of the functions.
     */
    // FunctionUser : Parallelism
    std::map<std::string, int> functionParallelism;
    // FunctionUser : Counter. It is used for shuffle grouping.
    std::map<std::string, int> functionCounter;
    // FunctionUser : Host
    std::map<std::string, std::string> stateHost;
    // FunctionUser : hashRing
    std::map<std::string, std::shared_ptr<util::ConsistentHashRing>>
      stateHashRing;
    // Only partitioned stateful function will be registered here.
    // FunctionUser : Input Parition Key
    std::map<std::string, std::string> statePartitionBy;

    int maxParallelism;
    // TODO - This can be detected by planner.
    // Function Source: All the chained functions invoked subsequently
    std::map<std::string, std::vector<std::string>> funcChainedMap;

    // TODO - This can be detected by state server and planner, but logic will
    // be extreamly complex. (How to create new function state, BALABALA)
    // Key is User-function : Value is <parititonInputKey, partitionStateKey>
    std::map<std::string, std::tuple<std::string, std::string>> funcStateRegMap;

    void initializeState(const HostMap& hostMap,
                         std::string userFunc,
                         int parallelism = 1);

    void funcStateInitializer();

    // Message Type : 0 - Stateless, 1 - Stateful, 2 - Paritioned Stateful

    HashAndParallelismInfo getHashAndParallelismIndex(
      const std::string& userFunction,
      const faabric::Message& msg);
};
}