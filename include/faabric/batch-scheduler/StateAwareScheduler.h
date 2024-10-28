#pragma once

#include <faabric/batch-scheduler/BatchScheduler.h>
#include <faabric/planner/FunctionMetrics.h>
#include <faabric/util/config.h>
#include <faabric/util/hash.h>

#include <map>
#include <set>
#include <string>
#include <tuple>
#include <shared_mutex>

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

    std::string scheduleMessage(const HostMap& hostMap, const std::unique_ptr<Message>& msg);
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

    void updateParallelism(
      const HostMap& hostMap,
      std::map<std::string, faabric::planner::FunctionMetrics> metrics);

    const std::map<std::string, std::string>& getStateHostMap() const
    {
        return stateHost;
    }

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

    /***
     * example should be like:
     * Function Parallelism Map:
     * Function: demo_function1, Parallelism: 1
     * Function: demo_function2, Parallelism: 1

     * State Host Map:
     * Function: demo_function1_0, Host: 172.31.0.6
     * Function: demo_function2_0, Host: 172.31.0.6

     * State Partition By Map:
     * Function: demo_function1, Partitioned By: k1
     * Function: demo_function2, Partitioned By: k2
    ***/
    // bool registerState(const std::string& userFunction,
    //                    const std::string& host,
    //                    const std::string& partitionBy = "");
    // return the message type, hash and parallelism index of this function,
    // which is align with state.

    // Message Type : 0 - Stateless, 1 - Stateful, 2 - Paritioned Stateful

    HashAndParallelismInfo getHashAndParallelismIndex(
      const std::string& userFunction,
      const faabric::Message& msg);
};
}