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

struct FunctionStateInfo
{
    std::string functionName;
    std::string partitionBy;
    std::string stateKey;
    int parallelism;
    std::map<int, std::string> stateHost;
};

std::string to_string(const FunctionStateInfo& info);

class StateAwareScheduler : public BatchScheduler
{
  public:
    /* Virtual functions which must be implemented. However, it is not used in
     * StateAwareScheduler.
     */
    std::shared_ptr<SchedulingDecision> makeSchedulingDecision(
      HostMap& hostMap,
      const InFlightReqs& inFlightReqs,
      std::shared_ptr<faabric::BatchExecuteRequest> req) override;

    bool updateFuncStatePar(const std::string& userFunction,
                            int newPar,
                            const HostMap& hostMap);

    // the following functions are public only for tests.
    void increaseFuncStatePar(const std::string& userFunction,
                              int numIncrease,
                              const HostMap& hostMap);
    
    void reduceFuncStatePar(const std::string& userFunction,
                            int numDecrease,
                            const HostMap& hostMap);

  private:
    bool isFirstDecisionBetter(
      std::shared_ptr<SchedulingDecision> decisionA,
      std::shared_ptr<SchedulingDecision> decisionB) override;

    std::vector<Host> getSortedHosts(
      HostMap& hostMap,
      const InFlightReqs& inFlightReqs,
      std::shared_ptr<faabric::BatchExecuteRequest> req,
      const DecisionType& decisionType) override;

  public:
    // ------------------------------------------
    // The following functions are implemented in StateAwareScheduler
    // ------------------------------------------

    StateAwareScheduler()
    {
        // Initialization code here
        funcStateInitializer();
    }

    virtual ~StateAwareScheduler() = default;

    virtual std::string scheduleMessage(const HostMap& hostMap,
                                        const std::unique_ptr<Message>& msg);

    std::vector<std::string> scheduleMessagesBatch(
      const HostMap& hostMap,
      const std::vector<std::unique_ptr<faabric::Message>>& msgs);

    bool repartitionParitionedState(
      std::string userFunction,
      std::shared_ptr<std::map<std::string, std::string>> oldStateHost);

    virtual void resetScheduler();

    const std::map<std::string, int>& getFunctionParallelismMap() const
    {
        return functionParallelism;
    }

    bool registerFunctionState(const std::string& userFunction,
                               const std::string& partitionBy,
                               const std::string& stateKey,
                               const HostMap& hostMap);

    const std::map<std::string, FunctionStateInfo> getStateInfo();

  protected:
    // scheduler lock
    std::shared_mutex scheduleMx;

    // The counter used for round robin scheduling.
    int rbCounter = 0;
    std::atomic<unsigned int> atomicRbCounter{ 0 };

    /***
     * The following maps are used to store the state of the functions.
     */
    // Function_User : Parallelism
    std::map<std::string, int> functionParallelism;
    // Function_User : Counter. It is used for shuffle grouping.
    std::map<std::string, int> functionCounter;
    // Function_User_ParallelismIndex : Host
    std::map<std::string, std::string> stateHost;
    // Function_User : hashRing
    std::map<std::string, std::shared_ptr<util::ConsistentHashRing>>
      stateHashRing;
    // Only partitioned stateful function will be registered here.
    // FunctionUser : Input Parition Key
    std::map<std::string, std::string> statePartitionBy;

    int maxParallelism;

    // TODO - This can be detected by state server and planner, but logic will
    // be extreamly complex. (How to create new function state, BALABALA)
    // Key is User-function : Value is <parititonInputKey, partitionStateKey>
    // It is only used for initialization.
    std::map<std::string, std::tuple<std::string, std::string>> funcStateRegMap;

    void registerState(const HostMap& hostMap,
                       std::string userFunc,
                       int parallelism = 1);

    void funcStateInitializer();

    // Message Type : 0 - Stateless, 1 - Stateful, 2 - Paritioned Stateful

    HashAndParallelismInfo getHashAndParallelismIndex(
      const std::string& userFunction,
      const faabric::Message& msg);
};
}