#pragma once

#include <faabric/batch-scheduler/Application.h>
#include <faabric/batch-scheduler/BatchScheduler.h>
#include <faabric/batch-scheduler/RuntimeSummary.h>
#include <faabric/planner/FunctionMetrics.h>
#include <faabric/util/config.h>
#include <faabric/util/hash.h>
#include <faabric/util/locks.h>

#include <map>
#include <set>
#include <shared_mutex>
#include <string>
#include <tuple>

namespace faabric::batch_scheduler {

// // List of map of Nodes.
// using FaaSFlowNodeGroup = std::map<std::string, std::shared_ptr<Node>>;
// List of Nodes, and the partition key of the group.
using NodeGroup = std::tuple<std::vector<std::shared_ptr<Node>>, std::string>;
inline const std::string NONE_STRING = "None";

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
    }

    virtual ~StateAwareScheduler() = default;

    void setScheduleMode(int mode);

    virtual void resetScheduler();

    bool registerApp(std::unique_ptr<batch_scheduler::Application> app);

    void initApp(const HostMap& hostMap);

    std::string scheduleStatelessMessageRB(std::string& userFunc,
                                           const HostMap& hostMap,
                                           const std::unique_ptr<Message>& msg);

    std::string scheduleStatelessMessageApportion(
      std::string& userFunc,
      const HostMap& hostMap,
      const std::unique_ptr<Message>& msg);

    std::string scheduleStatelessMessageFaaSFlow(
      std::string& userFunc,
      const HostMap& hostMap,
      const std::unique_ptr<Message>& msg);

    std::string scheduleStatelessMessageLocal(
      std::string& userFunc,
      const HostMap& hostMap,
      const std::unique_ptr<Message>& msg);

    std::string scheduleStatefulMessage(std::string& userFunc,
                                        const std::unique_ptr<Message>& msg);

    virtual std::string scheduleMessage(const HostMap& hostMap,
                                        const std::unique_ptr<Message>& msg);

    std::vector<std::string> scheduleMessagesBatch(
      const HostMap& hostMap,
      const std::vector<std::unique_ptr<faabric::Message>>& msgs);

    /**
     * Used for runtime rescheduling
     ***/
    bool repartitionParitionedState(
      std::string userFunction,
      std::shared_ptr<std::map<std::string, std::string>> oldStateHost);

    const std::map<std::string, int>& getFunctionParallelismMap() const
    {
        return functionParallelism;
    }

    const std::map<std::string, FunctionStateInfo> getStateInfo();

    void updateApp(
      const std::map<std::string, long>& nodeWorkloads,
      const std::map<std::string, std::map<std::string, int>> edgeWeightMap);

    void rescheduleApp(const HostMap& hostMap);

    std::tuple<std::vector<NodeGroup>,
               std::vector<std::map<std::string, double>>,
               std::map<std::string, double>>
    groupNodesGreedily(const HostMap& hostMap);

    void rescheduleAppFaaSFlow(const HostMap& hostMap);

    const std::map<std::string, std::shared_ptr<util::ConsistentHashRing>>&
    getStateHashRing() const
    {
        return stateHashRing;
    }

    const std::map<std::string, ScheduledOperator>& getScheduledOperatorsMap()
      const
    {
        return scheduledOperatorsMap;
    }

    bool nodeCollocation(const std::string& current,
                         const std::string& partitionKey,
                         const std::unordered_set<std::string>& groupNodeNames);

    void collectCollocation(
      const std::string& current,
      const std::string& psName,
      const std::string& partitionKey,
      std::map<std::string, std::string>& collocateMap,
      const std::unordered_set<std::string>& groupNodeNames);

    std::map<std::string, ScheduledOperator> buildScheduledOperatorsForGroup(
      int groupId,
      const std::vector<std::shared_ptr<Node>>& group,
      const std::map<std::string, std::string>& newOptsCollocateMap,
      const std::map<std::string, std::map<std::string, int>>&
        newStatelessReqWeight,
      const std::map<std::string, std::map<int, int>>& newParStateReqWeight)
      const;

    void runtimeDistTune(
      const std::map<std::string, std::map<std::string, int>>& observeDistMap);

    void printScheduleInfomation() const;

  protected:
    // scheduler lock
    std::shared_mutex scheduleMx;

    bool isplanner = true;
    int scheduleMode = 0;

    // hostAssign Counter is used when assign states to the hosts.
    std::atomic<unsigned int> stateRbCounter{ 0 };

    int weightFactor = 1000;

    RuntimeSummary runtimeSummary;

    /***
     * The following maps are used to store the state of the functions.
     */
    // Function_User : Parallelism
    std::map<std::string, int> functionParallelism;
    // Function_User_ParallelismIndex : Host
    std::map<std::string, std::string> stateHost;
    // Function_User : hashRing. It is used for partitioned stateful operators.
    std::map<std::string, std::shared_ptr<util::ConsistentHashRing>>
      stateHashRing;
    // Only partitioned stateful function will be registered here.
    // FunctionUser : Input Parition Key
    std::map<std::string, std::string> statePartitionBy;
    // Function_User : Counter. It is used for shuffle grouping.
    std::shared_mutex counterMx;
    std::map<std::string, std::shared_ptr<std::atomic_uint>> counterTable;
    unsigned int getNextCounter(const std::string& userFunc);

    std::unique_ptr<batch_scheduler::Application> application;
    std::map<std::string, ScheduledOperator> scheduledOperatorsMap;

    // TODO - This can be detected by state server and planner, but logic will
    // be extreamly complex. (How to create new function state, BALABALA)
    // Key is User-function : Value is <parititonInputKey, partitionStateKey>
    // It is only used for initialization.
    std::map<std::string, std::tuple<std::string, std::string>> funcStateRegMap;

    bool registerFunctionState(Node& node, const HostMap& hostMap);

    bool updateFuncStatePar(const std::string& userFunction,
                            int newPar,
                            const HostMap& hostMap);

    // the following functions are public only for tests.
    void increaseFuncStatePar(const std::string& userFunction,
                              int numIncrease,
                              const HostMap& hostMap);

    void doRegisterState(const HostMap& hostMap,
                         std::string userFunc,
                         int parallelism = 1);

    // Message Type : 0 - Stateless, 1 - Stateful, 2 - Paritioned Stateful
    HashAndParallelismInfo getHashAndParallelismIndex(
      const std::string& userFunction,
      const faabric::Message& msg);

    void groupNodesHelper(const std::string& nodeName,
                          std::vector<NodeGroup>& groups,
                          std::unordered_set<std::string>& visited);

    void groupNodesStrictHelper(const std::string& nodeName,
                                std::vector<NodeGroup>& groups,
                                std::unordered_set<std::string>& visited);

    void groupNodesLooseHelper(const std::string& nodeName,
                               std::vector<NodeGroup>& groups,
                               std::unordered_set<std::string>& visited);
};
}