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

    bool registerApp(std::unique_ptr<batch_scheduler::Application> app);

    bool updateFuncStatePar(const std::string& userFunction,
                            int newPar,
                            const HostMap& hostMap);

    // the following functions are public only for tests.
    void increaseFuncStatePar(const std::string& userFunction,
                              int numIncrease,
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

    std::string scheduleStatelessMessageRB(std::string& userFunc,
                                           const HostMap& hostMap,
                                           const std::unique_ptr<Message>& msg);

    std::string scheduleStatelessMessageApportion(
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

    bool repartitionParitionedState(
      std::string userFunction,
      std::shared_ptr<std::map<std::string, std::string>> oldStateHost);

    virtual void resetScheduler();

    void setScheduleMode(int mode);

    const std::map<std::string, int>& getFunctionParallelismMap() const
    {
        return functionParallelism;
    }

    bool registerFunctionState(const std::string& userFunction,
                               const std::string& partitionBy,
                               const std::string& stateKey,
                               const HostMap& hostMap);

    const std::map<std::string, FunctionStateInfo> getStateInfo();

    void updateApp(const std::map<std::string, long>& nodeWorkloads);

    void rescheduleApp(const HostMap& hostMap);

    const std::map<std::string, std::shared_ptr<util::ConsistentHashRing>>&
    getStateHashRing() const
    {
        return stateHashRing;
    }

    std::map<std::string, std::map<std::string, int>> getStatelessReqWeight()
      const
    {
        return statelessReqWeight;
    }

    void setStatelessReqWeight(
      const std::map<std::string, std::map<std::string, int>>& w)
    {
        statelessReqWeight = w;
    }

    std::map<std::string, std::map<int, int>> getParStateReqWeight() const
    {
        return parStateReqWeight;
    }

    void setParStateReqWeight(
      const std::map<std::string, std::map<int, int>>& w)
    {
        parStateReqWeight = w;
    }

    std::map<std::string, std::string> getOptsCollocateMap() const
    {
        return optsCollocateMap;
    }

    void setOptsCollocateMap(const std::map<std::string, std::string>& w)
    {
        optsCollocateMap = w;
        runtimeSummary.setOptsCollocateMap(w);
    }

    std::map<std::string, std::string> getOptsCollocateHeadMap() const
    {
        return optsCollocateHeadMap;
    }

    void setOptsCollocateHeadMap(const std::map<std::string, std::string>& w)
    {
        optsCollocateHeadMap = w;
        runtimeSummary.setOptsCollocateHeadMap(w);
    }

    // The Req dist for both stateless and stateful functions.
    void updateReqDist();

    void reallocateSummaryDist(
      const std::map<std::string, std::map<std::string, int>>&
        sourceCountStats);

    bool nodeCollocation(const std::string& current,
                         const std::string& partitionKey,
                         const std::unordered_set<std::string>& groupNodeNames);

    void collectCollocation(
      const std::string& current,
      const std::string& psName,
      const std::string& partitionKey,
      std::map<std::string, std::string>& collocateMap,
      std::map<std::string, std::string>& headMap,
      const std::unordered_set<std::string>& groupNodeNames);

  protected:
    // scheduler lock
    std::shared_mutex scheduleMx;

    int scheduleMode = 0;

    int maxParallelism;

    // hostAssign Counter is used when assign states to the hosts.
    std::atomic<unsigned int> stateRbCounter{ 0 };

    std::shared_mutex counterMx;
    std::map<std::string, std::shared_ptr<std::atomic_uint>> counterTable;

    int weightFactor = 1000;
    // MAP <USER_FUNC_PARALLELISM, MAP<IP, proportion>>
    std::map<std::string, std::map<std::string, int>> statelessReqWeight;
    // MAP <USER_FUNC, MAP<PARALLELISM_IDX, proportion>>
    std::map<std::string, std::map<int, int>> parStateReqWeight;
    // MAP <STATELESS_OPERATOR, PARATITIONED_STATEFUL_OPERATOR>
    std::map<std::string, std::string> optsCollocateMap;
    std::map<std::string, std::string> optsCollocateHeadMap;

    RuntimeSummary runtimeSummary;

    /***
     * The following maps are used to store the state of the functions.
     */
    // Function_User : Parallelism
    std::map<std::string, int> functionParallelism;
    // Function_User : Counter. It is used for shuffle grouping.
    std::map<std::string, unsigned int> functionCounter;
    // Function_User_ParallelismIndex : Host
    std::map<std::string, std::string> stateHost;
    // Function_User : hashRing
    std::map<std::string, std::shared_ptr<util::ConsistentHashRing>>
      stateHashRing;
    // Only partitioned stateful function will be registered here.
    // FunctionUser : Input Parition Key
    std::map<std::string, std::string> statePartitionBy;

    std::unique_ptr<batch_scheduler::Application> application;

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

    // void groupNodesHelper(
    //   const std::string& nodeName,
    //   std::vector<std::shared_ptr<Node>>& currentGroup,
    //   std::vector<std::vector<std::shared_ptr<Node>>>& groups,
    //   std::unordered_set<std::string>& visited);

    void groupNodesHelper(const std::string& nodeName,
                          std::vector<std::shared_ptr<Node>>& currentGroup,
                          std::string& currentPartition,
                          std::vector<NodeGroup>& groups,
                          std::unordered_set<std::string>& visited);
};
}