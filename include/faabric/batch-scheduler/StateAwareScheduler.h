#pragma once

#include <faabric/batch-scheduler/Application.h>
#include <faabric/batch-scheduler/BatchScheduler.h>
#include <faabric/batch-scheduler/RuntimeSummary.h>
#include <faabric/planner/ApplicationMetrics.h>
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

    const std::vector<std::string>& getInputNodeNames() const
    {
        static const std::vector<std::string> empty;
        return application ? application->getInputNodes() : empty;
    }

    void initApp(const HostMap& hostMap);

    // std::string scheduleStatelessMessageRBHost(std::string& userFunc,
    //                                        const HostMap& hostMap,
    //                                        const std::unique_ptr<Message>&
    //                                        msg);

    std::string scheduleStatelessMessageApportion(
      std::string& userFunc,
      const HostMap& hostMap,
      const std::unique_ptr<Message>& msg);

    std::string scheduleStatelessMessageRoundRobin(
      std::string& userFunc,
      const HostMap& hostMap,
      const std::unique_ptr<Message>& msg);

    std::string scheduleStatelessMessageLocal(
      std::string& userFunc,
      const HostMap& hostMap,
      const std::unique_ptr<Message>& msg);

    std::string scheduleStatefulMessage(std::string& userFunc,
                                        const std::unique_ptr<Message>& msg);

    std::string scheduleMessage(const HostMap& hostMap,
                                const faabric::Message& msg);

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

    void scheduleApp(const HostMap& hostMap);

    void rescheduleApp(
      const HostMap& hostMap,
      const faabric::planner::ApplicationMetrics* metrics = nullptr);

    std::tuple<std::vector<NodeGroup>,
               std::vector<std::map<std::string, double>>,
               std::map<std::string, double>>
    groupNodesGreedily(const HostMap& hostMap);

    std::tuple<std::vector<NodeGroup>,
               std::vector<std::map<std::string, double>>,
               std::map<std::string, double>>
    groupNodesTopo(const HostMap& hostMap);

    void rescheduleAppBinpack(const HostMap& hostMap);

    void rescheduleAppFaaSFlow(const HostMap& hostMap);

    void rescheduleAppFaaSFlowAdaptive(
      const HostMap& hostMap,
      const faabric::planner::ApplicationMetrics* metrics);

    void rescheduleAppStepConf(const HostMap& hostMap);

    // Predict the fraction of chained calls that remain host-local when the
    // application is binpacked onto `numHosts` workers. Read-only: it mirrors
    // quantiseResources() + groupNodesTopo() (the deterministic Binpack split)
    // without mutating any application or scheduler state, then estimates the
    // local share as Σ_edge weight·(Σ_w A_w·B_w) / Σ_edge weight, where A_w/B_w
    // are the per-worker placement fractions of the edge's two operators.
    // Returns a value in [0, 1], or -1.0 if prediction is not possible (no
    // application / no chained edges).
    double predictBinpackLocalShare(int numHosts) const;

    // Predict the per-worker CPU load (in us/s) when the application is
    // binpacked onto `numHosts` workers, given the current cost coefficients.
    // Unlike predictBinpackLocalShare (which collapses the layout to a single
    // local-share scalar), this attributes process and chained-call cost to
    // each worker individually, so the caller can detect per-worker overload —
    // the load imbalance the aggregate capacity model misses. Per worker:
    //   time_w = procRate_w·tE
    //          + localCalls_w·alpha + remoteCalls_w·beta
    // where procRate_w is the operators' processing rate landing on w (their
    // totalLoad share times their placement fraction), and chained calls are
    // attributed to the source operator's worker, local with probability B_w
    // (destination on the same worker) and remote otherwise. Returns a vector
    // of length numHosts, or empty if prediction is not possible.
    std::vector<double> predictBinpackWorkerLoads(int numHosts,
                                                  double totalLoad,
                                                  double tE,
                                                  double alpha,
                                                  double beta,
                                                  double chainedRatio) const;

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
      const std::map<std::string, std::map<std::string, double>>&
        newStatelessReqWeight,
      const std::map<std::string, std::map<int, double>>& newParStateReqWeight)
      const;

    void runtimeDistTune(
      const std::map<std::string, std::map<std::string, int>>& observeDistMap);

    void printScheduleInfomation() const;

    void setRuntimeReconfig(bool value);

    bool getRuntimeReconfig() const;

    void setAlpha(double value);

  protected:
    // scheduler lock
    std::shared_mutex scheduleMx;

    bool isplanner = true;
    int scheduleMode = 0;

    // hostAssign Counter is used when assign states to the hosts.
    std::atomic<unsigned int> stateRbCounter{ 0 };

    int weightFactor = 1000;

    RuntimeSummary runtimeSummary;

    std::atomic<bool> runtimeReconfig = true;

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

    // Deterministic Binpack placement of every operator onto `numHosts`
    // workers, as normalised per-worker fractions (placement[op][w] in [0,1],
    // Σ_w placement[op][w] = 1). Mirrors quantiseResources() + the contiguous
    // tape layout without mutating any state. Shared by predictBinpackLocalShare
    // and predictBinpackWorkerLoads. Empty if no application / quantisation
    // fails.
    std::map<std::string, std::map<int, double>> computeBinpackPlacement(
      int numHosts) const;

    // Window (in seconds) over which computeChainedCostCoeff samples metrics.
    static constexpr int kChainedCostWindowSec = 10;

    // Estimated CPU cost of one chained call, expressed in units of processed
    // tuples (avg per-call cost / t_e), derived from the cluster-average
    // local/remote chained mix: alpha·localShare + beta·(1-localShare). Used to
    // weight chained calls in the Binpack workload so remote-heavy operators
    // get more workers. Returns 0.0 when the estimator is not yet warmed up
    // (degrading Binpack to the original process-only weighting).
    static double computeChainedCostCoeff(
      const faabric::planner::ApplicationMetrics::ScalingSignals& signals);

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