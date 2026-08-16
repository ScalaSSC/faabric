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

    // CLAST-placement hybrid (schedule modes 3 and 4). When enabled, the base
    // mode is used ONLY as a sizing model — how many workers its own model says
    // the application needs — and the operators are then laid out on that many
    // workers by CLAST's capacity-aware packer (the mode-0 placement). See
    // rescheduleAppClastPlacement.
    void setClastPlacement(bool value) { isClastPlacement = value; }

    bool getClastPlacement() const { return isClastPlacement; }

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

    // Open-ended, capacity-aware Binpack placement (schedule mode 0).
    // Unlike groupNodesTopo (which fills a fixed tape of `numHosts` unit-capacity
    // slots ranked only by processing share), this places each operator under
    // the *physical* worker capacity model
    //   L_k = Σ procRate·t_e + alpha·local + beta·remote + gamma·#remoteHosts ≤ W·headroom
    // It opens workers on demand (so the worker count N is an OUTPUT, not a
    // prediction), greedily co-locates chained-call neighbours to convert remote
    // calls (beta) into local ones (alpha) while limiting fan-out (gamma), then
    // runs an exact local-refinement pass that recomputes L_k after every move to
    // close the placement→local/remote→load feedback loop. Returns the same
    // {groups, groupAllocations, workerRemaining} shape as groupNodesTopo so the
    // rest of rescheduleAppBinpack is unchanged.
    std::tuple<std::vector<NodeGroup>,
               std::vector<std::map<std::string, double>>,
               std::map<std::string, double>>
    groupNodesCapacity(
      const HostMap& hostMap,
      const faabric::planner::ApplicationMetrics::ScalingSignals& signals);

    // signals != nullptr (and warmed up) selects the capacity-aware packer
    // (groupNodesCapacity); otherwise it falls back to the legacy groupNodesTopo
    // tape layout (used on the very first schedule, before metrics exist).
    // `metrics` is only used for the mode-0 executor budget (per-operator exec
    // latency); the placement itself needs nothing beyond `signals`.
    void rescheduleAppBinpack(
      const HostMap& hostMap,
      const faabric::planner::ApplicationMetrics::ScalingSignals* signals =
        nullptr,
      const faabric::planner::ApplicationMetrics* metrics = nullptr);

    // Per-worker safety margin applied to the capacity budget (budget =
    // W·headroom) in groupNodesCapacity. Mirrors the planner's
    // workerLoadHeadroom; kept in sync via resetParameter("worker_load_headroom").
    void setCapacityHeadroom(double value) { capacityHeadroom = value; }

    // Absolute tolerance (req/s) of the saturation binary search in
    // groupNodesCapacity. Mirrors the planner's capacitySearchTol; kept in
    // sync via resetParameter("capacity_search_tol").
    void setCapacitySearchTol(double value) { capacitySearchTolRate = value; }

    // Per-round growth ceiling of the Binpack executor budget, as a multiple
    // of the executors currently running. Kept in sync via
    // resetParameter("exec_budget_growth_cap").
    void setExecBudgetGrowthCap(double value) { execBudgetGrowthCap = value; }

    // Reschedule with the non-linear performance model + soft-affinity
    // placement (schedule mode 4), after Zhang et al., HPCC'24. Per-operator
    // parallelism comes from the fitted scheduling-overhead curve
    // r(p) = b·(1-e^{-kp}) (min p with p·c·(1-r(p))·headroom ≥ rate); the
    // instances are then placed host-by-host with the greedy
    // Score = ws·Data + wb·Balance policy (paper's Algorithm 1).
    void rescheduleAppNonlinear(
      const HostMap& hostMap,
      const faabric::planner::ApplicationMetrics* metrics);

    // Weights of the soft-affinity scheduling score (mode 4):
    // Score_N = ws·Data_N + wb·Balance_N. Kept in sync with the planner via
    // resetParameter("soft_affinity_ws" / "soft_affinity_wb").
    void setSoftAffinityWeights(double ws, double wb)
    {
        softAffinityWs = ws;
        softAffinityWb = wb;
    }

    // Minimum observed samples before an r(p) curve is trusted (mode 4).
    // Synced via resetParameter("nl_min_fit_samples").
    void setNlMinFitSamples(int n) { nlMinFitSamples = n < 2 ? 2 : n; }

    void setMaxExecutorsPerWorker(int n)
    {
        maxExecutorsPerWorker = n < 0 ? 0 : n;
    }

    void rescheduleAppFaaSFlow(const HostMap& hostMap);

    void rescheduleAppFaaSFlowAdaptive(
      const HostMap& hostMap,
      const faabric::planner::ApplicationMetrics* metrics);

    // CLAST-placement hybrid for schedule modes 3 and 4 (isClastPlacement).
    // Step 1 asks the base mode for the ONLY thing we keep from it — the number
    // of workers it thinks the application needs:
    //   mode 3: ApplicationMetrics::computeAdaptiveHostCount(rate, |hosts|)
    //   mode 4: ceil(Σ n*_o / per-worker executor capacity) from the non-linear
    //           model (the same sizing rescheduleAppNonlinear runs on).
    // Step 2 hands the first N workers to the mode-0 capacity packer
    // (rescheduleAppBinpack -> groupNodesCapacity). That packer is open-ended
    // (it takes a rate, not a worker count) but bounded by the host set it is
    // given, so restricting it to N workers makes its existing saturation
    // binary search find the largest input rate whose placement fits in N —
    // i.e. "given N workers, find the admissible rate, then place at it".
    void rescheduleAppClastPlacement(
      const HostMap& hostMap,
      const faabric::planner::ApplicationMetrics* metrics,
      const faabric::planner::ApplicationMetrics::ScalingSignals* signals,
      double chainedCostCoeff);

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
    // (destination on the same worker) and remote otherwise. On top of that a
    // fixed gamma cost is added once per distinct remote worker that w fans out
    // to (gamma·|distinct remote dest hosts of w|), independent of call volume.
    // Returns a vector of length numHosts, or empty if prediction is not
    // possible.
    std::vector<double> predictBinpackWorkerLoads(int numHosts,
                                                  double totalLoad,
                                                  double tE,
                                                  double alpha,
                                                  double beta,
                                                  double gamma,
                                                  double chainedRatio) const;

    // Predict the total instance demand Σ n*_o at a hypothetical `inputRate`
    // under the mode-4 non-linear model: per operator the smallest p with
    // p·c·(1-r(p))·headroom ≥ rate_o, where c = nlCpuBudgetPerExecutorUs/t_e
    // (one executor = one CPU), using the fitted r(p) curves and the
    // per-operator exec times remembered from the last reschedule
    // (nlLastSnap). The demand is NOT capped by the cluster's capacity —
    // when it exceeds what the workers can provide, placement overcommits.
    // STRICTLY READ-ONLY — mutates no scheduler or application state; serves
    // the planner's predict_host_num query. Returns -1 when prediction is
    // not possible (no application / no input rate).
    long predictNonlinearInstanceTotal(
      double inputRate,
      const faabric::planner::ApplicationMetrics::ScalingSignals& signals)
      const;

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

    // Take only the worker-count estimate from schedule mode 3 / 4 and place
    // the operators with the mode-0 (CLAST) capacity packer. Kept in sync with
    // the planner via resetParameter("clast_placement"). Ignored by every other
    // schedule mode.
    bool isClastPlacement = false;

    // Per-worker capacity safety margin for groupNodesCapacity. Default matches
    // the planner's workerLoadHeadroom default.
    double capacityHeadroom = 0.9;

    // Tolerance (req/s) at which the saturation binary search in
    // groupNodesCapacity stops. Default matches the planner's
    // capacitySearchTol default.
    double capacitySearchTolRate = 100.0;

    // Cached result of the saturation binary search in groupNodesCapacity
    // (mode 0). The max-sustainable-rate placement depends only on the
    // capacity model (coefficients, DAG shares, host set) and NOT on the
    // requested rate, so under sustained overload it can be reused across
    // reschedules instead of redoing the ~10 runBackward probes — as long as
    // the model inputs stay within tolerance (checked at reuse time).
    struct CapacitySaturationCache
    {
        bool valid = false;
        // Input signature at cache time.
        std::vector<std::string> hostIps;
        double W = 0.0;
        double tE = 0.0;
        double alpha = 0.0;
        double beta = 0.0;
        double gamma = 0.0;
        double chainedRatio = 0.0;
        std::map<std::string, double> procShare; // op -> tuple share
        std::map<std::string, double> edgeShare; // "a->b" -> weight share
        // Cached result.
        std::map<std::string, std::map<std::string, double>> placeAbs;
        std::vector<std::string> opened;
        double sustainedAbsRate = 0.0; // bestScale · avgInputRate (req/s)
    };
    CapacitySaturationCache capSatCache;

    // ---- Schedule mode 4: non-linear model + soft affinity -----------------
    // Soft-affinity score weights (paper eq. 7).
    double softAffinityWs = 0.5;
    double softAffinityWb = 0.5;
    // Minimum samples before a fitted r(p) curve is used.
    int nlMinFitSamples = 3;
    // Paper assumption: one executor is pinned to one CPU, so each instance's
    // CPU budget is a full core (10^6 us of CPU time per second) and a
    // worker's budget scales with the executors allocated to it.
    static constexpr double nlCpuBudgetPerExecutorUs = 1e6;
    // Upper clamp on the (fitted or predicted) overhead ratio r(p).
    static constexpr double nlMaxOverheadRatio = 0.95;
    // Cold-start prior for r(p) = b·(1 - e^{-k·p}) (the paper's
    // r = a·e^{-kp} + b with a = -b). Used by nlPredictR until a real fit
    // becomes valid, so early reschedules/predictions do not run with zero
    // overhead.
    double nlPriorK = 0.155;
    double nlPriorB = 0.335;
    // Per-worker executor capacity (= CPU cores), configured through the
    // planner's max_executors parameter. 0 = not configured; fall back to
    // the host's reported slots.
    int maxExecutorsPerWorker = 0;

    // Executor capacity of one worker under the mode-4 model.
    int nlHostCapacity(int slots) const
    {
        if (maxExecutorsPerWorker > 0) {
            return maxExecutorsPerWorker;
        }
        return slots < 1 ? 1 : slots;
    }

    // Fitted scheduling-overhead curve r(p) = b·(1 - e^{-k·p}). This is the
    // paper's r = a·e^{-kp} + b with the (0,0) pre-fit point folded in as the
    // exact constraint a = -b, which guarantees r(0) = 0 and r monotonically
    // increasing towards the asymptote b.
    struct NlFit
    {
        double k = 0.0;
        double b = 0.0;
        bool valid = false;
    };

    // Per-operator overhead samples (p = instance count during the interval,
    // r = ts/(ts+te)) and their fitted curves; the global fit pools all
    // operators' samples as the cold-start fallback.
    std::map<std::string, std::vector<std::pair<double, double>>> nlSamples;
    std::map<std::string, NlFit> nlFits;
    NlFit nlGlobalFit;
    // Per-instance lifecycle averages at the last reschedule, and the
    // parallelism each operator ran with since then (the p a new delta
    // sample is tagged with).
    std::map<std::string, faabric::planner::InstanceMetrics::Snapshot>
      nlLastSnap;
    std::map<std::string, int> nlLastParallelism;

    // Result of the mode-4 sizing model: the per-operator instance demand and
    // what the cluster can actually provide.
    struct NlSizing
    {
        std::map<std::string, int> target; // op -> n*_o
        long plannedInstances = 0;         // Σ target
        long totalCapacity = 0;            // Σ per-worker executor capacity
    };

    // Steps 1-4 of the mode-4 reschedule: ingest the fresh overhead samples,
    // refit r(p), and solve each operator's smallest p with
    // p·c·(1-r(p))·headroom ≥ rate_o. Split out of rescheduleAppNonlinear so
    // the CLAST-placement hybrid can reuse the sizing without the
    // soft-affinity placement. NOT read-only: it advances the r(p) fit state
    // (nlCollectSamples), so it must run exactly once per reschedule.
    NlSizing nlComputeSizing(
      const HostMap& hostMap,
      const faabric::planner::ApplicationMetrics* metrics);

    // Ingest fresh lifecycle snapshots: derive each operator's interval
    // overhead ratio via count-weighted snapshot deltas, append samples and
    // refit the curves.
    void nlCollectSamples(
      const std::map<std::string, faabric::planner::InstanceMetrics::Snapshot>&
        snaps);

    static NlFit nlFitCurve(const std::vector<std::pair<double, double>>& samples,
                            int minSamples);

    // Predicted overhead ratio for `op` at parallelism p; falls back to the
    // global curve, then to 0 (pure linear model) when nothing is fitted yet.
    double nlPredictR(const std::string& op, double p) const;

    // Paper's Algorithm 1: place n_o instances per operator (in the given
    // topological order, upstream first) onto hosts by
    // Score = ws·Data + wb·Balance, capped by host slots. Returns
    // op -> host -> instance count.
    std::map<std::string, std::map<std::string, int>> softAffinityPlace(
      const HostMap& hostMap,
      const std::vector<std::pair<std::string, int>>& orderedTargets);

    // Shared commit tail used by rescheduleAppBinpack and
    // rescheduleAppNonlinear.
    void commitGroupAllocations(
      const std::vector<NodeGroup>& groups,
      const std::vector<std::map<std::string, double>>& groupAllocations);

    // Binpack (mode 0) elastic executor budget. Scales the cluster-wide
    // executor count by inputRate/throughput observed over the last window,
    // splits it across operators by their CPU demand (processedTuples x
    // per-operator exec latency) and across workers by each operator's
    // weightDist, and writes the result into
    // scheduledOperatorsMap[...].executorDist. Called at the end of
    // commitGroupAllocations; a no-op when the signals are not usable yet.
    void applyExecutorDist(
      const faabric::planner::ApplicationMetrics::ScalingSignals& sig,
      const faabric::planner::ApplicationMetrics* metrics);

    // Count-weighted average worker execution time per operator (User_Func, in
    // us), aggregated from the per-instance (User_Func_Par) lifecycle
    // snapshots. Empty when no instance has completed a request yet.
    std::map<std::string, double> perOperatorExecTime(
      const faabric::planner::ApplicationMetrics* metrics) const;

    // Ceiling on how much the cluster-wide executor budget may grow in one
    // round, as a multiple of the executors currently running. Without it a
    // momentary throughput dip compounds across consecutive reschedules.
    // Tunable via resetParameter("exec_budget_growth_cap").
    double execBudgetGrowthCap = 2.0;

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