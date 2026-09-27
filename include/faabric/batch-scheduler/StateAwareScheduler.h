#pragma once

#include <faabric/batch-scheduler/Application.h>
#include <faabric/batch-scheduler/BatchScheduler.h>
#include <faabric/batch-scheduler/RuntimeSummary.h>
#include <faabric/planner/ApplicationMetrics.h>
#include <faabric/planner/FunctionMetrics.h>
#include <faabric/util/config.h>
#include <faabric/util/hash.h>
#include <faabric/util/locks.h>

#include <functional>
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

// Values accepted by `scheduleMode`. Historically these were bare integers
// spread across StateAwareScheduler, Scheduler, Planner and Application; the
// names are here so the dispatch in scheduleApp/rescheduleApp can be read
// without cross-referencing. `scheduleMode` stays an int because it arrives
// from config and over HTTP.
enum ScheduleMode : int
{
    ModeBinpack = 0,          // Capacity Binpack
    ModeFaaSFlowAdaptive = 3,
    ModeNonlinear = 4,
    ModeApportion = 5,
    ModeFaaSFlow = 7,
    ModeStepConf = 11,
    // Operators are not pinned to workers at all. Placement follows state
    // ownership, which is claimed on demand in Redis by whichever scheduler
    // routes the first request for it.
    ModeFlux = 12,
};

struct HashAndParallelismInfo
{
    int messageType;
    size_t hash;
    int parallelismIdx;
    // ModeFlux partitioned stateful only: the shard the key hashes to.
    int shardIdx = -1;
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

    // Cluster-wide view: the stats the planner fetched from every worker in
    // its previous runtime-stats round, keyed by worker IP. Lives here rather
    // than on DecentralizedScheduler because the planner routes requests too
    // and needs the same view. Not cleared by resetScheduler(), as that also
    // runs on every migration.
    void setClusterWorkerStats(
      std::map<std::string, faabric::WorkerStats>&& stats);

    std::map<std::string, faabric::WorkerStats> getClusterWorkerStats();

    /**
     * ModeFlux: what a worker knows about one of its own queues right now.
     * The cluster stats are a round old, and a worker routing its own chained
     * requests can do better for its own queues.
     */
    struct FluxLocalLoad
    {
        // Requests waiting in the queue.
        int queued = 0;
        // How many more requests could start right now: free run slots of the
        // paridx, in requests, or 0 if the worker has no executor slot free.
        int freeRequests = 0;
        // Requests the paridx finished per second over the last rebalancing
        // window, and the executor time one request took; 0 when unmeasured.
        double ratePerSec = 0;
        double slotTimeUs = 0;
        // Time a request sent to another worker loses before it can start
        // there, on top of that worker's queue.
        double remoteCostUs = 0;
    };

    // Workers only: how the scheduler reads its host's own queues.
    void setFluxLocalLoadProvider(
      std::function<FluxLocalLoad(const std::string& queueKey)> provider)
    {
        fluxLocalLoad = std::move(provider);
    }

    /**
     * ModeFlux: how many units of `userFunc`'s state each host holds, as far
     * as this scheduler knows (the routing cache).
     */
    std::map<std::string, int> fluxUnitsPerHost(const std::string& userFunc);

    // Shard count of every partitioned stateful operator under ModeFlux. A
    // partitioned operator has a single paridx (0); its keys are hashed onto
    // this many shards, and each shard is placed and moved on its own. Must
    // be set before the application is registered and left alone afterwards:
    // every router hashes keys onto this many shards, so changing it mid-run
    // would send the same key to different shards on different routers.
    void setFluxPartitionShards(int value) { fluxPartitionShards = value; }

    int getFluxPartitionShards() const { return fluxPartitionShards; }

    /**
     * ModeFlux names every unit of state that is owned, locked and moved as
     * a whole: a stateful operator's paridx ("user_func_par"), or one shard
     * of a partitioned operator's paridx ("user_func_par#shard"). A negative
     * shard means the whole paridx.
     */
    static std::string fluxUnitKey(const std::string& userFuncPar, int shardId);

    // The unit a routed message addresses: its shard for a partitioned
    // message (messageType 2), its paridx otherwise.
    static std::string fluxUnitKey(const faabric::Message& msg);

    // Inverse of fluxUnitKey: {userFuncPar, shardId}, shardId -1 for a whole
    // paridx.
    static std::pair<std::string, int> splitFluxUnitKey(
      const std::string& unitKey);

    // Where Redis keeps the owner of a unit of state, and that owner's epoch.
    // The owner key of a whole paridx is also the one the state registry
    // reads (see FunctionStateRegistry::getMasterIP).
    static std::string fluxOwnerKey(const std::string& unitKey);

    static std::string fluxEpochKey(const std::string& unitKey);

    // Whether ModeFlux treats `node` as partitioned: keyed state, one paridx
    // split into shards.
    static bool fluxIsPartitioned(const Node& node);

    // The shard `key` of partitioned operator `userFunc` hashes to -- the
    // same hash routing uses, so a state key and the requests for it always
    // agree on their shard.
    int fluxShardOfKey(const std::string& userFunc, const std::string& key);

    /**
     * ModeFlux: record that unit `unitKey` is now owned by `host`, as of
     * ownership epoch `epoch`. Ignored unless `epoch` is newer than the one
     * already known, so stale and duplicate reports are harmless. Returns
     * whether the record changed anything.
     */
    bool updateStateHostFlux(const std::string& unitKey,
                             const std::string& host,
                             int64_t epoch);

    /**
     * ModeFlux: current owner of unit `unitKey`, as far as this scheduler
     * knows. For a request that already carries its paridx and shard, which
     * must not be re-hashed or re-shuffled onto another one.
     */
    std::string unitOwnerFlux(const std::string& unitKey,
                              const HostMap& hostMap)
    {
        return resolveStateHost(unitKey, hostMap);
    }

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

    /**
     * ModeFlux stateless routing, by expected wait: the request goes where it
     * can start soonest.
     *
     * A worker keeps its own chained requests unless another worker would
     * start one sooner even after paying for the hop -- which keeps an
     * application's operators together on the worker already running them,
     * and moves only what that worker cannot keep up with.
     *
     * The planner has no local. Among the hosts whose expected wait is within
     * one request's service time of the best, it picks the one most tied to
     * the operator (see fluxAffinity): workload balance first, operators
     * together second.
     */
    std::string scheduleStatelessMessageFlux(
      std::string& userFunc,
      const HostMap& hostMap,
      const std::unique_ptr<Message>& msg);

    /**
     * ModeFlux: executor time one request of `queueKey` takes, averaged over
     * the workers that measured it in their last window; 0 if none did.
     */
    static double clusterSlotTimeUs(
      const std::map<std::string, faabric::WorkerStats>& stats,
      const std::string& queueKey);

    /**
     * ModeFlux: expected wait (us) before one more request of `queueKey` could
     * start on `ip` -- its queue as last reported plus whatever was routed
     * there since, drained by as many batches as the paridx may run there.
     * Infinite if `ip` did not report.
     */
    double remoteWaitUsFlux(
      const std::map<std::string, faabric::WorkerStats>& stats,
      const std::string& ip,
      const std::string& queueKey,
      double slotTimeUs);

    /**
     * ModeFlux: how tied `ip` is to operator `userFunc` -- the share of the
     * state units of the operator and of its downstream operators that `ip`
     * holds, plus one if `ip` already runs the operator. Only ever used to
     * choose between hosts that are otherwise as good.
     */
    double fluxAffinity(
      const std::string& userFunc,
      const std::string& ip,
      const std::map<std::string, faabric::WorkerStats>& stats);

    /**
     * The registered DAG node for `userFunc`, or nullptr when no application
     * is registered or the operator is not part of it. Public because the
     * receiving side of a message needs the same view: under ModeFlux the
     * DAG is the only description of an operator anyone has.
     */
    std::shared_ptr<Node> lookupNode(const std::string& userFunc);

    std::string scheduleStatefulMessage(const HostMap& hostMap,
                                        std::string& userFunc,
                                        const std::unique_ptr<Message>& msg);

    /**
     * ModeFlux routing. Nothing is registered at application registration
     * time, so the operator's kind comes from the DAG every scheduler
     * received in registerApp rather than from functionParallelism /
     * statePartitionBy, which ModeFlux never populates:
     *   - stateless            -> purely load-driven (queue depths)
     *   - partitioned stateful -> the shard its partition key hashes to,
     *                             whose owner is claimed on demand the first
     *                             time any scheduler routes to it
     *   - stateful             -> the owner of the shard it shuffles onto
     * An operator missing from the DAG is routed by load: that is always
     * safe, whereas inventing a shard for it is not.
     */
    std::string scheduleMessageFlux(const HostMap& hostMap,
                                    std::string& userFunc,
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

    std::shared_mutex clusterWorkerStatsMx;
    std::map<std::string, faabric::WorkerStats> clusterWorkerStats;

    // Workers only; see setFluxLocalLoadProvider.
    std::function<FluxLocalLoad(const std::string& queueKey)> fluxLocalLoad;

    // ModeFlux: requests routed to each host since the last stats round, per
    // queue. The stats cannot show them yet, so without these every decision
    // until the next round would see the same stale picture and send
    // everything to whichever host looked least loaded. Cleared when new
    // stats arrive.
    std::mutex fluxAssignedMx;
    std::map<std::string, std::map<std::string, int>> fluxAssignedSinceStats;
    // Workers only: requests the current routing pass kept here, per queue.
    // The live queue does not show them until the pass is over and they are
    // enqueued. Cleared at the start of every scheduleMessagesBatch.
    std::map<std::string, int> fluxKeptThisPass;

    // See setFluxPartitionShards.
    int fluxPartitionShards = 200;

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
    // ModeFlux: operators whose units have all been placed. Guards the
    // per-message fast path from re-walking the whole unit set.
    std::set<std::string> fluxInitialisedOps;
    // ModeFlux: unit key (see fluxUnitKey) : ownership epoch of the owner
    // held in stateHost. Absent means epoch 0, i.e. the initial placement.
    // Guarded by scheduleMx, like stateHost, which ModeFlux also keys by unit.
    std::map<std::string, int64_t> stateHostEpoch;
    // ModeFlux: Function_User : shard count its cached hash ring was built
    // over.
    std::map<std::string, int> fluxRingShards;
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

    /**
     * ModeFlux variant: the partition key and the shard count are read off
     * the DAG node instead of statePartitionBy / functionParallelism.
     */
    HashAndParallelismInfo getHashAndParallelismIndexFlux(
      const std::string& userFunction,
      const Node& node,
      const faabric::Message& msg);

    /**
     * ModeFlux: how many paridx `node` has. A partitioned operator has one:
     * its keys are spread by shard (fluxPartitionShards) within that paridx,
     * not across paridx. A non-partitioned stateful operator's instances hold
     * genuinely separate states and the shuffle picks between them with
     * nothing mapping a key to one, so its declared parallelism is honoured.
     */
    int fluxParallelism(const Node& node) const;

    /**
     * ModeFlux: place every unit of `node` the first time any of them is
     * routed to, rather than one unit at a time, and place them all on one
     * worker. The units are the shards of paridx 0 for a partitioned
     * operator, and its paridx for a non-partitioned one.
     *
     * ModeFlux does no static placement, so the initial layout only has to be
     * deterministic, not good: the runtime spreads units out from there as
     * load appears. Starting every unit co-located is the honest expression
     * of that -- it commits to nothing, and an operator that never gets hot
     * never costs more than one worker. The whole set is placed at once
     * because a half-placed operator would otherwise claim its remaining
     * units one at a time, whenever the input happened to first hash to
     * them, making the layout depend on arrival order. A no-op once the
     * operator has been placed.
     */
    void initOperatorStateFlux(const std::string& userFunc,
                               const Node& node,
                               const HostMap& hostMap);

    /**
     * Hash ring over `shards` shards of `userFunction`, built on first use
     * and cached. Every shard carries equal weight: ModeFlux pins no operator
     * to a worker, so there are no per-worker weights to skew the ring with.
     * Rebuilt if the shard count ever differs from the cached one.
     */
    std::shared_ptr<faabric::util::ConsistentHashRing> getFluxHashRing(
      const std::string& userFunction,
      int shards);

    /**
     * ModeFlux: records in the routing cache an owner read from Redis
     * together with its epoch. Fills a missing entry, and replaces an
     * existing one only if this epoch is newer. Caller holds scheduleMx.
     */
    void recordOwnerFromRedisFlux(const std::string& unitKey,
                                  const std::string& owner,
                                  int64_t epoch);

    /**
     * Returns the host owning `userFuncPar`, claiming it for a freshly picked
     * candidate if nobody owns it yet (ModeFlux). Redis arbitrates, so
     * concurrent callers on different schedulers agree on a single owner and
     * the losers still get the winner's host back.
     *
     * Under the pinning modes the owner is always already in `stateHost`, so
     * this never reaches Redis.
     */
    std::string resolveStateHost(const std::string& userFuncPar,
                                 const HostMap& hostMap);

    /**
     * Deterministic first guess at who should own `userFuncPar`. Every
     * scheduler derives the same candidate from the key, so the claim in
     * resolveStateHost almost always succeeds on the first attempt instead of
     * a herd of schedulers fighting over one key.
     */
    static std::string pickCandidateHost(const std::string& userFuncPar,
                                         const HostMap& hostMap);

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