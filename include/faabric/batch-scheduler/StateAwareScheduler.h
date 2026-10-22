#pragma once

#include <faabric/batch-scheduler/Application.h>
#include <faabric/batch-scheduler/BatchScheduler.h>
#include <faabric/batch-scheduler/RuntimeSummary.h>
#include <faabric/planner/ApplicationMetrics.h>
#include <faabric/planner/FunctionMetrics.h>
#include <faabric/util/config.h>
#include <faabric/util/hash.h>
#include <faabric/util/locks.h>

#include <deque>
#include <functional>
#include <map>
#include <optional>
#include <set>
#include <shared_mutex>
#include <string>
#include <tuple>
#include <unordered_map>
#include <unordered_set>

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

    // Shared, not copied: routing reads it for every request.
    std::shared_ptr<const std::map<std::string, faabric::WorkerStats>>
    getClusterWorkerStats();

    // ModeFlux: what routing needs from the DAG, worked out once at
    // registerApp.
    struct FluxDag
    {
        // Per operator, its stateless children whose only caller it is.
        // Their requests run where it runs. A stateless operator with
        // several callers is in none's list.
        std::unordered_map<std::string, std::vector<std::string>> localChildren;
        // Every operator in some localChildren list: its requests are set
        // off here by work already counted with its caller's.
        std::unordered_set<std::string> localChildOps;
        // Per operator, every operator it calls or is called by.
        std::unordered_map<std::string, std::vector<std::string>> neighbours;
        // The stateful operators, partitioned or not: their requests can
        // only go to their unit's owner.
        std::unordered_set<std::string> statefulOps;
    };

    // ModeFlux: the DAG as registered. Never null.
    std::shared_ptr<const FluxDag> getFluxDag();

    /**
     * ModeFlux: what a worker has to get through before its next round, in
     * executor time, from what it reported: every request queued there with
     * the stateless requests it will set off there, and the same of what
     * arrives from outside over `horizonUs`. Arrivals of an operator's local
     * children are not counted again: their caller's share covers them.
     */
    static double demandWorkUsFlux(const faabric::WorkerStats& workerStats,
                                   double horizonUs)
    {
        return workerStats.queuedchainworkus() +
               workerStats.arrivalchainworkus() * horizonUs / 1e6;
    }

    // One stats round, and what routing reads off it, worked out once when
    // the round arrives rather than for every request until the next.
    struct FluxClusterSnapshot
    {
        std::map<std::string, faabric::WorkerStats> stats;
        // ModeFlux, per queue: clusterSlotTimeUs over `stats`.
        std::unordered_map<std::string, double> slotTimeUs;
        // ModeFlux, per worker: the operators (user_func) of every queue it
        // reported, i.e. those it has warm executors for.
        std::unordered_map<std::string, std::unordered_set<std::string>>
          operators;
        // ModeFlux, per worker and operator: its requests queued or running
        // there.
        std::unordered_map<std::string, std::unordered_map<std::string, int>>
          presence;
        // ModeFlux, per operator: executor time, per request of it, of the
        // stateless requests it sets off downstream. Those run where it runs
        // -- chained stateless calls stay local -- so this is the work one of
        // its requests brings to a worker besides its own. Stateful
        // operators downstream count nothing: their requests go to their
        // unit's owner. Nor do stateless ones with several callers (see
        // FluxDag::localChildren).
        std::unordered_map<std::string, double> downstreamWorkUs;
        // Time from the stats round before to this one; 0 for the first.
        double roundUs = 0;
        // ModeFlux, per worker and operator, over that round: its requests
        // finished there per second plus those queued, spread over the
        // round -- how much of the operator is active there -- and their
        // executor time, alone and with what they set off downstream.
        std::unordered_map<std::string, std::unordered_map<std::string, double>>
          opRatePerSec;
        std::unordered_map<std::string, std::unordered_map<std::string, double>>
          opWorkUs;
        std::unordered_map<std::string, std::unordered_map<std::string, double>>
          opChainWorkUs;
        // ModeFlux, per worker: what it had to get through over the round
        // (demandWorkUsFlux) and what it could (slots x round).
        std::unordered_map<std::string, double> demandUs;
        std::unordered_map<std::string, double> capacityUs;
        // ModeFlux, per worker and stateful operator: units held there.
        std::unordered_map<std::string, std::unordered_map<std::string, int>>
          heldUnits;
        // ModeFlux: the DAG as registered when the round arrived.
        std::shared_ptr<const FluxDag> dag;

        double slotTimeOf(const std::string& queueKey) const
        {
            auto it = slotTimeUs.find(queueKey);
            return it == slotTimeUs.end() ? 0 : it->second;
        }

        double downstreamOf(const std::string& userFunc) const
        {
            auto it = downstreamWorkUs.find(userFunc);
            return it == downstreamWorkUs.end() ? 0 : it->second;
        }

        bool runs(const std::string& ip, const std::string& userFunc) const
        {
            auto it = operators.find(ip);
            return it != operators.end() && it->second.contains(userFunc);
        }

        static double valueIn(
          const std::unordered_map<std::string,
                                   std::unordered_map<std::string, double>>&
            perWorker,
          const std::string& ip,
          const std::string& userFunc)
        {
            auto it = perWorker.find(ip);
            if (it == perWorker.end()) {
                return 0;
            }
            auto opIt = it->second.find(userFunc);
            return opIt == it->second.end() ? 0 : opIt->second;
        }

        // How many of `userFunc`'s DAG neighbours were active on `ip` in
        // the round: the more, the more of its chained calls stay local
        // there.
        int neighboursOn(const std::string& ip,
                         const std::string& userFunc) const
        {
            if (!dag) {
                return 0;
            }
            auto it = dag->neighbours.find(userFunc);
            if (it == dag->neighbours.end()) {
                return 0;
            }
            int n = 0;
            for (const auto& neighbour : it->second) {
                if (valueIn(opRatePerSec, ip, neighbour) > 0) {
                    n++;
                }
            }
            return n;
        }

        int presenceOf(const std::string& ip, const std::string& userFunc) const
        {
            auto it = presence.find(ip);
            if (it == presence.end()) {
                return 0;
            }
            auto opIt = it->second.find(userFunc);
            return opIt == it->second.end() ? 0 : opIt->second;
        }
    };

    std::shared_ptr<const FluxClusterSnapshot> getClusterSnapshot();

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
        // Executor time one request took over the last rebalancing window; 0
        // when unmeasured.
        double slotTimeUs = 0;
    };

    // Workers only: how the scheduler reads its host's own queues.
    void setFluxLocalLoadProvider(
      std::function<FluxLocalLoad(const std::string& queueKey)> provider)
    {
        fluxLocalLoad = std::move(provider);
    }

    /**
     * ModeFlux: what a worker has to get through before its next
     * rebalancing round that it cannot send elsewhere -- every request
     * queued there, and every stateful request bound to arrive, each with
     * the stateless requests it will set off there -- and what it can, its
     * executor slots over that time. Stateless requests yet to arrive are
     * left out: they can still be routed elsewhere when they do.
     */
    struct FluxLocalWork
    {
        double queuedUs = 0;
        double capacityUs = 0;
    };

    // Workers only: how the scheduler reads its host's work as a whole.
    void setFluxLocalWorkProvider(std::function<FluxLocalWork()> provider)
    {
        fluxLocalWork = std::move(provider);
    }

    /**
     * ModeFlux, planner: from the workers' last stats, advise every worker
     * that is falling behind where to send what it cannot get through before
     * the next round (`horizonUs`) -- see FluxOffloadPlan.
     *
     * Kept deliberately cheap: one pass over the stats and a greedy match.
     * A worker is behind when what it has to get through in the horizon --
     * its queued work (requests x executor time per request) and the work
     * arriving meanwhile -- exceeds what it can run, and has room when it is
     * the other way round. Those behind, most first, are matched with those
     * with room, most first -- a worker already running one of the operators
     * backed up there going first, for its warm executors -- and each worker's
     * room is handed out once, so no two workers are sent to the same room.
     */
    std::map<std::string, faabric::FluxOffloadPlan> planFluxOffload(
      const std::map<std::string, faabric::WorkerStats>& stats,
      double horizonUs);

    /**
     * ModeFlux, planner: add to `plans` the advice to bring a stateful
     * operator spread over several workers back together, step by step, once
     * its load is low enough -- conservatively, performance first.
     *
     * The target is the holder where the operator's DAG neighbours are most
     * active, so the most chained calls stay local, then the one holding
     * most of it. Holders hand theirs over smallest first, as many as the
     * target can take: all of them onto one if the operator is light, onto
     * one fewer worker if only part fits, the rest left for a later round. A
     * holder takes part only if:
     *  - the stats of the whole last kFluxMergeWindowSec are in, and in
     *    every round of it the target would have got through its own work
     *    and what is merged onto it, the stateless requests that sets off
     *    downstream included, so those still run there: the recent peak,
     *    not the current load, has to fit;
     *  - neither it nor the target is short of executors in this round;
     *  - the operator's neighbours are no more active on it than on the
     *    target, so no more chained calls cross workers than before;
     *  - it takes part in no other merge this round.
     * Operators go least spread first. Call after setClusterWorkerStats,
     * with the round's plans.
     */
    void planFluxMerges(std::map<std::string, faabric::FluxOffloadPlan>& plans);

    /**
     * ModeFlux, workers: the planner's latest advice for this worker. The
     * quotas are drawn down as work is sent, by chained-call routing and by
     * rebalancing alike, and start afresh with every new plan.
     */
    void setFluxOffloadQuota(std::map<std::string, double> quotaUs);

    std::map<std::string, double> getFluxOffloadQuota();

    // Draws `us` from the quota for `target`. Fails, drawing nothing, if the
    // quota is short -- unless `allowOver`, which lets it go to zero.
    bool drawFluxOffloadQuota(const std::string& target,
                              double us,
                              bool allowOver = false);

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

    // fluxShardOfKey for many keys of one operator: the ring is looked up
    // once, not once per key.
    std::function<int(const std::string&)> fluxShardOfKeyFn(
      const std::string& userFunc);

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
     * ModeFlux stateless routing. An operator's requests are kept together,
     * and spread over more workers only as far as one cannot get through
     * them. A request is sized with what it sets off: its executor time and
     * that of the stateless requests downstream of it, which run where it
     * runs (FluxClusterSnapshot::downstreamWorkUs).
     *
     * Among places with room, the one where the operator's DAG neighbours
     * run comes first -- so the fewest chained calls cross workers -- then
     * the one where the operator itself runs, then the one with most room.
     *
     * A worker keeps its own chained requests -- the caller runs here, so
     * here is where its neighbour is -- while a run slot is free or it still
     * gets through them, and all else it has, before its next round. Past
     * that, if the planner has advised it where to send work, they go to an
     * advised worker: the one where most of the operator's neighbours run,
     * then the one this operator already goes to this round, then one
     * already running it, then the one with the most quota left.
     *
     * The planner sends each input to the first host with room for it before
     * the next stats round: the one where most of its neighbours run, then
     * one already running it, most of it first, then the one with the most
     * room. If none has room, the least loaded host.
     */
    std::string scheduleStatelessMessageFlux(
      std::string& userFunc,
      const HostMap& hostMap,
      const std::unique_ptr<Message>& msg);

    // The planner's fallback for an operator nobody has measured yet: where
    // an input starts soonest by queue length, warm hosts breaking ties.
    std::string scheduleStatelessByWaitFlux(
      const std::string& userFunc,
      const HostMap& hostMap,
      const FluxClusterSnapshot& snapshot);

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
     * there since (`assignedSinceStats`), drained by as many batches as the
     * paridx may run there. Infinite if `ip` did not report.
     */
    static double remoteWaitUsFlux(
      const std::map<std::string, faabric::WorkerStats>& stats,
      const std::string& ip,
      const std::string& queueKey,
      double slotTimeUs,
      int assignedSinceStats);

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

    // ModeFlux: what routing an operator's requests needs, looked up once
    // per routing pass rather than once per request.
    struct FluxOpRoute
    {
        // nullptr: not in the DAG.
        std::shared_ptr<Node> node;
        // Partitioned stateful only: its shard ring.
        std::shared_ptr<faabric::util::ConsistentHashRing> ring;
        // Non-partitioned stateful only: its shuffle counter.
        std::shared_ptr<std::atomic_uint> counter;
    };
    using FluxOpRoutes = std::unordered_map<std::string, FluxOpRoute>;

    // scheduleMessageFlux with the routes found so far this pass.
    std::string scheduleMessageFlux(const HostMap& hostMap,
                                    std::string& userFunc,
                                    const std::unique_ptr<Message>& msg,
                                    FluxOpRoutes& routes);

    std::string scheduleMessage(const HostMap& hostMap,
                                const faabric::Message& msg);

    virtual std::string scheduleMessage(const HostMap& hostMap,
                                        const std::unique_ptr<Message>& msg);

    // scheduleMessage within a routing pass; see FluxOpRoutes.
    std::string scheduleMessage(const HostMap& hostMap,
                                const std::unique_ptr<Message>& msg,
                                FluxOpRoutes& routes);

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
    // Replaced whole by each stats round, never modified in place.
    std::shared_ptr<const FluxClusterSnapshot> clusterSnapshot =
      std::make_shared<FluxClusterSnapshot>();

    // ModeFlux, set at registerApp, guarded by scheduleMx.
    std::shared_ptr<const FluxDag> fluxDag = std::make_shared<FluxDag>();

    // Fills `snapshot.downstreamWorkUs`: the DAG's local children, weighed
    // with the throughputs and executor times its stats report.
    void computeDownstreamWorkFlux(FluxClusterSnapshot& snapshot);

    // Fills the per-worker, per-operator activity of `snapshot`, and each
    // worker's demand, capacity and units held. After
    // computeDownstreamWorkFlux, whose result it uses.
    void computeOperatorActivityFlux(FluxClusterSnapshot& snapshot);

    // Workers only; see setFluxLocalLoadProvider.
    std::function<FluxLocalLoad(const std::string& queueKey)> fluxLocalLoad;
    // Workers only; see setFluxLocalWorkProvider.
    std::function<FluxLocalWork()> fluxLocalWork;

    // ModeFlux: requests routed to each host since the last stats round, per
    // queue. The stats cannot show them yet, so without these every decision
    // until the next round would see the same stale picture and send
    // everything to whichever host looked least loaded. Cleared when new
    // stats arrive.
    std::mutex fluxAssignedMx;
    std::map<std::string, std::map<std::string, int>> fluxAssignedSinceStats;
    // Workers only: the planner's latest advice, drawn down as work is sent.
    std::mutex fluxOffloadMx;
    std::map<std::string, double> fluxOffloadQuotaUs;
    // Workers only: requests the current routing pass kept here, per queue,
    // and their executor time with what they set off downstream. The live
    // queue does not show them until the pass is over and they are enqueued.
    // With them, this host's work as it stood when the pass began, read once
    // for the pass. All cleared at the start of every scheduleMessagesBatch.
    std::map<std::string, int> fluxKeptThisPass;
    double fluxKeptWorkThisPass = 0;
    std::optional<FluxLocalWork> fluxPassWork;
    // Planner only, guarded by fluxAssignedMx: executor time, with what it
    // sets off downstream, routed to each host since the last stats round;
    // the same per second over the round before, which that round's stats
    // already count among the host's arrivals; and when that round came.
    std::unordered_map<std::string, double> fluxAssignedWorkUs;
    std::unordered_map<std::string, double> fluxPrevAssignedRateUs;
    int64_t fluxLastStatsUs = 0;
    // Planner only, guarded by fluxAssignedMx: before any stats round, the
    // one host each operator's inputs go to.
    std::unordered_map<std::string, std::string> fluxStartupHost;

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
    std::shared_ptr<std::atomic_uint> counterOf(const std::string& userFunc);

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

    // ModeFlux, planner only: the stats rounds of the last
    // kFluxMergeWindowSec, oldest first, for planFluxMerges. The window is
    // the planner's metrics window, as for chained-call costs.
    static constexpr int kFluxMergeWindowSec = kChainedCostWindowSec;
    std::mutex fluxHistoryMx;
    std::deque<std::shared_ptr<const FluxClusterSnapshot>> fluxHistory;

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
      const faabric::Message& msg,
      const FluxOpRoute* route = nullptr);

    // The FluxOpRoute of `userFunc`, from `routes` or looked up and added.
    // Places the operator's state first if it is stateful and nobody has.
    const FluxOpRoute& fluxOpRoute(const std::string& userFunc,
                                   const HostMap& hostMap,
                                   FluxOpRoutes& routes);

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
     * ModeFlux: the worker every unit of stateful operator `userFunc` starts
     * on, fixed by whoever first routes to it. A worker proposes itself: it is
     * running the operator upstream, so state and caller start together. The
     * planner proposes the host with the least queued work. One Redis claim on
     * the operator settles it, so the units are never split between workers
     * routing to the operator at the same time.
     */
    std::string fluxHomeOf(const std::string& userFunc, const HostMap& hostMap);

    static std::string fluxHomeKey(const std::string& userFunc)
    {
        return "flux_home_" + userFunc;
    }

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