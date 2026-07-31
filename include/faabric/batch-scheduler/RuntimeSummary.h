#pragma once

#include <faabric/batch-scheduler/Application.h>
#include <faabric/util/config.h>
#include <faabric/util/locks.h>
#include <faabric/util/map.h>
#include <faabric/util/string_tools.h>

#include <algorithm>
#include <cmath>
#include <map>
#include <random>
#include <shared_mutex>
#include <sstream>
#include <string>
#include <vector>
#include <atomic>

namespace faabric::batch_scheduler {

enum LocalStatelessOperatorType
{
    UNKNOWN = 0,
    ROUNDROBIN_HEAD = 1,
    ROUNDROBIN_BODY = 2,
    COLLOCATE_HEAD = 3,
    COLLOCATE_BODY = 4,
};

inline ::faabric::planner::LocalStatelessOperatorType toProto(
  LocalStatelessOperatorType t)
{
    switch (t) {
        case LocalStatelessOperatorType::UNKNOWN:
            return ::faabric::planner::LocalStatelessOperatorType::UNKNOWN;
        case LocalStatelessOperatorType::ROUNDROBIN_HEAD:
            return ::faabric::planner::LocalStatelessOperatorType::
              ROUNDROBIN_HEAD;
        case LocalStatelessOperatorType::ROUNDROBIN_BODY:
            return ::faabric::planner::LocalStatelessOperatorType::
              ROUNDROBIN_BODY;
        case LocalStatelessOperatorType::COLLOCATE_HEAD:
            return ::faabric::planner::LocalStatelessOperatorType::
              COLLOCATE_HEAD;
        case LocalStatelessOperatorType::COLLOCATE_BODY:
            return ::faabric::planner::LocalStatelessOperatorType::
              COLLOCATE_BODY;
    }
    // Fallback for any future/new values
    return ::faabric::planner::LocalStatelessOperatorType::UNKNOWN;
}

inline LocalStatelessOperatorType fromProto(
  ::faabric::planner::LocalStatelessOperatorType t)
{
    switch (t) {
        case ::faabric::planner::LocalStatelessOperatorType::UNKNOWN:
            return LocalStatelessOperatorType::UNKNOWN;
        case ::faabric::planner::LocalStatelessOperatorType::ROUNDROBIN_HEAD:
            return LocalStatelessOperatorType::ROUNDROBIN_HEAD;
        case ::faabric::planner::LocalStatelessOperatorType::ROUNDROBIN_BODY:
            return LocalStatelessOperatorType::ROUNDROBIN_BODY;
        case ::faabric::planner::LocalStatelessOperatorType::COLLOCATE_HEAD:
            return LocalStatelessOperatorType::COLLOCATE_HEAD;
        case ::faabric::planner::LocalStatelessOperatorType::COLLOCATE_BODY:
            return LocalStatelessOperatorType::COLLOCATE_BODY;
        // If Protobuf ever adds values you don’t recognize, fall back:
        default:
            return LocalStatelessOperatorType::UNKNOWN;
    }
}

struct ScheduledOperator
{
    ScheduledOperator(Node nodeIn,
                      int groupIdIn,
                      bool isCollocateIn,
                      std::string collocateWithIn,
                      int parallelismIn,
                      std::map<std::string, double> weightDistIn,
                      std::map<int, std::string> parallelismDistIn,
                      LocalStatelessOperatorType localTypeIn)
      : node(std::move(nodeIn))
      , groupId(groupIdIn)
      , isCollocate(isCollocateIn)
      , collocateWith(std::move(collocateWithIn))
      , parallelism(parallelismIn)
      , weightDist(std::move(weightDistIn))
      , parallelismDist(std::move(parallelismDistIn))
      , localType(localTypeIn)
    {}

    Node node;
    // std::string userFunction;
    // NodeType type;
    // bool isInput;
    int groupId;
    // Only used for stateless operators.
    bool isCollocate;
    std::string collocateWith;
    // This is the parallelism of the operator after rescheduled.
    // The parallelism in node maybe changed by the scheduler.
    int parallelism;
    // IP -> weight distribution.
    std::map<std::string, double> weightDist;
    // Idx -> IP distribution. (only for partitioned stateful operators)
    std::map<int, std::string> parallelismDist;
    enum LocalStatelessOperatorType localType = UNKNOWN;
    // Only used for stateless operators. True if both the direct
    // predecessor and direct successor are partitioned-stateful operators
    // in the same group, i.e. this operator's collocate host is pinned by
    // the partition key on both sides and should never be runtime-tuned.
    bool isSandwichedByPartitioned = false;
};

static const char* localNodeTypeToString(NodeType t)
{
    switch (t) {
        case NodeType::STATELESS:
            return "STATELESS";
        case NodeType::STATEFUL:
            return "STATEFUL";
        case NodeType::PARTITIONED_STATEFUL:
            return "PARTITIONED_STATEFUL";
        default:
            return "UNKNOWN";
    }
}

[[maybe_unused]]
static std::string to_string(const ScheduledOperator& s)
{
    std::ostringstream oss;
    oss << "{ name=" << s.node.name
        << " type=" << localNodeTypeToString(s.node.type)
        << " group=" << s.groupId
        << " collocate=" << (s.isCollocate ? "true" : "false")
        << " collWith=" << s.collocateWith << " par=" << s.parallelism
        << " wDist={";
    for (auto const& [h, w] : s.weightDist) {
        oss << h << ":" << w << ",";
    }
    oss << "} pDist={";
    for (auto const& [i, h] : s.parallelismDist) {
        oss << i << "->" << h << ",";
    }
    oss << "}}";
    return oss.str();
}

[[maybe_unused]]
static void printScheduledOperatorsMap(
  const std::map<std::string, ScheduledOperator>& scheduledOperatorsMap)
{
    std::ostringstream ss;

    for (const auto& [key, sop] : scheduledOperatorsMap) {
        ss << key << " => " << to_string(sop) << "\n";
    }

    SPDLOG_INFO("ScheduledOperatorsMap:\n{}", ss.str());
}

inline ScheduledOperator& getScheduledOperatorOrThrow(
  std::map<std::string, ScheduledOperator>& ops,
  const std::string& nodeName)
{
    auto it = ops.find(nodeName);
    if (it == ops.end()) {
        SPDLOG_ERROR("Source node {} not found in scheduled operators map",
                     nodeName);
        throw std::runtime_error(
          "Source node not found in scheduled operators map");
    }
    return it->second;
}

// The number of slots in the ring.
// const std::size_t RING_SIZE = 100;

// NEW: A stateless scheduler that uses probability. Replaces WindowedRecord.
class ProbabilisticScheduler
{
  public:
    // Constructor pre-computes the Cumulative Distribution Function (CDF)
    explicit ProbabilisticScheduler(
      const std::map<std::string, double>& weights);

    // Returns a host based on the weighted probability.
    // This method is const and lock-free, making it extremely fast.
    const std::string& schedule() const;

  private:
    // This vector stores the pre-computed CDF.
    // e.g., {{0.6, "host_a"}, {0.9, "host_b"}, {1.0, "host_c"}}
    std::vector<std::pair<double, std::string>> cdf;
};

// MAP<IP address: ProbabilisticScheduler>
using MetaScheduler =
  std::map<std::string, std::shared_ptr<ProbabilisticScheduler>>;

class RuntimeSummary
{
  public:
    RuntimeSummary() = default;

    void initScheduledOperators(
      const batch_scheduler::Application& application,
      const std::map<std::string, ScheduledOperator>& scheduledOperatorsMapIn,
      bool planner,
      int scheduleMode);

    void requestDistTune(
      const std::map<std::string, std::map<std::string, int>>& observedDistMap);

    // The 'counter' and 'recommended' arguments are no longer needed for
    // probabilistic scheduling but are kept for API compatibility.
    std::string getHost(const std::string& instance, unsigned int counter);
    std::string getHost(const std::string& instance,
                        const std::string& recommended);
    
    void setAlpha(double value);

    void reset();

  private:
    std::shared_mutex summaryMx;
    std::string localHost = faabric::util::getSystemConfig().endpointHost;
    bool isPlanner = false;
    std::atomic<double> alpha = 0.2;

    // MAP<instance name: operatorType>
    std::map<std::string, LocalStatelessOperatorType> localOperatorsMap;
    // The expected distribution of messages around workers.
    std::map<std::string, std::map<std::string, double>> expectedDistMap;
    // The current distribution probability of messages around workers.
    // Higher means more likely to distribute messages to that worker. It
    // usually means in the past observation, the worker has received messages
    // less than expected.
    std::map<std::string, std::map<std::string, double>> implDistMap;
    std::map<std::string, std::map<std::string, int>> recommendedHostMap;

    std::map<std::string, ScheduledOperator> scheduledOperatorsMap;

    // Schedulers used for scheduling stateless operators.
    std::map<std::string, std::shared_ptr<ProbabilisticScheduler>>
      probabilisticSchedulers;
    std::map<std::string, std::shared_ptr<MetaScheduler>> metaSchedulers;

    void initAll(const batch_scheduler::Application& application,
                 bool planner,
                 int scheduleMode);

    void initOpertaor(const batch_scheduler::Application& application,
                      std::string operatorName);

    void doInitExpectedDist(ScheduledOperator& schedOp);

    MetaScheduler buildMetaScheduler(
      const std::string& instanceName,
      std::map<std::string, double> schedulingWeights);

    void TuneImplDist(const std::string instanceName,
                      const std::map<std::string, int>& observedDist);

    std::map<std::string, double> calculateBodyWeights(
      const std::string instanceName,
      const std::map<std::string, int>& observedDist);

    void CollocateHeadTune(const std::string instanceName,
                           const std::map<std::string, int>& observedDist);
    void CollocateBodyTune(const std::string instanceName,
                           const std::map<std::string, int>& observedDist);
    void RoundRobinBodyTune(const std::string instanceName,
                            const std::map<std::string, int>& observedDist);
};
}