#pragma once

#include <faabric/batch-scheduler/Application.h>
#include <faabric/util/config.h>
#include <faabric/util/locks.h>
#include <faabric/util/string_tools.h>

#include <algorithm>
#include <cmath>
#include <map>
#include <random>
#include <shared_mutex>
#include <sstream>
#include <string>
#include <vector>

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
                      std::map<std::string, int> weightDistIn,
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
    std::map<std::string, int> weightDist;
    // Idx -> IP distribution. (only for partitioned stateful operators)
    std::map<int, std::string> parallelismDist;
    enum LocalStatelessOperatorType localType = UNKNOWN;
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

class RuntimeSummary
{
  public:
    RuntimeSummary() = default;

    void initScheduledOperators(
      const batch_scheduler::Application& application,
      const std::map<std::string, ScheduledOperator>& scheduledOperatorsMapIn,
      bool planner = false);

    void updateSourceDist(
      std::map<std::string, std::map<std::string, int>> sourceCountStats);

    // The 'counter' and 'recommended' arguments are no longer needed for
    // probabilistic scheduling but are kept for API compatibility.
    std::string getHost(const std::string& instance, unsigned int counter);
    std::string getHost(const std::string& instance,
                        const std::string& recommended);

  private:
    std::shared_mutex summaryMx;
    std::string localHost = faabric::util::getSystemConfig().endpointHost;
    bool isPlanner = false;

    std::map<std::string, std::map<std::string, double>> expectedDist;
    std::map<std::string, std::map<std::string, double>> sourceDist;

    std::map<std::string, ScheduledOperator> scheduledOperatorsMap;

    // CHANGED: Replaced WindowedRecord with the new ProbabilisticScheduler
    std::map<std::string, std::shared_ptr<ProbabilisticScheduler>>
      probabilisticSchedulers;

    // ... (other private members and methods are the same) ...
    void initAll(const batch_scheduler::Application& application, bool planner);

    void initOpertaor(const batch_scheduler::Application& application,
                      std::string operatorName);

    void doInitExpectedDist(ScheduledOperator& schedOp);
    void doInitSourceDist(ScheduledOperator& schedOp,
                          const batch_scheduler::Application& application);
};
}