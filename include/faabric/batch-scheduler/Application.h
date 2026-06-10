#pragma once

#include <faabric/planner/planner.pb.h>
#include <faabric/util/logging.h>

#include <iostream>
#include <map>
#include <memory>
#include <set>
#include <sstream>
#include <string>
#include <vector>

namespace faabric::batch_scheduler {
enum NodeType
{
    STATELESS = 0,
    STATEFUL = 1,
    PARTITIONED_STATEFUL = 2
};

inline ::faabric::planner::NodeType toProto(NodeType t)
{
    switch (t) {
        case NodeType::STATELESS:
            return ::faabric::planner::NodeType::STATELESS;
        case NodeType::STATEFUL:
            return ::faabric::planner::NodeType::STATEFUL;
        case NodeType::PARTITIONED_STATEFUL:
            return ::faabric::planner::NodeType::PARTITIONED_STATEFUL;
        default:
            SPDLOG_ERROR("Unknown NodeType: {}", static_cast<int>(t));
            throw std::runtime_error("Unknown NodeType");
    }
}

inline NodeType fromProto(::faabric::planner::NodeType t)
{
    switch (t) {
        case ::faabric::planner::NodeType::STATELESS:
            return NodeType::STATELESS;
        case ::faabric::planner::NodeType::STATEFUL:
            return NodeType::STATEFUL;
        case ::faabric::planner::NodeType::PARTITIONED_STATEFUL:
            return NodeType::PARTITIONED_STATEFUL;
        default:
            SPDLOG_ERROR("Unknown proto NodeType: {}", static_cast<int>(t));
            throw std::runtime_error("Unknown proto NodeType");
    }
}

std::string nodeTypeToString(NodeType type);

class Node
{
  public:
    Node(const std::string& nameIn,
         NodeType typeIn,
         bool isInputIn,
         int parallelismIn,
         std::set<std::string> inputFeildsIn = {},
         const std::string& partitionByIn = "None");

    std::string name;
    NodeType type;
    bool isInput; // If this node is an input node
    int parallelism;
    std::set<std::string> inputFeilds;
    std::string partitionBy;
    long processedTuples = 0;
    // Used for rescheduling. It the records the metrics in the last window.
    double preWorkload = 0;
    double reqResource = 0;
};

// ConnectionInfo is a map where the key is the source node name and the value
// are the destinations.
using ConnectionInfo = std::map<std::string, std::vector<std::string>>;

struct Connection
{
    std::string input;
    std::string output;
    int weight;
};
using ConnectionInfoWithWeight =
  std::map<std::string, std::map<std::string, int>>;

class Application
{
  private:
    std::string name;
    std::map<std::string, std::shared_ptr<Node>> nodes;
    std::vector<std::string> inputNodes;
    // Source -> Destination connections
    ConnectionInfo connections;
    ConnectionInfoWithWeight connectionsWithWeight;
    ConnectionInfo reverseConnections;

  public:
    using NodePair = std::pair<std::string, std::shared_ptr<Node>>;
    using NodeList = std::vector<NodePair>;

    std::string getName() const { return name; }
    Application(const std::string& appName);
    void addNode(std::shared_ptr<Node> node, bool isInput = false);
    void addConnection(const std::string& src, const std::string& dest);
    void buildInvertConnections();
    void displayApplication() const;
    // Compute each operator's preWorkload (the proportional weight used to
    // allocate workers). chainedCostCoeff is the estimated CPU cost of one
    // chained call expressed in units of processed tuples (avg per-call cost /
    // t_e); when > 0 in Binpack mode, each operator's outgoing chained calls
    // are added to its workload so remote-heavy operators get more workers.
    // Defaults to 0.0 (process-only, i.e. the original behaviour).
    double computePreWorkloads(int scheduleMode, double chainedCostCoeff = 0.0);
    // TODO - Now we only support homogenous cluster.
    void quantiseResources(const int numHosts,
                           int scheduleMode,
                           double chainedCostCoeff = 0.0);

    // Pure quantisation: distribute `numHosts` workers across operators in
    // proportion to their preWorkload share, rounded to 0.1-worker units (>=
    // 0.1 each, summing to numHosts). Returns operator -> reqResource without
    // touching any Node. Shared by quantiseResources() (which writes the result
    // back to the nodes) and by the Binpack capacity predictor.
    static std::map<std::string, double> quantiseFromPreWorkloads(
      const std::map<std::string, double>& preWorkloads,
      int numHosts);

    std::vector<std::shared_ptr<Node>> getSource(const std::string& node) const;

    void dfsVisit(const std::string& nodeName,
                  std::set<std::string>& visited,
                  NodeList& orderedNodes) const;
    NodeList getNodesDFSOrder();

    std::map<std::string, std::shared_ptr<Node>>& getNodes()
    {
        if (nodes.empty()) {
            SPDLOG_WARN("APP: No nodes in application {}", name);
        }
        return nodes;
    }
    const std::vector<std::string>& getInputNodes() const
    {
        if (inputNodes.empty()) {
            SPDLOG_WARN("APP: No input nodes in application {}", name);
        }
        return inputNodes;
    }

    const ConnectionInfo& getConnections() const { return connections; }

    void updateConnectionsWithWeight(
      const ConnectionInfoWithWeight& newConnectionsWithWeight);

    std::vector<Connection> getConnectionsWithWeight();

    const void showConnections() const
    {
        std::ostringstream oss;
        bool firstSrc = true;

        for (const auto& [src, dests] : connections) {
            if (!firstSrc) {
                oss << "; ";
            }
            firstSrc = false;

            oss << src << "->[";
            bool firstDst = true;
            for (const auto& dst : dests) {
                if (!firstDst) {
                    oss << ", ";
                }
                firstDst = false;
                oss << dst;
            }
            oss << "]";
        }

        SPDLOG_INFO("Connections in application {}: {}", name, oss.str());
    }
};
}