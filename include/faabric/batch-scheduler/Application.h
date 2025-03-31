#pragma once

#include <faabric/util/logging.h>

#include <iostream>
#include <map>
#include <memory>
#include <string>
#include <vector>

namespace faabric::batch_scheduler {
enum NodeType
{
    STATELESS = 0,
    STATEFUL = 1,
    PARTITIONED_STATEFUL = 2
};
std::string nodeTypeToString(NodeType type);
class Node
{
  public:
    std::string name;
    NodeType type;
    int parallelism;
    std::string partitionBy;
    long processedTuples = 0;
    Node(const std::string& nameIn,
         NodeType typeIn,
         int parallelismIn = 1,
         const std::string& partitionByIn = "None");
    // Used for rescheduling. It the records the metrics in the last window.
    double preWorkload = 0;
    double reqResource = 0;
};

using ConnectionInfo = std::map<std::string, std::vector<std::string>>;

class Application
{
  private:
    std::string name;
    std::map<std::string, std::shared_ptr<Node>> nodes;
    std::vector<std::string> inputNodes;
    ConnectionInfo connections;

  public:
    std::string getName() const { return name; }
    Application(const std::string& appName);
    void addNode(std::shared_ptr<Node> node, bool isInput = false);
    void addConnection(const std::string& src, const std::string& dest);
    void displayApplication() const;

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
};
}