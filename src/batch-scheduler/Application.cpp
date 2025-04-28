#include <faabric/batch-scheduler/Application.h>
#include <faabric/util/logging.h>
#include <sstream>
namespace faabric::batch_scheduler {
std::string nodeTypeToString(NodeType type)
{
    switch (type) {
        case STATELESS:
            return "Stateless";
        case STATEFUL:
            return "Stateful";
        case PARTITIONED_STATEFUL:
            return "Partitioned Stateful";
        default:
            return "Unknown";
    }
}

Node::Node(const std::string& nameIn,
           NodeType typeIn,
           int parallelismIn,
           std::set<std::string> inputFeildsIn,
           const std::string& partitionByIn)
  : name(nameIn)
  , type(typeIn)
  , parallelism(parallelismIn)
  , inputFeilds(std::move(inputFeildsIn))
  , partitionBy(partitionByIn)
{}

Application::Application(const std::string& appName)
  : name(appName)
{}

void Application::addNode(std::shared_ptr<Node> node, bool isInput)
{
    nodes[node->name] = node;
    if (isInput) {
        inputNodes.push_back(node->name);
    }
}

std::vector<std::shared_ptr<Node>> Application::getSource(
  const std::string& node)
{
    std::vector<std::shared_ptr<Node>> sourceNodes;
    for (auto& [name, successors] : connections) {
        for (auto& successor : successors) {
            if (successor == node) {
                sourceNodes.push_back(nodes[name]);
                break;
            }
        }
    }

    return sourceNodes;
}

void Application::addConnection(const std::string& src, const std::string& dest)
{
    connections[src].push_back(dest);
}

void Application::displayApplication() const
{
    SPDLOG_INFO("OUTPUT Application DAG: {}", name);
    std::ostringstream logStream;
    logStream << "Application: " << name << "\n";
    logStream << "Input Nodes:\n";
    for (const auto& node : inputNodes) {
        logStream << "    " << node << "\n";
    }
    logStream << "Nodes and their outgoing connections:\n";
    for (const auto& [nodeName, node] : nodes) {
        logStream << "Node " << nodeName << " : type - "
                  << nodeTypeToString(node->type) << " , parallelism - "
                  << node->parallelism << " , partitionBy - "
                  << node->partitionBy << "\n";
        logStream << "    Input fields: ";
        for (const auto& field : node->inputFeilds) {
            logStream << field << " ";
        }
        // Check for outgoing connections
        auto connIt = connections.find(nodeName);
        if (connIt != connections.end() && !connIt->second.empty()) {
            logStream << "    Connections: ";
            for (const auto& dest : connIt->second) {
                logStream << nodeName << "->" << dest << " ";
            }
            logStream << "\n";
        }
    }
    SPDLOG_INFO("{}", logStream.str());
}
}