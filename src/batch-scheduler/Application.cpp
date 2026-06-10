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
           bool isInputIn,
           int parallelismIn,
           std::set<std::string> inputFeildsIn,
           const std::string& partitionByIn)
  : name(nameIn)
  , type(typeIn)
  , isInput(isInputIn)
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
  const std::string& node) const
{
    std::vector<std::shared_ptr<Node>> sourceNodes;
    for (auto& [name, successors] : connections) {
        if (nodes.contains(name) == false) {
            SPDLOG_ERROR("Node {} not found in application nodes", name);
            throw std::runtime_error("Node not found in application nodes");
            continue;
        }
        for (auto& successor : successors) {
            if (successor == node) {
                sourceNodes.push_back(nodes.at(name));
                break;
            }
        }
    }

    return sourceNodes;
}

void Application::updateConnectionsWithWeight(
  const ConnectionInfoWithWeight& newConnectionsWithWeight)
{
    for (const auto& [source, outputConnections] : newConnectionsWithWeight) {
        for (const auto& [dest, weight] : outputConnections) {
            // Add the connection with weight to the connectionsWithWeight map.
            connectionsWithWeight[source][dest] = weight;
        }
    }
}

/**
 * @brief Transforms a map of connections into a flat vector and sorts it.
 *
 * This function takes a map where each key is a source node and the value
 * is a list of destination nodes with associated weights. It converts this
 * nested structure into a flat vector of Connection objects and then sorts
 * this vector in descending order based on the connection weight.
 *
 * @param connections The input map representing the graph's weighted
 * connections.
 * @return A std::vector<Connection> sorted by weight in descending order.
 */
std::vector<Connection> Application::getConnectionsWithWeight()
{
    std::vector<Connection> result;

    // Iterate over each key-value pair in the input map.
    // 'source' is the pair (e.g., {"FuncA", vector_of_connections}).
    for (const auto& [inputNode, outputConnections] : connectionsWithWeight) {
        for (const auto& [outputNode, weight] : outputConnections) {
            // Create a Connection object and add it to our result vector.
            result.push_back({ inputNode, outputNode, weight });
            SPDLOG_DEBUG("Connection: {} -> {} with weight {}",
                         inputNode,
                         outputNode,
                         weight);
        }
    }

    // Sort the resulting vector in descending order based on the 'weight'
    // member. A lambda function is used to define the custom comparison logic.
    std::sort(result.begin(),
              result.end(),
              [](const Connection& a, const Connection& b) {
                  return a.weight > b.weight;
              });

    return result;
}

void Application::addConnection(const std::string& src, const std::string& dest)
{
    connections[src].push_back(dest);
    connectionsWithWeight[src][dest] = 1;
}

void Application::buildInvertConnections()
{
    reverseConnections.clear();
    for (const auto& [src, dests] : connections) {
        for (const auto& dest : dests) {
            reverseConnections[dest].push_back(src);
        }
    }
    for (auto& [dest, srcs] : reverseConnections) {
        std::sort(srcs.begin(), srcs.end());
        srcs.erase(std::unique(srcs.begin(), srcs.end()), srcs.end());
    }
}

void Application::displayApplication() const
{
    SPDLOG_INFO("OUTPUT Application DAG: {}", name);
    std::ostringstream logStream;
    logStream << "\nApplication: " << name << "\n";
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

double Application::computePreWorkloads(int scheduleMode,
                                        double chainedCostCoeff)
{
    // Get the minimum processed tuples operator in the application.
    long minimizedInput = std::numeric_limits<long>::max();
    for (auto& [nodeName, node] : nodes) {
        if (node->processedTuples < minimizedInput) {
            minimizedInput = node->processedTuples;
        }
    }

    // When minimized input is 0, we set each operator share the same workload.
    if (minimizedInput == 0) {
        for (auto& [nodeName, node] : nodes) {
            node->processedTuples = 1;
        }
        minimizedInput = 1;
    }

    // Calculate the workload of each operator and total workload.
    // Preworkload is quantified by number of requests processed.
    double totalPreWorkload = 0;
    for (auto& [nodeName, node] : nodes) {
        double estimateWork = static_cast<double>(node->processedTuples);
        if (scheduleMode == 0) {
            // Binpack: weight outgoing chained calls by their estimated CPU
            // cost (chainedCostCoeff = avg per-call cost / t_e, in units of
            // processed tuples), so operators that emit many — especially
            // remote — chained calls get a larger share of workers. When the
            // coefficient is 0 (e.g. before the estimator warms up), this
            // degrades to the original process-only weighting.
            if (chainedCostCoeff > 0.0 &&
                connectionsWithWeight.count(nodeName) > 0) {
                for (const auto& [_, weight] :
                     connectionsWithWeight.at(nodeName)) {
                    estimateWork += chainedCostCoeff * static_cast<double>(weight);
                }
            }
        } else if (scheduleMode != 3 && scheduleMode != 7 &&
                   connectionsWithWeight.count(nodeName) > 0) {
            for (const auto& [_, weight] : connectionsWithWeight.at(nodeName)) {
                if (minimizedInput == 1) {
                    estimateWork += 0.1;
                } else {
                    estimateWork += 0.1 * static_cast<double>(weight);
                }
            }
        }

        node->preWorkload = std::round(estimateWork / minimizedInput);

        // It should never be less than 1.0, just in case.
        if (node->preWorkload < 1.0) {
            node->preWorkload = 1.0;
        }
        totalPreWorkload += node->preWorkload;
    }

    return totalPreWorkload;
}

void Application::dfsVisit(const std::string& nodeName,
                           std::set<std::string>& visited,
                           NodeList& orderedNodes) const
{
    visited.insert(nodeName);

    // 2. --- (THIS IS THE KEY CHANGE) ---
    //    Get the node pointer using the const getNodes()
    auto nodeIt = nodes.find(nodeName);
    if (nodeIt == nodes.end()) {
        SPDLOG_ERROR("DFS: Node '{}' not found in application nodes.",
                     nodeName);
        // This case should ideally not be hit if graph is consistent
        return;
    }

    orderedNodes.push_back({ nodeName, nodeIt->second });

    // 3. Find successors (this part is unchanged)
    const auto& allConnections = getConnections();
    auto connIt = allConnections.find(nodeName);

    if (connIt != allConnections.end()) {
        const std::vector<std::string>& successors = connIt->second;

        // 4. Recurse for each successor (this part is unchanged)
        for (const std::string& successorName : successors) {
            if (visited.find(successorName) == visited.end()) {
                dfsVisit(successorName, visited, orderedNodes);
            }
        }
    }
}

Application::NodeList Application::getNodesDFSOrder()
{
    NodeList orderedNodes;
    std::set<std::string> visited;

    const auto& inputNodes = getInputNodes();
    if (inputNodes.empty()) {
        SPDLOG_WARN("Cannot get DFS order, application has no input nodes.");
        return orderedNodes;
    }

    // Start a DFS from each input node
    for (const std::string& inputNodeName : inputNodes) {
        if (visited.find(inputNodeName) == visited.end()) {
            dfsVisit(inputNodeName, visited, orderedNodes);
        }
    }

    SPDLOG_INFO("--- DFS Ordered NodeList Start (Size: {}) ---",
                orderedNodes.size());

    for (size_t i = 0; i < orderedNodes.size(); ++i) {
        const auto& [nodeName, nodePtr] = orderedNodes[i];
        SPDLOG_INFO("[{}] Name: {}, Ptr: {:p}, Resource: {}",
                    i,
                    nodeName,
                    (void*)nodePtr.get(),
                    nodePtr ? nodePtr->reqResource : 0.0);
    }

    SPDLOG_INFO("--- DFS Ordered NodeList End ---");

    return orderedNodes;
}

std::map<std::string, double> Application::quantiseFromPreWorkloads(
  const std::map<std::string, double>& preWorkloads,
  int numHosts)
{
    double totalPreWorkload = 0.0;
    for (const auto& [name, pw] : preWorkloads) {
        totalPreWorkload += pw;
    }

    struct Quantised
    {
        std::string name; // operator name
        int units;        // 1 unit == 0.1 workers
        double frac;      // fractional part kept for tie‑breaks
    };

    const int TOTAL_UNITS = numHosts * 10; // 0.1‑granularity budget
    std::vector<Quantised> bucket;
    bucket.reserve(preWorkloads.size());

    int usedUnits = 0;

    //--------------------------------------------------------------------------
    // step 1: convert each exact share to "baseUnits" (floor in 0.1 steps)
    //--------------------------------------------------------------------------
    for (const auto& [name, pw] : preWorkloads) {

        double exactShare =
          static_cast<double>(numHosts) * pw / totalPreWorkload; // original
        double rawUnitsD = exactShare * 10.0;                    // 0.1 units
        int baseUnits = static_cast<int>(std::floor(rawUnitsD));

        if (baseUnits == 0) // enforce the 0.1 minimum
            baseUnits = 1;

        double frac = rawUnitsD - baseUnits; // 0 ≤ frac < 1

        bucket.push_back({ name, baseUnits, frac });
        usedUnits += baseUnits;
    }

    //--------------------------------------------------------------------------
    // step 2: distribute (+) or steal (–) leftover units
    //--------------------------------------------------------------------------

    // 2a. We have *too few* units → hand out leftovers to highest "frac"
    int remain = TOTAL_UNITS - usedUnits;
    if (remain > 0) {
        std::sort(bucket.begin(),
                  bucket.end(),
                  [](const Quantised& a, const Quantised& b) {
                      return a.frac > b.frac; // descending
                  });
        for (int i = 0; i < remain; ++i)
            bucket[i % bucket.size()].units += 1;
    }

    // 2b. We have *too many* units → take units from lowest "frac"
    else if (remain < 0) {
        remain = -remain; // units to remove   (> 0)

        // sort buckets by ascending fractional remainder (cheapest to cut)
        std::vector<Quantised*> order;
        order.reserve(bucket.size());
        for (auto& q : bucket)
            order.push_back(&q);

        std::sort(order.begin(),
                  order.end(),
                  [](const Quantised* a, const Quantised* b) {
                      return a->frac < b->frac;
                  });

        // round‑robin removal: at most one unit per bucket per sweep
        std::size_t i = 0;
        while (remain > 0) {
            Quantised* q = order[i];

            if (q->units > 1) { // keep ≥ 0.1 (1 unit) per bucket
                --q->units;
                --remain;
            }

            i = (i + 1) % order.size(); // next bucket in the cycle
            /* If we've looped back to the beginning and could not remove
               anything on this entire sweep, it means all buckets are at
               their minimum 0.1 already. */
            if (i == 0 && remain > 0) {
                SPDLOG_ERROR("Unable to rebalance to integer‑0.1 units");
                throw std::runtime_error("quantisation failed");
            }
        }
    }

    //--------------------------------------------------------------------------
    // step 3: write the quantised values back to each node
    //--------------------------------------------------------------------------
    std::map<std::string, double> reqResource;
    for (auto& q : bucket)
        reqResource[q.name] = q.units / 10.0;

    return reqResource;
}

void Application::quantiseResources(const int numHosts,
                                    int scheduleMode,
                                    double chainedCostCoeff)
{
    computePreWorkloads(scheduleMode, chainedCostCoeff);

    // Gather each operator's preWorkload and run the shared quantisation.
    std::map<std::string, double> preWorkloads;
    for (auto& [name, n] : nodes) {
        preWorkloads[name] = n->preWorkload;
    }

    auto reqResource = quantiseFromPreWorkloads(preWorkloads, numHosts);

    // Write the quantised values back to each node.
    for (auto& [name, n] : nodes) {
        n->reqResource = reqResource.at(name);
    }
}
}