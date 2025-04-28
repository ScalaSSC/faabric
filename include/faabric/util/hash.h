#pragma once

#include <faabric/util/locks.h>
#include <faabric/util/serialization.h>

#include <cmath>
#include <cstring> // For std::memcpy in converting strings to vectors
#include <functional>
#include <iostream>
#include <map>
#include <vector>

namespace faabric::util {

class ConsistentHashRing
{
  public:
    ConsistentHashRing(int nodes = 0)
    {
        faabric::util::FullLock lock(ringMutex);
        // Every node gets weight 1
        for (int id = 0; id < nodes; ++id) {
            nodeWeights[id] = 1;
        }
        rebuildRing();
    }

    /// Construct from a list { nodeID, weight } and a total VN limit
    explicit ConsistentHashRing(
      const std::vector<std::pair<int, int>>& nodeWeightPairs = {})
    {
        faabric::util::FullLock lock(ringMutex);
        for (const auto& [id, w] : nodeWeightPairs) {
            nodeWeights[id] = w;
        }
        rebuildRing();
    }

    explicit ConsistentHashRing(const std::map<int, int>& nodeWeightMap = {})
    {
        faabric::util::FullLock lock(ringMutex);
        for (const auto& [id, w] : nodeWeightMap) {
            nodeWeights[id] = w;
            SPDLOG_DEBUG(
              "ConsistentHashRing: node ID {} with weight {}", id, w);
        }
        // Double check, node should start from 0 until n-1
        for (int i = 0; i < nodeWeights.size(); ++i) {
            if (nodeWeights.find(i) == nodeWeights.end()) {
                SPDLOG_WARN(
                  "ConsistentHashRing: node ID {} not found in node weights",
                  i);
                throw std::runtime_error("ConsistentHashRing: node ID " +
                                         std::to_string(i) +
                                         " not found in node weights");
            }
        }

        rebuildRing();
    }

    /// Return the physical node responsible for an arbitrary key
    int getNode(const std::vector<uint8_t>& keyBytes)
    {
        faabric::util::SharedLock lock(ringMutex);
        std::size_t h = faabric::util::hashVector(keyBytes);
        auto it = ring.lower_bound(h);
        if (it == ring.end())
            it = ring.begin();
        return it->second;
    }

    /// (Occasionally handy for debugging)
    std::pair<size_t, int> getHashAndNode(const std::vector<uint8_t>& keyBytes)
    {
        faabric::util::SharedLock lock(ringMutex);
        std::size_t h = faabric::util::hashVector(keyBytes);
        auto it = ring.lower_bound(h);
        if (it == ring.end())
            it = ring.begin();
        return { h, it->second };
    }

  private:
    /* ---------- helpers -------------------------------------------------- */

    void rebuildRing()
    {

        ring.clear();
        if (nodeWeights.empty() || totalVirtualNodes == 0)
            return;

        // 1. basic share and rounding‑error bookkeeping
        int totalWeight = 0;
        std::unordered_map<int, double> fractional;
        // the number of virtual nodes assigned to each node
        std::unordered_map<int, int> vnPerNode;
        for (auto& [_, w] : nodeWeights)
            totalWeight += w;

        int assigned = 0;
        for (auto& [id, w] : nodeWeights) {
            double exact = static_cast<double>(w) * totalVirtualNodes /
                           static_cast<double>(totalWeight);
            int vn = static_cast<int>(std::floor(exact));
            vnPerNode[id] = vn;
            fractional[id] = exact - vn;
            assigned += vn;
        }

        // 2. distribute any leftover VNs to the largest remainders
        int leftover = totalVirtualNodes - assigned;
        while (leftover-- > 0) {
            int bestId = -1;
            double bestFrac = -1.0;
            for (auto& [id, frac] : fractional) {
                if (frac > bestFrac) {
                    bestFrac = frac;
                    bestId = id;
                }
            }
            ++vnPerNode[bestId];
            fractional[bestId] = 0.0; // ensure we don’t favour it again
        }

        // 3. actually plant the virtual nodes
        for (auto& [id, vn] : vnPerNode) {
            for (int v = 0; v < vn; ++v) {
                std::string key =
                  "Node" + std::to_string(id) + "Virtual" + std::to_string(v);
                std::vector<uint8_t> keyBytes(key.begin(), key.end());
                std::size_t h = faabric::util::hashVector(keyBytes);
                ring[h] = id;
            }
        }
    }

    /* ---------- data ------------------------------------------------------ */
    std::map<std::size_t, int> ring;          // hash → nodeID
    std::unordered_map<int, int> nodeWeights; // nodeID → weight
    int totalVirtualNodes = 1000;             // global VN budget
    std::shared_mutex ringMutex;              // protects the ring
};

}