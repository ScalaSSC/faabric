#pragma once

#include <faabric/util/config.h>
#include <faabric/util/locks.h>

#include <algorithm> // std::sort, std::shuffle
#include <cmath>     // std::floor
#include <map>
#include <random> // std::default_random_engine
#include <shared_mutex>
#include <string>
#include <vector>

namespace faabric::batch_scheduler {

struct HostSlot
{
    std::string host;
    std::size_t slots = 0;  // how many entries in the ring
    double remainder = 0.0; // fractional leftover
};

struct GapInfo
{
    std::string host;
    double gap = 0.0;      // positive (expected – source)
    std::size_t slots = 0; // integer allocation
    double rem = 0.0;      // fractional remainder
};

// The number of slots in the ring.
const std::size_t RING_SIZE = 100;

// WindowedRecord is a class used for scheduling requests to stateless operators
// before partitioned stateful operators.
class WindowedRecord
{
  public:
    // Constructor: workers list, target proportions, and window size
    explicit WindowedRecord(std::map<std::string, double>& expectedDist,
                            bool isHead)
      : windowPos(0)
      , windowSize(RING_SIZE)
    {
        faabric::util::FullLock lock(wrMx);
        setWindow(expectedDist, isHead);
    }

    // Schedule a request, returning the chosen host
    std::string schedule(const std::string& recommended)
    {
        faabric::util::FullLock lock(wrMx);

        if (windowPos == windowSize) {
            assignedCount.clear();
            windowPos = 0;
        }
        std::string pickHost = recommended;
        if (assignedCount[recommended] >= windowQuota[recommended]) {
            for (auto& [host, count] : assignedCount) {
                if (assignedCount[host] < windowQuota[host]) {
                    pickHost = host;
                    break;
                }
            }
        }

        assignedCount[pickHost]++;
        windowPos++;
        return pickHost;
    }

    void updateWindow(std::map<std::string, int>& scheduleWeights)
    {
        faabric::util::FullLock lock(wrMx);
        windowSize = 0;
        for (const auto& [host, weight] : scheduleWeights) {
            windowQuota[host] = weight;
            windowSize += weight;
        }
        windowPos = 0;
        assignedCount.clear();
    }

  private:
    std::shared_mutex wrMx;
    // Windowing state
    int windowPos;
    // If might not equals to WINDOW_SIZE
    int windowSize;
    std::map<std::string, int> windowQuota;
    std::map<std::string, int> assignedCount;
    std::string localHost = faabric::util::getSystemConfig().endpointHost;

    // Reset quotas and counters at the start of a window
    void setWindow(std::map<std::string, double>& expectedDist, bool isHead)
    {
        windowSize = 0;
        std::vector<std::string> hosts;
        for (const auto& [host, _] : expectedDist) {
            hosts.push_back(host);
        }
        if (!isHead) {
            // If this operator is the not first one in the group, we only
            // schedule them locally.
            if (windowQuota.count(localHost) == 0) {
                SPDLOG_WARN("For the none-head stateless operator, local host "
                            "{} is not expected distribution",
                            localHost);
                throw std::runtime_error(
                  "Local host is not expected distribution");
            }
            windowQuota[localHost] = RING_SIZE;
        } else {
            // If this operator is the first one int the group, we assign them
            // into workers according to the expected distribution.
            for (const auto& [host, share] : expectedDist) {
                int hostQuota = static_cast<int>(std::ceil(share * RING_SIZE));
                windowQuota[host] = hostQuota;
            }
        }

        windowPos = 0;
        for (const auto& [host, quota] : windowQuota) {
            windowSize += quota;
        }
        assignedCount.clear();
    }
};

class RuntimeSummary
{
  public:
    RuntimeSummary() = default;

    void setOptsCollocateMap(const std::map<std::string, std::string>& w)
    {
        faabric::util::FullLock lock(summaryMx);
        optsCollocateMap = w;
    }

    void setOptsCollocateHeadMap(const std::map<std::string, std::string>& w)
    {
        faabric::util::FullLock lock(summaryMx);
        optsCollocateHeadMap = w;
    }

    void reallocateAll(bool init)
    {
        faabric::util::FullLock lock(summaryMx);
        for (const auto& [instanceName, _] : expectedDist) {
            reallocate(instanceName, init);
        }
    }

    void updateSourceDist(
      std::map<std::string, std::map<std::string, int>> sourceCountStats)
    {
        faabric::util::FullLock lock(summaryMx);
        sourceDist.clear();
        for (const auto& [instanceName, hostCount] : sourceCountStats) {
            int sumCount = 0;
            for (const auto& [host, count] : hostCount) {
                sumCount += count;
            }
            if (sumCount <= 0) {
                SPDLOG_WARN("Total count <= 0 for instance {}", instanceName);
                continue;
            }
            auto& dist = sourceDist[instanceName];
            for (const auto& [host, count] : hostCount) {
                dist[host] = static_cast<double>(count) / sumCount;
            }
        }
    }

    void updateExpDist(
      const std::map<std::string, std::map<std::string, int>>& statelessWeights)
    {
        faabric::util::FullLock lock(summaryMx);
        expectedDist.clear();
        for (const auto& [instanceName, hostWeight] : statelessWeights) {
            int sumWeight = 0;
            for (const auto& [host, weight] : hostWeight) {
                sumWeight += weight;
            }
            if (sumWeight <= 0) {
                SPDLOG_WARN("Total weight <= 0 for instance {}", instanceName);
                continue;
            }
            auto& dist = expectedDist[instanceName];
            for (const auto& [host, weight] : hostWeight) {
                dist[host] = static_cast<double>(weight) / sumWeight;
            }
        }
    }

    std::string getHost(const std::string& instance, unsigned int counter)
    {
        faabric::util::SharedLock lock(summaryMx);
        auto it = hostsRingMap.find(instance);
        if (it == hostsRingMap.end()) {
            SPDLOG_WARN("No ring for instance {}", instance);
            throw std::runtime_error("No ring for instance");
        }
        const auto& ring = it->second;
        // counter is unsigned, ring.size() is size_t → coerce to size_t
        size_t idx = static_cast<size_t>(counter) % ring.size();
        return ring[idx];
    }

    std::string getHost(const std::string& instance, std::string recommended)
    {
        faabric::util::SharedLock lock(summaryMx);
        auto it = windowedRecords.find(instance);
        if (it == windowedRecords.end()) {
            SPDLOG_ERROR("No collocate map for instance {} found", instance);
            throw std::runtime_error("No collocate map for instance " +
                                     instance);
        }
        std::string host = it->second->schedule(recommended);
        return host;
    }

  private:
    std::shared_mutex summaryMx;
    std::string localHost = faabric::util::getSystemConfig().endpointHost;

    std::map<std::string, std::map<std::string, double>> sourceDist;
    // The monitored actual distribution.
    // MAP<Instance Name, MAP<IP, distribution>>
    // std::map<std::string, std::map<std::string, double>> actualDist;

    // The expected distribution from centralized scheduler.
    // MAP<Instance Name, MAP<IP, distribution>>
    std::map<std::string, std::map<std::string, double>> expectedDist;

    std::map<std::string, std::string> optsCollocateMap;
    std::map<std::string, std::string> optsCollocateHeadMap;

    std::map<std::string, std::vector<std::string>> hostsRingMap;

    // MAP <USER_FUNC, WindowedRecord>
    std::map<std::string, std::shared_ptr<WindowedRecord>> windowedRecords;

    std::map<std::string, int> getReallocateLocalShare(
      const std::string& instance)
    {
        SPDLOG_DEBUG(
          "Reallocating hostsShares for instance {} which runs locally",
          instance);

        std::map<std::string, int> hostsSlots;
        // 1. Look up the expected and source distribution for this instance.
        const auto expIt = expectedDist.find(instance);
        const auto srcIt = sourceDist.find(instance);
        if (expIt == expectedDist.end() || srcIt == sourceDist.end()) {
            SPDLOG_ERROR("No expected/source distribution for instance {}",
                         instance);
            throw std::runtime_error(
              "No expected/source distribution for instance");
        }
        const auto& expMap = expIt->second;
        const auto& srcMap = srcIt->second;

        if (expMap.count(localHost) == 0 || srcMap.count(localHost) == 0) {
            SPDLOG_WARN("No local host in expected or source distribution");
            throw std::runtime_error(
              "No local host in expected or source distribution");
        }

        double expectedLocal = expMap.at(localHost);
        double sourceLocal = srcMap.at(localHost);

        double localShare = expectedLocal / sourceLocal;
        int localSlots = static_cast<int>(std::round(localShare * RING_SIZE));
        hostsSlots[localHost] = localSlots;

        // 2. If generated request on local host is less than expected, we
        // assign all requests locally.
        if (localSlots >= RING_SIZE) {
            hostsSlots[localHost] = RING_SIZE;
            return hostsSlots;
        }

        // 3. Otherwise, we have to assign the remaining requests to other
        // hosts according to round-robin.
        std::size_t remaining = RING_SIZE - localSlots;

        // 4. Build the gaps between the expected and source distributions of
        // other hosts.
        std::vector<GapInfo> gaps;
        double totalGap = 0.0;

        for (const auto& [host, expShare] : expMap) {
            if (host == localHost)
                continue;
            double srcShare = srcMap.count(host) ? srcMap.at(host) : 0.0;
            double gap = expShare - srcShare;
            if (gap > 0.0) {
                totalGap += gap;
                gaps.push_back({ host, gap });
            }
        }

        // 5. Allocate the remaining slots.
        auto allocateByShare = [&](double share) {
            double scaled = share * remaining;
            std::size_t n = static_cast<std::size_t>(std::floor(scaled));
            return std::pair<std::size_t, double>{ n, scaled - n };
        };

        if (totalGap == 0.0) {
            // It shouldn't happen, but just in case. Allocate requets locally.
            hostsSlots[localHost] = RING_SIZE;
            return hostsSlots;
        } else {
            for (auto& g : gaps) {
                auto [cnt, frac] = allocateByShare(g.gap / totalGap);
                g.slots = cnt;
                g.rem = frac;
            }
        }

        // 6. Hand out the remaining slots to the largest remainders.
        std::size_t used = 0;
        for (const auto& g : gaps)
            used += g.slots;
        std::size_t left = remaining - used;

        std::sort(
          gaps.begin(), gaps.end(), [](const GapInfo& a, const GapInfo& b) {
              return a.rem > b.rem;
          });
        for (std::size_t i = 0; i < left; ++i)
            ++gaps[i].slots;

        // 7. Emit the final ring.
        for (const auto& g : gaps) {
            hostsSlots[g.host] = g.slots;
        }

        return hostsSlots;
    }

    std::vector<std::string> reallocateLocalImpl(const std::string& instance)
    {
        SPDLOG_DEBUG("Reallocating ring for instance {} which runs locally",
                     instance);
        std::vector<std::string> hostsRing;
        auto hostsSlots = getReallocateLocalShare(instance);
        for (const auto& [host, slots] : hostsSlots) {
            hostsRing.insert(hostsRing.end(), slots, host);
        }
        std::shuffle(hostsRing.begin(),
                     hostsRing.end(),
                     std::default_random_engine{ std::random_device{}() });
        return hostsRing;
    }

    std::vector<std::string> reallocateRemoteImpl(const std::string& instance)
    {
        SPDLOG_DEBUG("Reallocating ring for instance {} which runs remotely",
                     instance);
        std::vector<std::string> hostsRing;
        hostsRing.reserve(RING_SIZE);

        // 1. Look up the expected distribution for this instance.
        const auto expIt = expectedDist.find(instance);
        if (expIt == expectedDist.end()) {
            return hostsRing; // nothing to do
        }
        const auto& distMap = expIt->second; // map<host, share>

        // 2. Build per‑host slot counts using the “largest remainder” rule.
        std::vector<HostSlot> slots;
        slots.reserve(distMap.size());

        std::size_t used = 0;
        for (const auto& [host, share] : distMap) {
            double scaled = share * RING_SIZE;
            // std::floor is the C++ standard‑library function that rounds a
            // floating‑point value down to the nearest integer boundary.
            std::size_t cnt = static_cast<std::size_t>(std::floor(scaled));
            used += cnt;
            slots.push_back({ host, cnt, scaled - cnt });
        }

        // 3. Hand out the remaining slots to the largest remainders.
        std::size_t remaining = RING_SIZE - used;
        std::sort(
          slots.begin(), slots.end(), [](const HostSlot& a, const HostSlot& b) {
              return a.remainder > b.remainder;
          });
        for (std::size_t i = 0; i < remaining; ++i) {
            ++slots[i].slots;
        }

        // 4. Emit the final ring.
        for (const auto& h : slots) {
            hostsRing.insert(hostsRing.end(), h.slots, h.host);
        }

        std::shuffle(hostsRing.begin(),
                     hostsRing.end(),
                     std::default_random_engine{ std::random_device{}() });

        return hostsRing;
    }

    // Calculate the adjusted distribution.
    void reallocate(const std::string& instanceName, bool init)
    {
        bool isLocal = false;
        for (const auto& [host, _] : expectedDist[instanceName]) {
            if (host == localHost) {
                isLocal = true;
                break;
            }
        }

        // If no sourceDist found, we assume each host has the same.
        if (sourceDist.find(instanceName) == sourceDist.end()) {
            sourceDist[instanceName] = expectedDist[instanceName];
        }
        // Print the source distribution and expected distribution.
        SPDLOG_INFO("The source distribution for {} is:", instanceName);
        for (const auto& [host, share] : sourceDist[instanceName]) {
            SPDLOG_INFO("{}: {:.2f}", host, share);
        }
        SPDLOG_INFO("The expected distribution for {} is:", instanceName);
        for (const auto& [host, share] : expectedDist[instanceName]) {
            SPDLOG_INFO("{}: {:.2f}", host, share);
        }

        // If collocate with partitioned operators.
        if (optsCollocateHeadMap.contains(instanceName)) {
            if (init) {
                auto expDist = expectedDist[instanceName];
                windowedRecords[instanceName] =
                  std::make_shared<WindowedRecord>(expDist, true);
            }
            return;
        }
        if (optsCollocateMap.contains(instanceName)) {
            if (init) {
                auto expDist = expectedDist[instanceName];
                windowedRecords[instanceName] =
                  std::make_shared<WindowedRecord>(expDist, false);
            } else {
                auto dist = getReallocateLocalShare(instanceName);
                windowedRecords[instanceName]->updateWindow(dist);
            }
            return;
        }

        if (isLocal) {
            // If the next instance will run on the local host.
            hostsRingMap[instanceName] = reallocateLocalImpl(instanceName);
        } else {
            // Otherwise, just round-robin according to the expected
            // distribution.
            hostsRingMap[instanceName] = reallocateRemoteImpl(instanceName);
        }

        auto localHostRing = hostsRingMap[instanceName];
        std::unordered_map<std::string, int> counts;
        for (const auto& host : localHostRing) {
            ++counts[host];
        }

        int total = static_cast<int>(localHostRing.size());
        SPDLOG_INFO("The host ring info for {} is:", instanceName);
        for (auto& [host, cnt] : counts) {
            double share = static_cast<double>(cnt) / total;
            SPDLOG_INFO("{}: {:.2f} / count {}", host, share, cnt);
        }
    }
};
}