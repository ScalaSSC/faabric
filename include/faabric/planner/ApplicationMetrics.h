#pragma once

#include <faabric/planner/CoeffEstimator.h>
#include <faabric/planner/InstanceMetrics.h>
#include <faabric/planner/LatencyHistogramConfig.h>
#include <faabric/planner/planner.pb.h>
#include <faabric/proto/faabric.pb.h>
#include <faabric/scheduler/InstancesRuntimeStats.h>
#include <faabric/util/clock.h>
#include <faabric/util/locks.h>
#include <faabric/util/logging.h>
#include <faabric/util/string_tools.h>

#include <atomic>
#include <cstdint>
#include <limits>
#include <map>
#include <memory>
#include <rapidjson/document.h>
#include <rapidjson/prettywriter.h>
#include <rapidjson/stringbuffer.h>
#include <rapidjson/writer.h>
#include <set>
#include <shared_mutex>
#include <string>
#include <vector>

namespace faabric::planner {

class ApplicationMetrics
{
  public:
    ApplicationMetrics(std::string appNameIn, int periodIn)
      : appName(appNameIn)
      , period(periodIn) {};

    void record(const std::map<int, std::shared_ptr<faabric::Message>>& msgMap,
                int runningReqs,
                int64_t recordStartTime)
    {
        int msgMapSize = msgMap.size();
        if (msgMapSize == 0) {
            return;
        }

        faabric::util::FullLock lock(opMx);

        avgExecutionOperators =
          avgExecutionOperators +
          (static_cast<double>(msgMapSize) - avgExecutionOperators) /
            (count + 1);
        avgRunningReqs =
          avgRunningReqs +
          (static_cast<double>(runningReqs) - avgRunningReqs) / (count + 1);

        int64_t tempStartTime = std::numeric_limits<int64_t>::max();
        int64_t tempEndTime = std::numeric_limits<int64_t>::min();
        for (auto& [msgId, msg] : msgMap) {
            std::string operatorName = getOperatorName(msg);
            std::string instanceName = getInstanceName(msg);
            if (!instances.contains(instanceName)) {
                instances[instanceName] =
                  std::make_unique<InstanceMetrics>(instanceName, period);
            }
            instances[instanceName]->record(msg);

            // Update the edge weight map based on chained messages.
            for (int32_t chainedMsgId : msg->chainedmsgids()) {
                if (!msgMap.count(chainedMsgId)) {
                    continue;
                }
                auto chainedMsg = msgMap.at(chainedMsgId);
                std::string chainedOpt = getOperatorName(chainedMsg);
                edgeWeightMap[operatorName][chainedOpt]++;
            }

            if (msg->plannerqueuetime() < tempStartTime) {
                tempStartTime = msg->plannerqueuetime();
            }
            if (msg->workerexecuteend() > tempEndTime) {
                tempEndTime = msg->workerexecuteend();
            }
        }
        count++;
        int64_t tempLatencyMicro = tempEndTime - tempStartTime;

        // Latency in microseconds is less than 10ms (10,000 us)
        if (tempLatencyMicro < 10000) {
            latenciesUnderTen[static_cast<int>(tempLatencyMicro)]++;
        } else {
            // Otherwise, record in milliseconds
            int tempLatencyMilli = tempLatencyMicro / 1000;
            latenciesOverTen[tempLatencyMilli]++;
        }

        int64_t tempTotalLatencyMicro;
        if (recordStartTime > 0) {
            tempTotalLatencyMicro =
              faabric::util::getGlobalClock().epochMicros() - recordStartTime;
        } else {
            tempTotalLatencyMicro =
              faabric::util::getGlobalClock().epochMicros() - tempStartTime;
        }
        // Latency in microseconds is less than 10ms (10,000 us)
        if (tempTotalLatencyMicro < 10000) {
            totalLatenciesUnderTen[static_cast<int>(tempTotalLatencyMicro)]++;
        } else {
            // Otherwise, record in milliseconds
            int tempLatencyMilli = tempTotalLatencyMicro / 1000;
            totalLatenciesOverTen[tempLatencyMilli]++;
        }

        if (tempStartTime < startTime) {
            startTime = tempStartTime;
        }
        if (tempEndTime > endTime) {
            endTime = tempEndTime;
        }

        if (tempEndTime != std::numeric_limits<int64_t>::min()) {
            // tempEndTime is in microseconds.
            int64_t secondKey = tempEndTime / 1000000;

            runtimeMetricsHistory[secondKey].init(histConfig.totalBins);
            runtimeMetricsHistory[secondKey].count++;

            int latencyMs = static_cast<int>(tempTotalLatencyMicro / 1000);
            int binIdx = histConfig.getBinIdx(latencyMs);
            runtimeMetricsHistory[secondKey].latencyBins[binIdx]++;
        }
    }

    std::map<std::string, long> getWorkloads()
    {
        faabric::util::FullLock lock(opMx);

        std::map<std::string, long> workloads;
        for (const auto& [instName, instancePtr] : instances) {
            workloads[instName] = instancePtr->getCount();
        }

        return workloads;
    }

    std::map<std::string, long> getOptWorkloads()
    {
        faabric::util::FullLock lock(opMx);

        std::map<std::string, long> instWorkloadMap;
        for (const auto& [instName, instancePtr] : instances) {
            instWorkloadMap[instName] = instancePtr->getCount();
        }

        std::map<std::string, long> operatorWorkloadMap;

        for (const auto& [instName, count] : instWorkloadMap) {
            auto userFuncParTuple = util::splitUserFuncPar(instName);
            std::string funcName = std::get<0>(userFuncParTuple) + "_" +
                                   std::get<1>(userFuncParTuple);
            operatorWorkloadMap[funcName] += count;
        }

        return operatorWorkloadMap;
    }

    std::map<std::string, std::map<std::string, int>> getEdgeWeightMap() const
    {
        faabric::util::FullLock lock(opMx);
        return edgeWeightMap;
    }

    struct RuntimeMetricsRecord
    {
        int count = 0;
        std::map<std::string, int> hostQueueSize; // <host_ip, total_queue_size>

        // Currently the initialization size is 1000, which means recording
        // latencies up to 1 second in microsecond granularity.
        std::vector<int> latencyBins;
        void init(int totalBins)
        {
            if (latencyBins.empty()) {
                latencyBins.resize(totalBins, 0);
            }
        }
        int workersNum = 0;
        std::atomic<int> inputRate = 0;
        int waitingQueueSize = 0; // planner waiting queue length snapshot
        int64_t waitingQueueAgeMicros = 0; // age of oldest waiting message (us)
        bool plannerQueueSaturated = false;
        // CoeffEstimator snapshot at this second
        double coeffC = 0.0; // physical CPU budget C (us/s)
        double alpha = 0.0; // local chained-call overhead coefficient (us/call)
        double beta = 0.0; // remote chained-call overhead coefficient (us/call)
        double gamma =
          0.0; // per-dest-host fixed overhead coefficient (us/host)
        double maxProcessed = 0.0; // C / avgExecTime — baseline max req/s
        double avgExecTime =
          0.0; // weighted avg exec time across instances (us)
        double avgChainedRatio = 0.0; // chained calls per processed request
        double avgLocalChainedRatio =
          0.0; // local chained calls per processed request
        double avgRemoteChainedRatio =
          0.0; // remote chained calls per processed request
        double avgNumDestHosts = 0.0; // avg distinct dest hosts per worker
        // Each entry records one coeffEstimator.update() call (one per
        // saturated host per second): throughput / execTime / local / remote /
        // distinct remote hosts.
        struct RlsUpdateEntry
        {
            double throughput;
            double execTime;
            double localChained;
            double remoteChained;
            double remoteHosts;
        };
        std::vector<RlsUpdateEntry> rlsUpdates;
    };

    rapidjson::Document getMetrics() const
    {
        faabric::util::FullLock lock(opMx);

        rapidjson::Document doc;
        doc.SetObject();
        auto& alloc = doc.GetAllocator();

        // Add basic application-level metrics.
        doc.AddMember(
          "appName", rapidjson::Value(appName.c_str(), alloc), alloc);
        doc.AddMember("period", period, alloc);
        doc.AddMember("count", count, alloc);
        doc.AddMember("avgRunningReqs", avgRunningReqs, alloc);
        doc.AddMember("startTime", startTime, alloc);
        doc.AddMember("endTime", endTime, alloc);
        doc.AddMember("avgExecutionOperators", avgExecutionOperators, alloc);

        // Calculate throughput as messages per second.
        double throughput = 0.0;
        if (endTime > startTime) {
            // Assuming startTime and endTime are in Microseconds.
            throughput = static_cast<double>(count) * 1e6 /
                         static_cast<double>(endTime - startTime);
        }
        doc.AddMember("throughput", throughput, alloc);

        // Compute percentiles from the latencies histogram.
        std::map<int, int> combinedLatencies = latenciesUnderTen;
        for (const auto& [latencyMilli, freq] : latenciesOverTen) {
            combinedLatencies[latencyMilli * 1000] += freq;
        }

        long totalCount = 0;
        for (const auto& [latency, freq] : combinedLatencies) {
            totalCount += freq;
        }

        int medianLatency = 0;
        int p95Latency = 0;
        int p99Latency = 0;

        if (totalCount > 0) {
            long cumCount = 0;
            // Calculate thresholds for each percentile.
            double medianThreshold = totalCount * 0.50;
            double p95Threshold = totalCount * 0.95;
            double p99Threshold = totalCount * 0.99;

            for (const auto& [latencyMicro, freq] : combinedLatencies) {
                cumCount += freq;
                if (medianLatency == 0 && cumCount >= medianThreshold) {
                    medianLatency = latencyMicro;
                }
                if (p95Latency == 0 && cumCount >= p95Threshold) {
                    p95Latency = latencyMicro;
                }
                if (p99Latency == 0 && cumCount >= p99Threshold) {
                    p99Latency = latencyMicro;
                    break; // Optimization: exit once all percentiles are found.
                }
            }
        }

        // Add computed percentiles to the JSON document.
        doc.AddMember("medianLatency", medianLatency, alloc);
        doc.AddMember("p95Latency", p95Latency, alloc);
        doc.AddMember("p99Latency", p99Latency, alloc);

        // Compute percentiles from the latencies histogram.
        std::map<int, int> combinedTotalLatencies = totalLatenciesUnderTen;
        for (const auto& [latencyMilli, freq] : totalLatenciesOverTen) {
            combinedTotalLatencies[latencyMilli * 1000] += freq;
        }

        long totalLatencyCount = 0;
        for (const auto& [latency, freq] : combinedTotalLatencies) {
            totalLatencyCount += freq;
        }

        int medianTotalLatency = 0;
        int p95TotalLatency = 0;
        int p99TotalLatency = 0;

        if (totalLatencyCount > 0) {
            long cumCount = 0;
            // Calculate thresholds for each percentile.
            double medianTotalThreshold = totalLatencyCount * 0.50;
            double p95TotalThreshold = totalLatencyCount * 0.95;
            double p99TotalThreshold = totalLatencyCount * 0.99;

            for (const auto& [latencyMicro, freq] : combinedTotalLatencies) {
                cumCount += freq;
                if (medianTotalLatency == 0 &&
                    cumCount >= medianTotalThreshold) {
                    medianTotalLatency = latencyMicro;
                }
                if (p95TotalLatency == 0 && cumCount >= p95TotalThreshold) {
                    p95TotalLatency = latencyMicro;
                }
                if (p99TotalLatency == 0 && cumCount >= p99TotalThreshold) {
                    p99TotalLatency = latencyMicro;
                    break; // Optimization: exit once all percentiles are found.
                }
            }
        }

        // Add computed percentiles to the JSON document.
        doc.AddMember("medianTotalLatency", medianTotalLatency, alloc);
        doc.AddMember("p95TotalLatency", p95TotalLatency, alloc);
        doc.AddMember("p99TotalLatency", p99TotalLatency, alloc);

        // MODIFICATION: Add the runtime count history as a JSON array with
        // version markers.
        rapidjson::Value historyArray(rapidjson::kArrayType);

        if (!runtimeMetricsHistory.empty()) {
            int64_t startSec = runtimeMetricsHistory.begin()->first;
            int64_t endSec = runtimeMetricsHistory.rbegin()->first;

            int currentVersion = 0;
            auto it = versionHistory.upper_bound(startSec);
            if (it != versionHistory.begin()) {
                auto prevIt = it;
                --prevIt;
                currentVersion = prevIt->second;
            }

            std::string initialVStr =
              "version_" + std::to_string(currentVersion);
            rapidjson::Value initialVal;
            initialVal.SetString(
              initialVStr.c_str(), initialVStr.length(), alloc);
            historyArray.PushBack(initialVal, alloc);

            for (int64_t s = startSec; s <= endSec; ++s) {
                if (versionHistory.count(s)) {
                    int newVersion = versionHistory.at(s);

                    if (newVersion != currentVersion) {
                        currentVersion = newVersion;
                        std::string vStr =
                          "version_" + std::to_string(currentVersion);
                        rapidjson::Value vVal;
                        vVal.SetString(vStr.c_str(), vStr.length(), alloc);
                        historyArray.PushBack(vVal, alloc);
                    }
                }
                int inputRate = 0;
                int currentCount = 0;
                int totalQueueSize = 0;

                int secMedianLat = 0;
                int secP95Lat = 0;
                int secP99Lat = 0;

                int workersNum = 0;
                bool plannerSaturated = false;
                double coeffC = 0.0;
                double alpha = 0.0;
                double beta = 0.0;
                double gamma = 0.0;
                double maxProcessed = 0.0;

                if (runtimeMetricsHistory.count(s)) {
                    const auto& record = runtimeMetricsHistory.at(s);
                    inputRate = record.inputRate;
                    currentCount = record.count;

                    workersNum = record.workersNum;
                    plannerSaturated = record.plannerQueueSaturated;
                    coeffC = record.coeffC;
                    alpha = record.alpha;
                    beta = record.beta;
                    gamma = record.gamma;
                    maxProcessed = record.maxProcessed;

                    for (const auto& [hostIp, qSize] : record.hostQueueSize) {
                        totalQueueSize += qSize;
                    }

                    if (currentCount > 0) {
                        long target50 = currentCount * 0.50;
                        long target95 = currentCount * 0.95;
                        long target99 = currentCount * 0.99;

                        long cumCount = 0;
                        for (int i = 0; i < histConfig.totalBins; ++i) {
                            if (record.latencyBins[i] == 0)
                                continue;

                            cumCount += record.latencyBins[i];

                            int currentLatMs = histConfig.getLatencyMs(i);

                            if (secMedianLat == 0 && cumCount >= target50)
                                secMedianLat = currentLatMs;
                            if (secP95Lat == 0 && cumCount >= target95)
                                secP95Lat = currentLatMs;
                            if (secP99Lat == 0 && cumCount >= target99) {
                                secP99Lat = currentLatMs;
                                break;
                            }
                        }
                    }
                }

                long long totalWaitSum = 0;
                long totalWaitCount = 0;
                long long totalExecSum = 0;
                long totalExecCount = 0;

                int totalExecutors = 0;
                double totalCpuLoad = 0.0;
                int hostCountWithCpu = 0;

                for (const auto& [ip, workerNode] : clusterWorkerMetrics) {
                    for (const auto& [instanceName, timeSeries] :
                         workerNode.instances) {
                        auto it = timeSeries.history.find(s);
                        if (it != timeSeries.history.end()) {
                            const auto& metrics = it->second;

                            totalWaitSum += static_cast<long long>(
                                              metrics.workerQueueTime.average) *
                                            metrics.workerQueueTime.count;
                            totalWaitCount += metrics.workerQueueTime.count;

                            totalExecSum += static_cast<long long>(
                                              metrics.workerExecTime.average) *
                                            metrics.workerExecTime.count;
                            totalExecCount += metrics.workerExecTime.count;
                        }
                    }
                    auto execIt = workerNode.executorsHistory.find(s);
                    if (execIt != workerNode.executorsHistory.end()) {
                        totalExecutors += execIt->second;
                    }

                    auto cpuIt = workerNode.cpuLoadHistory.find(s);
                    if (cpuIt != workerNode.cpuLoadHistory.end()) {
                        totalCpuLoad += cpuIt->second;
                        hostCountWithCpu++;
                    }
                }

                double avgExecutors = workersNum > 0
                                        ? static_cast<double>(totalExecutors) /
                                            static_cast<double>(workersNum)
                                        : 0.0;

                int avgWaitTime =
                  totalWaitCount > 0
                    ? static_cast<int>(totalWaitSum / totalWaitCount)
                    : 0;
                int avgExecTime =
                  totalExecCount > 0
                    ? static_cast<int>(totalExecSum / totalExecCount)
                    : 0;

                double avgCpuLoad = hostCountWithCpu > 0
                                      ? (totalCpuLoad / hostCountWithCpu)
                                      : 0.0;
                char cpuBuffer[32], coeffCBuf[32], alphaBuf[32], betaBuf[32],
                  gammaBuf[32], maxProcBuf[32];
                snprintf(cpuBuffer, sizeof(cpuBuffer), "%.2f", avgCpuLoad);
                snprintf(coeffCBuf, sizeof(coeffCBuf), "%.0f", coeffC);
                snprintf(alphaBuf, sizeof(alphaBuf), "%.4f", alpha);
                snprintf(betaBuf, sizeof(betaBuf), "%.4f", beta);
                snprintf(gammaBuf, sizeof(gammaBuf), "%.4f", gamma);
                snprintf(maxProcBuf, sizeof(maxProcBuf), "%.1f", maxProcessed);

                std::string entryStr =
                  std::to_string(inputRate) + " / " +
                  std::to_string(currentCount) + " / " +
                  std::to_string(workersNum) + " / " +
                  std::to_string(totalQueueSize) + " / " +
                  std::to_string(avgWaitTime) + " / " +
                  std::to_string(avgExecTime) + " / " +
                  std::to_string(totalExecutors) + " / " +
                  std::to_string(avgExecutors) + " / " +
                  std::string(cpuBuffer) + " / " +
                  std::to_string(secMedianLat) + " / " +
                  std::to_string(secP95Lat) + " / " +
                  std::to_string(secP99Lat) + " / " +
                  (plannerSaturated ? "saturated" : "no") + " / " +
                  std::string(coeffCBuf) + " / " + std::string(alphaBuf) +
                  " / " + std::string(betaBuf) + " / " + std::string(gammaBuf) +
                  " / " + std::string(maxProcBuf);

                historyArray.PushBack(
                  rapidjson::Value(entryStr.c_str(), alloc).Move(), alloc);
            }
        }

        doc.AddMember("runtimeCountHistory: input_rate / count / worker_num / "
                      "queue_size / avg_wait / avg_exec / executors / "
                      "average_executors / avg_cpu / p50latency (ms) / "
                      "p95latency (ms) / p99latency (ms) / planner_saturated"
                      " / coeff_C / alpha / beta / gamma / max_processed",
                      historyArray,
                      alloc);

        // Optionally, if you want to include per-instance metrics, add them
        // here.
        rapidjson::Value instancesObj(rapidjson::kObjectType);
        for (const auto& [instName, instancePtr] : instances) {
            std::string instanceJson = instancePtr->getMetrics();
            SPDLOG_INFO("we got instanceJson: {}", instanceJson);
            rapidjson::Document instDoc;
            instDoc.Parse(instanceJson.c_str());
            if (instDoc.HasParseError()) {
                // Handle the parse error as needed.
                continue;
            }
            rapidjson::Value instValue;
            instValue.CopyFrom(instDoc, alloc);
            instancesObj.AddMember(
              rapidjson::Value(instName.c_str(), alloc).Move(),
              instValue,
              alloc);
        }
        doc.AddMember("instances", instancesObj, alloc);

        // Per-worker time series: throughput / num_dest_workers /
        // total_chained_count
        rapidjson::Value workerMetricDetailsObj(rapidjson::kObjectType);

        if (!runtimeMetricsHistory.empty()) {
            int64_t startSec = runtimeMetricsHistory.begin()->first;
            int64_t endSec = runtimeMetricsHistory.rbegin()->first;

            for (const auto& [ip, workerNode] : clusterWorkerMetrics) {
                rapidjson::Value workerArr(rapidjson::kArrayType);

                for (int64_t s = startSec; s <= endSec; ++s) {
                    int totalInputCount = 0;
                    int totalThroughput = 0;
                    long long queueNumWeightedSum = 0;
                    long long queueNumTotalCount = 0;
                    long long queueTimeWeightedSum = 0;
                    long long queueTimeTotalCount = 0;
                    std::map<std::string, int> aggregatedChained;

                    for (const auto& [instanceName, timeSeries] :
                         workerNode.instances) {
                        auto it = timeSeries.history.find(s);
                        if (it != timeSeries.history.end()) {
                            const auto& rec = it->second;
                            totalInputCount += rec.inputCount;
                            totalThroughput += rec.throughput;
                            if (rec.workerQueueNum.count > 0) {
                                queueNumWeightedSum +=
                                  static_cast<long long>(
                                    rec.workerQueueNum.average) *
                                  rec.workerQueueNum.count;
                                queueNumTotalCount += rec.workerQueueNum.count;
                            }
                            if (rec.workerQueueTime.count > 0) {
                                queueTimeWeightedSum +=
                                  static_cast<long long>(
                                    rec.workerQueueTime.average) *
                                  rec.workerQueueTime.count;
                                queueTimeTotalCount +=
                                  rec.workerQueueTime.count;
                            }
                            for (const auto& [dest, cnt] :
                                 rec.chainedCallHistory) {
                                aggregatedChained[dest] += cnt;
                            }
                        }
                    }
                    auto satIt = workerNode.saturatedHistory.find(s);
                    bool workerSaturated =
                      satIt != workerNode.saturatedHistory.end() &&
                      satIt->second;

                    int numDestWorkers =
                      static_cast<int>(aggregatedChained.size());
                    int totalChainedCount = 0;
                    for (const auto& [dest, cnt] : aggregatedChained) {
                        totalChainedCount += cnt;
                    }
                    double avgQueueNum =
                      queueNumTotalCount > 0
                        ? static_cast<double>(queueNumWeightedSum) /
                            queueNumTotalCount
                        : 0.0;
                    double avgQueueTime =
                      queueTimeTotalCount > 0
                        ? static_cast<double>(queueTimeWeightedSum) /
                            queueTimeTotalCount
                        : 0.0;

                    char queueNumBuf[16], queueTimeBuf[16];
                    snprintf(
                      queueNumBuf, sizeof(queueNumBuf), "%.1f", avgQueueNum);
                    snprintf(
                      queueTimeBuf, sizeof(queueTimeBuf), "%.1f", avgQueueTime);
                    std::string entry =
                      std::to_string(totalInputCount) + " / " +
                      std::to_string(totalThroughput) + " / " + queueNumBuf +
                      " / " + queueTimeBuf + " / " +
                      std::to_string(numDestWorkers) + " / " +
                      std::to_string(totalChainedCount) + " / " +
                      (workerSaturated ? "saturated" : "no");
                    workerArr.PushBack(
                      rapidjson::Value(entry.c_str(), alloc).Move(), alloc);
                }

                workerMetricDetailsObj.AddMember(
                  rapidjson::Value(ip.c_str(), alloc).Move(), workerArr, alloc);
            }
        }

        doc.AddMember(
          "workerMetricDetails: input / processed / "
          "queueNum / queueTime / num_dest_workers / total_chained_count",
          workerMetricDetailsObj,
          alloc);

        // Per-second RLS update inputs: one entry per saturated host per
        // second. Format: throughput / execTime(us) / localChained /
        // remoteChained
        rapidjson::Value rlsHistObj(rapidjson::kObjectType);
        if (!runtimeMetricsHistory.empty()) {
            int64_t startSec = runtimeMetricsHistory.begin()->first;
            int64_t endSec = runtimeMetricsHistory.rbegin()->first;
            for (int64_t s = startSec; s <= endSec; ++s) {
                auto it = runtimeMetricsHistory.find(s);
                if (it == runtimeMetricsHistory.end() ||
                    it->second.rlsUpdates.empty())
                    continue;
                rapidjson::Value arr(rapidjson::kArrayType);
                for (const auto& u : it->second.rlsUpdates) {
                    char buf[128];
                    snprintf(buf,
                             sizeof(buf),
                             "%.1f / %.1f / %.1f / %.1f",
                             u.throughput,
                             u.execTime,
                             u.localChained,
                             u.remoteChained);
                    arr.PushBack(rapidjson::Value(buf, alloc).Move(), alloc);
                }
                std::string secKey = std::to_string(s);
                rlsHistObj.AddMember(
                  rapidjson::Value(secKey.c_str(), alloc).Move(), arr, alloc);
            }
        }
        doc.AddMember("rlsUpdateHistory: throughput / execTime(us) / "
                      "localChained / remoteChained",
                      rlsHistObj,
                      alloc);

        return doc;
    }

    void setVersion(int version)
    {
        faabric::util::FullLock lock(opMx);

        // Record the version change at the current second
        int64_t secondKey =
          faabric::util::getGlobalClock().epochMicros() / 1000000;
        versionHistory[secondKey] = version;
    }

    // Thread-safe: called by multiple threads to accumulate incoming request
    // count for the current second. Uses a shared lock when the per-second
    // entry already exists (common case) and falls back to a full lock only
    // when a new entry must be created.
    void recordInputRate(int n)
    {
        int64_t secondKey = faabric::util::getGlobalClock().epochSeconds();

        {
            faabric::util::SharedLock sharedLock(opMx);
            auto it = runtimeMetricsHistory.find(secondKey);
            if (it != runtimeMetricsHistory.end()) {
                it->second.inputRate.fetch_add(n, std::memory_order_relaxed);
                return;
            }
        }

        // Entry not found — release shared lock, acquire exclusive lock to
        // insert, then increment.
        faabric::util::FullLock fullLock(opMx);
        runtimeMetricsHistory[secondKey].inputRate.fetch_add(
          n, std::memory_order_relaxed);
    }

    // Per-second snapshot for one instance on one worker.
    struct InstanceRecord
    {
        faabric::scheduler::AverageAndCount workerQueueTime;
        faabric::scheduler::AverageAndCount workerQueueNum;
        faabric::scheduler::AverageAndCount workerExecTime;
        int inputCount = 0;
        int throughput = 0;
        std::map<std::string, int> chainedCallHistory;
    };
    struct InstanceTimeSeries
    {
        std::map<time_t, InstanceRecord> history;
    };
    struct WorkerNodeMetrics
    {
        std::map<std::string, InstanceTimeSeries> instances;
        std::map<time_t, double> cpuLoadHistory;
        std::map<time_t, double> executorsHistory;
        std::map<time_t, bool> saturatedHistory;
        // Worker-level outgoing chained calls: time -> (destHost -> count).
        // Sourced from WorkerStats.workerChainHistory (Scheduler-only).
        std::map<time_t, std::map<std::string, int>> chainHistory;
    };

    // Adds or updates the worker stats fetched from a specific host.
    // Deserialise one InstanceMetricsResultProto into an InstanceRecord.
    // The proto carries only the latest second's data (isRuntime=true path),
    // so we read the first (and only) entry of each map.
    static void deserialiseInstanceMetrics(
      const faabric::InstanceMetricsResultProto& proto,
      InstanceRecord& rec)
    {
        if (!proto.workerqueuetimestats().empty()) {
            const auto& qt = proto.workerqueuetimestats().begin()->second;
            rec.workerQueueTime.average = qt.average();
            rec.workerQueueTime.count = qt.count();
        }
        if (!proto.workerqueuenumstats().empty()) {
            const auto& qn = proto.workerqueuenumstats().begin()->second;
            rec.workerQueueNum.average = qn.average();
            rec.workerQueueNum.count = qn.count();
        }
        if (!proto.workerexectimestats().empty()) {
            const auto& et = proto.workerexectimestats().begin()->second;
            rec.workerExecTime.average = et.average();
            rec.workerExecTime.count = et.count();
        }
        if (!proto.inputcountstats().empty()) {
            rec.inputCount = proto.inputcountstats().begin()->second;
        }
        if (!proto.throughputstats().empty()) {
            rec.throughput = proto.throughputstats().begin()->second;
        }
        if (!proto.chainedcallhistory().empty()) {
            const auto& chainedSecond =
              proto.chainedcallhistory().begin()->second;
            for (const auto& [destHost, count] : chainedSecond.hostcount()) {
                rec.chainedCallHistory[destHost] += count;
            }
        }
    }

    void recordWorkerMetrics(
      const std::map<std::string, std::unique_ptr<faabric::WorkerStats>>&
        results)
    {
        faabric::util::FullLock lock(opMx);
        auto currentTime = faabric::util::getGlobalClock().epochSeconds();

        // Iterate hosts, update the per-worker records and coeff estimator.
        for (const auto& [ip, statsPtr] : results) {
            if (!statsPtr) {
                continue;
            }

            clusterWorkerMetrics[ip].cpuLoadHistory[currentTime] =
              statsPtr->cpuload();
            clusterWorkerMetrics[ip].executorsHistory[currentTime] =
              statsPtr->executorsnum();

            // Aggregate across all instances on this host before RLS update.
            double hostThroughput = 0.0;
            long long hostExecTimeSum = 0;
            long long hostExecTimeCount = 0;
            double hostLocalChainedCalls = 0.0;
            double hostRemoteChainedCalls = 0.0;
            long long hostQueueNumSum = 0;
            long long hostQueueNumCount = 0;
            long long hostQueueTimeSum = 0;
            long long hostQueueTimeCount = 0;

            for (const auto& [instanceName, metricsProto] :
                 statsPtr->workermetrics()) {
                auto& rec = clusterWorkerMetrics[ip]
                              .instances[instanceName]
                              .history[currentTime];
                deserialiseInstanceMetrics(metricsProto, rec);

                hostThroughput += rec.throughput;
                if (rec.workerExecTime.count > 0) {
                    hostExecTimeSum +=
                      static_cast<long long>(rec.workerExecTime.average) *
                      rec.workerExecTime.count;
                    hostExecTimeCount += rec.workerExecTime.count;
                }
                if (rec.workerQueueNum.count > 0) {
                    hostQueueNumSum +=
                      static_cast<long long>(rec.workerQueueNum.average) *
                      rec.workerQueueNum.count;
                    hostQueueNumCount += rec.workerQueueNum.count;
                }
                if (rec.workerQueueTime.count > 0) {
                    hostQueueTimeSum +=
                      static_cast<long long>(rec.workerQueueTime.average) *
                      rec.workerQueueTime.count;
                    hostQueueTimeCount += rec.workerQueueTime.count;
                }
            }

            // Worker-level chain counts: read from
            // WorkerStats.workerChainHistory (proto field 10), aggregated by
            // dest host. dest == ip (or empty) is local, anything else is
            // remote. Also persist into clusterWorkerMetrics for the snapshot
            // block below.
            auto& persistedChain =
              clusterWorkerMetrics[ip].chainHistory[currentTime];
            std::set<std::string> remoteDestHosts;
            for (const auto& [ts, secProto] : statsPtr->workerchainhistory()) {
                for (const auto& [dest, cnt] : secProto.hostcount()) {
                    persistedChain[dest] += cnt;
                    if (dest.empty() || dest == ip) {
                        hostLocalChainedCalls += cnt;
                    } else {
                        hostRemoteChainedCalls += cnt;
                        remoteDestHosts.insert(dest);
                    }
                }
            }
            double hostRemoteHostCount =
              static_cast<double>(remoteDestHosts.size());

            // Host is saturated when the host-wide weighted average queue depth
            // and wait time both exceed their thresholds.
            double hostAvgQueueNum =
              hostQueueNumCount > 0
                ? static_cast<double>(hostQueueNumSum) / hostQueueNumCount
                : 0.0;
            double hostAvgQueueTime =
              hostQueueTimeCount > 0
                ? static_cast<double>(hostQueueTimeSum) / hostQueueTimeCount
                : 0.0;
            bool hostSaturated =
              (hostAvgQueueNum > workerQueueNumThreshold) &&
              (hostAvgQueueTime > workerQueueTimeThresholdUs);

            clusterWorkerMetrics[ip].saturatedHistory[currentTime] =
              hostSaturated;

            // One RLS update per host per second, only when saturated.
            if (hostSaturated && hostThroughput > 0.0 &&
                hostExecTimeCount > 0) {
                double avgExecTime =
                  static_cast<double>(hostExecTimeSum) / hostExecTimeCount;
                coeffEstimator.update(hostThroughput,
                                      avgExecTime,
                                      hostLocalChainedCalls,
                                      hostRemoteChainedCalls,
                                      hostRemoteHostCount);
                runtimeMetricsHistory[currentTime].rlsUpdates.push_back(
                  { hostThroughput,
                    avgExecTime,
                    hostLocalChainedCalls,
                    hostRemoteChainedCalls,
                    hostRemoteHostCount });
                SPDLOG_DEBUG(
                  "CoeffEstimator updated for host {}: alpha={:.4f}, "
                  "beta={:.4f}, gamma={:.4f} (throughput={:.0f}, "
                  "execTime={:.1f}us, localChained={:.0f}, "
                  "remoteChained={:.0f}, remoteHosts={:.0f})",
                  ip,
                  coeffEstimator.alpha(),
                  coeffEstimator.beta(),
                  coeffEstimator.gamma(),
                  hostThroughput,
                  avgExecTime,
                  hostLocalChainedCalls,
                  hostRemoteChainedCalls,
                  hostRemoteHostCount);
            }

            int totalQueueSize = 0;
            for (const auto& [instName, qSize] : statsPtr->instancequeuenum()) {
                totalQueueSize += qSize;
            }
            runtimeMetricsHistory[currentTime].hostQueueSize[ip] =
              totalQueueSize;
        }

        int workersNum = results.size();
        runtimeMetricsHistory[currentTime].workersNum = workersNum;

        // Snapshot CoeffEstimator state + derived scaling fields.
        {
            long long execTimeSum = 0;
            long execTimeCount = 0;
            long long totalChained = 0;
            long long totalLocalChained = 0;
            long long totalRemoteChained = 0;
            long long totalThroughput = 0;
            double totalDestHosts = 0.0;
            int workerCount = 0;

            for (const auto& [workerIp, workerNode] : clusterWorkerMetrics) {
                bool workerHasData = false;
                for (const auto& [instName, timeSeries] :
                     workerNode.instances) {
                    auto it = timeSeries.history.find(currentTime);
                    if (it == timeSeries.history.end())
                        continue;
                    const auto& r = it->second;
                    if (r.workerExecTime.count > 0) {
                        execTimeSum +=
                          static_cast<long long>(r.workerExecTime.average) *
                          r.workerExecTime.count;
                        execTimeCount += r.workerExecTime.count;
                    }
                    totalThroughput += r.throughput;
                    workerHasData = true;
                }

                // Worker-level chained calls + distinct dest hosts come from
                // chainHistory (Scheduler-recorded), not per-instance data.
                auto chainIt = workerNode.chainHistory.find(currentTime);
                if (chainIt != workerNode.chainHistory.end()) {
                    std::set<std::string> destSet;
                    for (const auto& [dest, cnt] : chainIt->second) {
                        totalChained += cnt;
                        if (dest.empty() || dest == workerIp) {
                            totalLocalChained += cnt;
                        } else {
                            totalRemoteChained += cnt;
                            destSet.insert(dest);
                        }
                    }
                    totalDestHosts += (double)destSet.size();
                    workerHasData = true;
                }

                if (workerHasData) {
                    workerCount++;
                }
            }

            double avgExecTime =
              execTimeCount > 0
                ? static_cast<double>(execTimeSum) / execTimeCount
                : 0.0;
            double avgChainedRatio =
              totalThroughput > 0
                ? static_cast<double>(totalChained) / totalThroughput
                : 0.0;
            double avgLocalChainedRatio =
              totalThroughput > 0
                ? static_cast<double>(totalLocalChained) / totalThroughput
                : 0.0;
            double avgRemoteChainedRatio =
              totalThroughput > 0
                ? static_cast<double>(totalRemoteChained) / totalThroughput
                : 0.0;
            double avgNumDestHosts =
              workerCount > 0 ? totalDestHosts / workerCount : 0.0;

            auto& snap = runtimeMetricsHistory[currentTime];
            snap.coeffC = coeffEstimator.C();
            snap.alpha = coeffEstimator.alpha();
            snap.beta = coeffEstimator.beta();
            snap.gamma = coeffEstimator.gamma();
            snap.maxProcessed =
              coeffEstimator.maxProcessed(avgExecTime, 0.0, 0.0, 0.0);
            snap.avgExecTime = avgExecTime;
            snap.avgChainedRatio = avgChainedRatio;
            snap.avgLocalChainedRatio = avgLocalChainedRatio;
            snap.avgRemoteChainedRatio = avgRemoteChainedRatio;
            snap.avgNumDestHosts = avgNumDestHosts;
        }

        // Clean old metrics beyond a certain time window (e.g., 1000 seconds)
        time_t cutoffTime = currentTime - 1000;
        for (auto& [ip, workerNode] : clusterWorkerMetrics) {
            for (auto& [instanceName, timeSeries] : workerNode.instances) {
                auto& historyMap = timeSeries.history;
                while (!historyMap.empty() &&
                       historyMap.begin()->first < cutoffTime) {
                    historyMap.erase(historyMap.begin());
                }
            }

            while (!workerNode.cpuLoadHistory.empty() &&
                   workerNode.cpuLoadHistory.begin()->first < cutoffTime) {
                workerNode.cpuLoadHistory.erase(
                  workerNode.cpuLoadHistory.begin());
            }

            while (!workerNode.executorsHistory.empty() &&
                   workerNode.executorsHistory.begin()->first < cutoffTime) {
                workerNode.executorsHistory.erase(
                  workerNode.executorsHistory.begin());
            }

            while (!workerNode.saturatedHistory.empty() &&
                   workerNode.saturatedHistory.begin()->first < cutoffTime) {
                workerNode.saturatedHistory.erase(
                  workerNode.saturatedHistory.begin());
            }

            while (!workerNode.chainHistory.empty() &&
                   workerNode.chainHistory.begin()->first < cutoffTime) {
                workerNode.chainHistory.erase(workerNode.chainHistory.begin());
            }
        }

        while (!runtimeMetricsHistory.empty() &&
               runtimeMetricsHistory.begin()->first < cutoffTime) {
            runtimeMetricsHistory.erase(runtimeMetricsHistory.begin());
        }
    }

    struct ScalingSignals
    {
        double avgInputRate = 0.0;
        double avgQueueSize = 0.0;
        int plannerWaitingQueueSize = 0;
        int64_t plannerWaitingQueueAgeMicros = 0;
        bool plannerQueueSaturated = false;
        // CoeffEstimator-derived fields for capacity estimation
        double coeffC = 0.0;      // physical CPU budget per worker (us/s)
        double alpha = 0.0;       // local chained-call overhead (us/call)
        double beta = 0.0;        // remote chained-call overhead (us/call)
        double gamma = 0.0;       // per-dest-host fixed overhead (us/host)
        double avgExecTime = 0.0; // avg exec time per request (us)
        double avgChainedRatio =
          0.0; // total chained calls per processed request
        double avgLocalChainedRatio =
          0.0; // local chained calls per processed request
        double avgRemoteChainedRatio =
          0.0; // remote chained calls per processed request
        double avgNumDestHosts = 0.0; // avg distinct dest hosts per worker
        // totalLoad = inputRate + inputRate × chainedMultiplier
        // chainedMultiplier = chainedOperatorCount / inputOperatorCount
        double chainedMultiplier = 0.0;
    };

    // Numeric per-instance lifecycle snapshot (instanceName = User_Func_Par),
    // fed by record() from each request's payload timestamps. Consumed by the
    // mode-4 non-linear scaling model, which turns snapshot deltas into
    // scheduling-overhead-ratio samples.
    std::map<std::string, InstanceMetrics::Snapshot>
    getInstanceLifecycleSnapshots() const
    {
        faabric::util::SharedLock lock(opMx);
        std::map<std::string, InstanceMetrics::Snapshot> out;
        for (const auto& [name, inst] : instances) {
            out[name] = inst->snapshot();
        }
        return out;
    }

    ScalingSignals getScalingSignals(int windowSec) const
    {
        faabric::util::SharedLock lock(opMx);
        int64_t nowSec = faabric::util::getGlobalClock().epochSeconds();
        int totalInput = 0, totalQueue = 0, validSecs = 0;
        double totalExecTime = 0.0, totalChainedRatio = 0.0,
               totalLocalChainedRatio = 0.0, totalRemoteChainedRatio = 0.0,
               totalDestHosts = 0.0;

        for (int64_t s = nowSec - windowSec; s < nowSec; s++) {
            auto it = runtimeMetricsHistory.find(s);
            if (it == runtimeMetricsHistory.end())
                continue;
            totalInput += it->second.inputRate.load(std::memory_order_relaxed);
            for (const auto& [ip, q] : it->second.hostQueueSize)
                totalQueue += q;
            totalExecTime += it->second.avgExecTime;
            totalChainedRatio += it->second.avgChainedRatio;
            totalLocalChainedRatio += it->second.avgLocalChainedRatio;
            totalRemoteChainedRatio += it->second.avgRemoteChainedRatio;
            totalDestHosts += it->second.avgNumDestHosts;
            validSecs++;
        }

        ScalingSignals sig;
        if (validSecs > 0) {
            sig.avgInputRate = (double)totalInput / validSecs;
            sig.avgQueueSize = (double)totalQueue / validSecs;
            sig.avgExecTime = totalExecTime / validSecs;
            sig.avgChainedRatio = totalChainedRatio / validSecs;
            sig.avgLocalChainedRatio = totalLocalChainedRatio / validSecs;
            sig.avgRemoteChainedRatio = totalRemoteChainedRatio / validSecs;
            sig.avgNumDestHosts = totalDestHosts / validSecs;
        }

        // CoeffEstimator physical coefficients (current estimates, not
        // windowed)
        sig.coeffC = coeffEstimator.C();
        sig.alpha = coeffEstimator.alpha();
        sig.beta = coeffEstimator.beta();
        sig.gamma = coeffEstimator.gamma();

        // Latest planner waiting queue snapshot
        if (!runtimeMetricsHistory.empty()) {
            const auto& latest = runtimeMetricsHistory.rbegin()->second;
            sig.plannerWaitingQueueSize = latest.waitingQueueSize;
            sig.plannerWaitingQueueAgeMicros = latest.waitingQueueAgeMicros;
            sig.plannerQueueSaturated = latest.plannerQueueSaturated;
        }
        return sig;
    }

    struct RuntimeRecordSummary
    {
        int count = 0;
        int workersNum = 0;
    };

    // Returns up to the last `n` records where plannerQueueSaturated == true,
    // newest first.
    std::vector<RuntimeRecordSummary> getLastNSaturatedRecords(int n) const
    {
        faabric::util::SharedLock lock(opMx);
        std::vector<RuntimeRecordSummary> result;
        result.reserve(n);
        for (auto it = runtimeMetricsHistory.rbegin();
             it != runtimeMetricsHistory.rend() && (int)result.size() < n;
             ++it) {
            if (!it->second.plannerQueueSaturated)
                continue;
            result.push_back({ it->second.count, it->second.workersNum });
        }
        return result;
    }

    // Computes the adaptive target host count using the FaaSFlow Adaptive
    // algorithm: derives avgCountPerWorker from the last N saturated records
    // and divides avgInputRate by it, clamped to [1, numHosts].
    int computeAdaptiveHostCount(double avgInputRate, int numHosts) const
    {
        double avgCountPerWorker = 0.0;
        auto saturated = getLastNSaturatedRecords(10);
        if (!saturated.empty()) {
            double total = 0.0;
            int valid = 0;
            for (const auto& rec : saturated) {
                if (rec.workersNum > 0) {
                    total += static_cast<double>(rec.count) / rec.workersNum;
                    ++valid;
                }
            }
            if (valid > 0)
                avgCountPerWorker = total / valid;
        }

        if (avgCountPerWorker <= 0.0 || avgInputRate <= 0.0)
            return numHosts;

        double totalRequired =
          std::max(1.0,
                   std::min(static_cast<double>(numHosts),
                            avgInputRate / avgCountPerWorker));
        return std::min(numHosts, static_cast<int>(std::ceil(totalRequired)));
    }

    // Called from updateRuntimeStats to snapshot the planner's waiting queue.
    // Saturation is computed here so getScalingSignals just reads the result.
    void recordQueueSnapshot(int queueSize, int64_t queueAgeMicros)
    {
        faabric::util::FullLock lock(opMx);
        int64_t currentTime = faabric::util::getGlobalClock().epochSeconds();
        auto& rec = runtimeMetricsHistory[currentTime];
        rec.waitingQueueSize = queueSize;
        rec.waitingQueueAgeMicros = queueAgeMicros;
        rec.plannerQueueSaturated =
          (queueSize > plannerWaitingQueueSizeThreshold) &&
          (queueAgeMicros > plannerWaitingQueueAgeThresholdUs);
    }

    // Dynamically update saturation thresholds (called from resetParameter).
    bool setThreshold(const std::string& key, int64_t value)
    {
        faabric::util::FullLock lock(opMx);
        if (key == "worker_queue_num_threshold") {
            workerQueueNumThreshold = static_cast<int>(value);
        } else if (key == "worker_queue_time_threshold") {
            workerQueueTimeThresholdUs = value;
        } else if (key == "planner_queue_size_threshold") {
            plannerWaitingQueueSizeThreshold = static_cast<int>(value);
        } else if (key == "planner_queue_age_threshold") {
            plannerWaitingQueueAgeThresholdUs = value;
        } else {
            return false;
        }
        SPDLOG_INFO("ApplicationMetrics: set {} = {}", key, value);
        return true;
    }

    bool setCoeff(const std::string& key, double value)
    {
        faabric::util::FullLock lock(opMx);
        if (!coeffEstimator.set(key, value))
            return false;
        SPDLOG_INFO("ApplicationMetrics: set coeff {} = {:.4f}", key, value);
        return true;
    }

    bool setRlsParam(const std::string& key, double value)
    {
        faabric::util::FullLock lock(opMx);
        if (!coeffEstimator.setRlsParam(key, value))
            return false;
        SPDLOG_INFO(
          "ApplicationMetrics: set RLS param {} = {:.6f}", key, value);
        return true;
    }

    void reset()
    {
        faabric::util::FullLock lock(opMx);

        count = 0;
        startTime = std::numeric_limits<int64_t>::max();
        endTime = std::numeric_limits<int64_t>::min();
        latenciesUnderTen.clear();
        latenciesOverTen.clear();
        totalLatenciesUnderTen.clear();
        totalLatenciesOverTen.clear();

        for (auto& [instName, instancePtr] : instances) {
            instancePtr->reset();
        }

        edgeWeightMap.clear();
        runtimeMetricsHistory.clear();
        versionHistory.clear();

        avgExecutionOperators = 0.0;
        avgRunningReqs = 0.0;
        coeffEstimator = CoeffEstimator{};
        clusterWorkerMetrics.clear();
    }

  private:
    mutable std::shared_mutex opMx;
    const std::string appName;
    int period;
    std::map<std::string, std::unique_ptr<InstanceMetrics>> instances;
    std::map<int, int> latenciesOverTen;
    std::map<int, int> latenciesUnderTen;
    std::map<int, int> totalLatenciesOverTen;
    std::map<int, int> totalLatenciesUnderTen;

    long count = 0;
    int64_t startTime = std::numeric_limits<int64_t>::max();
    int64_t endTime = std::numeric_limits<int64_t>::min();
    double avgExecutionOperators = 0.0;
    double avgRunningReqs = 0.0;

    // Online RLS coefficient estimator (alpha, beta in CPU-budget model).
    CoeffEstimator coeffEstimator;

    // thresholds for worker-side saturation detection
    int workerQueueNumThreshold = 500;
    int64_t workerQueueTimeThresholdUs = 100000; // microseconds

    // thresholds for planner-level waiting queue saturation
    int plannerWaitingQueueSizeThreshold = 5000;
    int64_t plannerWaitingQueueAgeThresholdUs = 500000;

    LatencyHistogramConfig histConfig;

    std::map<int64_t, RuntimeMetricsRecord> runtimeMetricsHistory;

    std::map<std::string, std::map<std::string, int>> edgeWeightMap;

    std::map<int64_t, int> versionHistory;

    // MAP<host_ip, WorkerNodeMetrics>
    std::map<std::string, WorkerNodeMetrics> clusterWorkerMetrics;

    std::string getInstanceName(const std::shared_ptr<faabric::Message> msg)
    {
        return msg->user() + "_" + msg->function() + "_" +
               std::to_string(msg->parallelismid());
    }

    std::string getOperatorName(const std::shared_ptr<faabric::Message> msg)
    {
        return msg->user() + "_" + msg->function();
    }
};

}
