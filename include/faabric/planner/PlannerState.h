#pragma once

#include <faabric/batch-scheduler/BatchScheduler.h>
#include <faabric/batch-scheduler/SchedulingDecision.h>
#include <faabric/planner/planner.pb.h>
#include <faabric/proto/faabric.pb.h>
#include <faabric/scheduler/InstancesRuntimeStats.h>
#include <faabric/util/queue.h>

#include <cstdint>
#include <limits>
#include <list>
#include <map>
#include <rapidjson/document.h>
#include <rapidjson/prettywriter.h>
#include <rapidjson/stringbuffer.h>
#include <rapidjson/writer.h>
#include <set>
#include <shared_mutex>
#include <unordered_map>

namespace faabric::planner {

// The Latency accuracy is dynamic to balance the accuracy and the memory cost.
// From 0 - 50, the accuracy is 1ms, from 50 - 500ms, the accuracy is 5ms, etc.
struct LatencyHistogramConfig
{
    int limit1 = 50;    // 0 - 50ms
    int limit2 = 500;   // 50 - 500ms
    int limit3 = 2000;  // 500 - 2000ms
    int limit4 = 10000; // 2000 - 10000ms

    int step1 = 1;   // 1ms
    int step2 = 5;   // 5ms
    int step3 = 50;  // 50ms
    int step4 = 500; // 500ms

    int offset1 = 0;
    int offset2 = limit1 / step1;
    int offset3 = offset2 + (limit2 - limit1) / step2;
    int offset4 = offset3 + (limit3 - limit2) / step3;
    int overflowBin = offset4 + (limit4 - limit3) / step4;
    int totalBins = overflowBin + 1;

    inline int getBinIdx(int latencyMs) const
    {
        if (latencyMs < 0)
            return 0;
        if (latencyMs < limit1)
            return offset1 + latencyMs / step1;
        if (latencyMs < limit2)
            return offset2 + (latencyMs - limit1) / step2;
        if (latencyMs < limit3)
            return offset3 + (latencyMs - limit2) / step3;
        if (latencyMs < limit4)
            return offset4 + (latencyMs - limit3) / step4;
        return overflowBin;
    }

    inline int getLatencyMs(int binIdx) const
    {
        if (binIdx < offset2)
            return binIdx * step1;
        if (binIdx < offset3)
            return limit1 + (binIdx - offset2) * step2;
        if (binIdx < offset4)
            return limit2 + (binIdx - offset3) * step3;
        if (binIdx < overflowBin)
            return limit3 + (binIdx - offset4) * step4;
        return limit4;
    }
};

// TODO - Period is not used yet
class InstanceMetrics
{
  public:
    InstanceMetrics(std::string instanceNameIn, int periodIn)
      : instanceName(instanceNameIn)
      , period(periodIn) {};

    long getCount() const
    {
        faabric::util::FullLock lock(instMx);
        return count;
    }

    void record(const std::shared_ptr<faabric::Message>& msg)
    {
        faabric::util::FullLock lock(instMx);

        int newPlannerQueueTime =
          msg->plannerpoptime() - msg->plannerqueuetime();
        int newPlannerConsumeTime =
          msg->plannerdispatchtime() - msg->plannerpoptime();
        int newDistpatchTime =
          msg->dispatchreceivetime() - msg->plannerdispatchtime();
        int newWorkerQueueTime = msg->workerqueuewaittime();
        int newExecutorPrepTime = msg->executorpreparetime();
        int newWorkerExecuteTime =
          msg->workerexecuteend() - msg->workerexecutestart();
        int newExecuteBatchSize = msg->executebatchsize();
        bool isSchedLocally = msg->isscheduledlocally();

        count++;

        avgPlannerQueueTime =
          avgPlannerQueueTime +
          (static_cast<double>(newPlannerQueueTime) - avgPlannerQueueTime) /
            static_cast<double>(count);
        avgPlannerConsumeTime =
          avgPlannerConsumeTime +
          (static_cast<double>(newPlannerConsumeTime) - avgPlannerConsumeTime) /
            static_cast<double>(count);
        // Dispatch time is calculated cross workers, if the platform does not
        // support ntp. It can be negative.
        if (newDistpatchTime >= 0) {
            avgDispatchTime =
              avgDispatchTime +
              (static_cast<double>(newDistpatchTime) - avgDispatchTime) /
                static_cast<double>(count);
        }
        avgWorkerQueueTime =
          avgWorkerQueueTime +
          (static_cast<double>(newWorkerQueueTime) - avgWorkerQueueTime) /
            static_cast<double>(count);
        avgExecutorPrepTime =
          avgExecutorPrepTime +
          (static_cast<double>(newExecutorPrepTime) - avgExecutorPrepTime) /
            static_cast<double>(count);
        avgWorkerExecuteTime =
          avgWorkerExecuteTime +
          (static_cast<double>(newWorkerExecuteTime) - avgWorkerExecuteTime) /
            static_cast<double>(count);
        int recordBatchSize = ++batchCounter[newExecuteBatchSize];
        if (recordBatchSize >= newExecuteBatchSize) {
            batchCount++;
            avgExecuteBatchSize =
              avgExecuteBatchSize +
              (static_cast<double>(newExecuteBatchSize) - avgExecuteBatchSize) /
                static_cast<double>(batchCount);
            batchCounter[newExecuteBatchSize] = 0;
        }

        if (isSchedLocally) {
            numSchedLocally++;
        }
        std::string host = msg->executedhost();
        hostStats[host]++;
    }

    std::string getMetrics() const
    {
        SPDLOG_INFO("Getting metrics for instance {}", instanceName);
        faabric::util::FullLock lock(instMx);

        rapidjson::Document doc;
        doc.SetObject();
        auto& alloc = doc.GetAllocator();

        doc.AddMember(
          "instanceName", rapidjson::Value(instanceName.c_str(), alloc), alloc);
        doc.AddMember("period", period, alloc);
        doc.AddMember("count", count, alloc);
        double schedLocallyRate =
          static_cast<double>(numSchedLocally) / static_cast<double>(count);
        doc.AddMember("schedLocallyRate", schedLocallyRate, alloc);
        doc.AddMember("avgPlannerScheduleTime", avgPlannerQueueTime, alloc);
        doc.AddMember("avgPlannerDispatchTime", avgPlannerConsumeTime, alloc);
        doc.AddMember("avgDispatchTime", avgDispatchTime, alloc);
        doc.AddMember("avgWorkerQueueTime", avgWorkerQueueTime, alloc);
        doc.AddMember("avgExecutorPrepTime", avgExecutorPrepTime, alloc);
        doc.AddMember("avgWorkerExecuteTime", avgWorkerExecuteTime, alloc);
        doc.AddMember("avgExecuteBatchSize", avgExecuteBatchSize, alloc);

        rapidjson::Value hostStatsObj(rapidjson::kObjectType);
        for (const auto& [host, stat] : hostStats) {
            rapidjson::Value key;
            key.SetString(host.c_str(),
                          static_cast<rapidjson::SizeType>(host.size()),
                          alloc);
            rapidjson::Value value;
            value.SetInt(stat);
            hostStatsObj.AddMember(key, value, alloc);
        }
        doc.AddMember("hostStats", hostStatsObj, alloc);

        rapidjson::StringBuffer buffer;
        rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);
        doc.Accept(writer);

        SPDLOG_INFO("Returning metrics: {}", buffer.GetString());
        return buffer.GetString();
    }

    void reset()
    {
        faabric::util::FullLock lock(instMx);

        count = 0;
        batchCount = 0;
        numSchedLocally = 0;

        avgPlannerQueueTime = 0.0;
        avgPlannerConsumeTime = 0.0;
        avgDispatchTime = 0.0;
        avgWorkerQueueTime = 0.0;
        avgExecutorPrepTime = 0.0;
        avgWorkerExecuteTime = 0.0;
        avgExecuteBatchSize = 0.0;

        batchCounter.clear();
        hostStats.clear();
    }

  private:
    // Name is User_Func_Par
    mutable std::shared_mutex instMx;
    const std::string instanceName;
    const int period;
    long count = 0;
    long batchCount = 0;
    long numSchedLocally = 0;

    double avgPlannerQueueTime = 0.0;
    double avgPlannerConsumeTime = 0.0;
    double avgDispatchTime = 0.0;
    double avgWorkerQueueTime = 0.0;
    double avgExecutorPrepTime = 0.0;
    double avgWorkerExecuteTime = 0.0;
    double avgExecuteBatchSize = 0.0;
    std::map<int, int> batchCounter;
    std::map<std::string, int> hostStats;
};

class ApplicationMetrics
{
  public:
    // -----------------------------------------------------------------------
    // CPU-budget model (per worker, at saturation):
    //
    //   C = processedNum_i × execTime_i + alpha × chainedCalls_i
    //                                    + beta  × numDestHosts_i
    //
    // Rearranging into RLS form:
    //   y_i = C - processedNum_i × execTime_i
    //       = alpha × chainedCalls_i + beta × numDestHosts_i
    //
    // where C is a fixed, identical constant across all workers.
    // alpha and beta are estimated online via Recursive Least Squares (RLS)
    // with forgetting factor lambda, updated only when the worker is saturated.
    // -----------------------------------------------------------------------
    struct CoeffEstimator
    {
        // All three unknowns — C, alpha, beta — are estimated jointly from
        // saturated-worker observations via 3-parameter RLS.  No manual tuning
        // of C is required.
        //
        // RLS state: theta = [C, alpha, beta]
        //   C     : total CPU budget per second (us), same for all workers
        //   alpha : chained-call overhead coefficient (us/call)
        //   beta  : fan-out host overhead coefficient (us/host)
        double C = 0.0;       // estimated CPU budget (us/s)
        double alpha = 0.0;   // estimated chained-call cost (us/call)
        double beta = 0.0;    // estimated fan-out host cost (us/host)
        double lambda = 0.97; // RLS forgetting factor
        // 3×3 RLS covariance matrix, row-major (theta has 3 elements)
        double P[9] = { 1e12, 0, 0, 0, 1e12, 0, 0, 0, 1e12 };

        // Update estimates with one saturated-worker observation.
        // Regression form:
        //   y = processedNum × execTime
        //   x = [1, -chainedCalls, -numDestHosts]
        //   y ≈ x · [C, alpha, beta]
        // i.e. processedNum × execTime ≈ C - alpha×chainedCalls -
        // beta×numDestHosts
        void update(double processedNum,
                    double execTime,
                    double chainedCalls,
                    double numDestHosts)
        {
            if (processedNum <= 0.0 || execTime <= 0.0)
                return;
            double y = processedNum * execTime;
            double x[3] = { 1.0, -chainedCalls, -numDestHosts };

            // Px = P · x  (3-vector)
            double Px[3] = { 0.0, 0.0, 0.0 };
            for (int i = 0; i < 3; i++)
                for (int j = 0; j < 3; j++)
                    Px[i] += P[i * 3 + j] * x[j];

            // denom = lambda + x' · Px
            double denom = lambda;
            for (int i = 0; i < 3; i++)
                denom += x[i] * Px[i];
            if (denom < 1e-12)
                return;

            // K = Px / denom
            double K[3] = { Px[0] / denom, Px[1] / denom, Px[2] / denom };

            // theta = [C, alpha, beta]; prediction = x · theta
            double pred = x[0] * C + x[1] * alpha + x[2] * beta;
            // equivalently: C - alpha×chainedCalls - beta×numDestHosts
            double err = y - pred;

            C = C + K[0] * err;
            alpha = std::max(0.0, alpha + K[1] * err);
            beta = std::max(0.0, beta + K[2] * err);

            // P = (P - K·Px') / lambda  (rank-1 update)
            for (int i = 0; i < 3; i++)
                for (int j = 0; j < 3; j++)
                    P[i * 3 + j] = (P[i * 3 + j] - K[i] * Px[j]) / lambda;

            SPDLOG_DEBUG("CoeffEstimator: C={:.0f}, alpha={:.2f}, beta={:.2f} "
                         "(y={:.0f}, err={:.0f}, "
                         "processedNum={:.0f}, execTime={:.1f}us, "
                         "chained={:.0f}, destHosts={:.0f})",
                         C,
                         alpha,
                         beta,
                         y,
                         err,
                         processedNum,
                         execTime,
                         chainedCalls,
                         numDestHosts);
        }

        // Maximum sustainable processed-requests given current coefficients.
        // chainedRatio : chained calls per processed request
        // numDestHosts : fan-out host count
        double maxProcessed(double execTime,
                            double chainedRatio,
                            double numDestHosts) const
        {
            double fanout = beta * numDestHosts;
            double effCost = execTime + alpha * chainedRatio;
            if (effCost <= 0.0 || C <= fanout)
                return 0.0;
            return (C - fanout) / effCost;
        }
    };

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
                if (!msgMap.contains(chainedMsgId)) {
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
                if (versionHistory.contains(s)) {
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

                if (runtimeMetricsHistory.contains(s)) {
                    const auto& record = runtimeMetricsHistory.at(s);
                    inputRate = record.inputRate;
                    currentCount = record.count;

                    workersNum = record.workersNum;
                    plannerSaturated = record.plannerQueueSaturated;

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

                double average_executors =
                  workersNum > 0 ? static_cast<double>(totalExecutors) /
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
                char cpuBuffer[32];
                snprintf(cpuBuffer, sizeof(cpuBuffer), "%.2f", avgCpuLoad);

                std::string entryStr = std::to_string(inputRate) + " / " +
                                       std::to_string(currentCount) + " / " +
                                       std::to_string(workersNum) + " / " +
                                       std::to_string(totalQueueSize) + " / " +
                                       std::to_string(avgWaitTime) + " / " +
                                       std::to_string(avgExecTime) + " / " +
                                       std::to_string(totalExecutors) + " / " +
                                       std::to_string(average_executors) +
                                       " / " + std::string(cpuBuffer) + " / " +
                                       std::to_string(secMedianLat) + " / " +
                                       std::to_string(secP95Lat) + " / " +
                                       std::to_string(secP99Lat) + " / " +
                                       (plannerSaturated ? "saturated" : "no");

                historyArray.PushBack(
                  rapidjson::Value(entryStr.c_str(), alloc).Move(), alloc);
            }
        }

        doc.AddMember("runtimeCountHistory: input_rate / count / worker_num / "
                      "queue_size / avg_wait / avg_exec / executors / "
                      "average_executors / avg_cpu / p50latency (ms) / "
                      "p95latency (ms) / p99latency (ms) / planner_saturated",
                      historyArray,
                      alloc);

        rapidjson::Value versionMetricsObj(rapidjson::kObjectType);

        if (!runtimeMetricsHistory.empty()) {
            int64_t startSec = runtimeMetricsHistory.begin()->first;
            int64_t endSec = runtimeMetricsHistory.rbegin()->first;

            int lastKnownVersion = 0;
            auto it = versionHistory.upper_bound(startSec);
            if (it != versionHistory.begin()) {
                auto prevIt = it;
                --prevIt;
                lastKnownVersion = prevIt->second;
            }

            struct VersionStats
            {
                long totalThroughput = 0;
                int durationSeconds = 0;
                std::vector<int> history;
            };
            std::map<int, VersionStats> versionStatsMap;

            for (int64_t s = startSec; s <= endSec; ++s) {
                if (versionHistory.contains(s)) {
                    lastKnownVersion = versionHistory.at(s);
                }

                int currentThroughput = 0;
                if (runtimeMetricsHistory.contains(s)) {
                    currentThroughput = runtimeMetricsHistory.at(s).count;
                }

                versionStatsMap[lastKnownVersion].totalThroughput +=
                  currentThroughput;
                versionStatsMap[lastKnownVersion].durationSeconds++;
                versionStatsMap[lastKnownVersion].history.push_back(
                  currentThroughput);
            }

            for (const auto& [vId, stats] : versionStatsMap) {
                rapidjson::Value vObj(rapidjson::kObjectType);

                vObj.AddMember("totalThroughput",
                               static_cast<int64_t>(stats.totalThroughput),
                               alloc);
                vObj.AddMember("durationSeconds", stats.durationSeconds, alloc);

                double avgThroughput =
                  stats.durationSeconds > 0
                    ? static_cast<double>(stats.totalThroughput) /
                        stats.durationSeconds
                    : 0.0;
                vObj.AddMember("averageThroughput", avgThroughput, alloc);

                rapidjson::Value historyArray(rapidjson::kArrayType);
                for (int count : stats.history) {
                    historyArray.PushBack(count, alloc);
                }
                vObj.AddMember("history", historyArray, alloc);

                std::string vKey = "version_" + std::to_string(vId);
                versionMetricsObj.AddMember(
                  rapidjson::Value(vKey.c_str(), alloc).Move(), vObj, alloc);
            }
        }

        doc.AddMember("versionMetrics", versionMetricsObj, alloc);

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

                    bool workerSaturated = false;
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
                            if (rec.isSaturated) {
                                workerSaturated = true;
                            }
                        }
                    }

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
          "workerMetricDetails: input / throughput / "
          "queueNum / queueTime / num_dest_workers / total_chained_count",
          workerMetricDetailsObj,
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
        bool isSaturated = false;
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
    };

    // Adds or updates the worker stats fetched from a specific host
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

        for (const auto& [ip, statsPtr] : results) {
            if (!statsPtr) {
                continue;
            }

            clusterWorkerMetrics[ip].cpuLoadHistory[currentTime] =
              statsPtr->cpuload();
            clusterWorkerMetrics[ip].executorsHistory[currentTime] =
              statsPtr->executorsnum();

            for (const auto& [instanceName, metricsProto] :
                 statsPtr->workermetrics()) {
                auto& rec = clusterWorkerMetrics[ip]
                              .instances[instanceName]
                              .history[currentTime];
                deserialiseInstanceMetrics(metricsProto, rec);

                // Compute saturation flag immediately after deserialization.
                rec.isSaturated =
                  (rec.workerQueueNum.average > workerQueueNumThreshold) &&
                  (rec.workerQueueTime.average > workerQueueTimeThresholdUs);

                // Update RLS coefficients only when this instance is saturated.
                if (rec.isSaturated) {
                    double chainedCalls = 0.0;
                    std::set<std::string> destHostSet;
                    for (const auto& [dest, cnt] : rec.chainedCallHistory) {
                        chainedCalls += cnt;
                        if (!dest.empty()) {
                            destHostSet.insert(dest);
                        }
                    }
                    double numDestHosts =
                      static_cast<double>(destHostSet.size());
                    coeffEstimator.update(static_cast<double>(rec.throughput),
                                          rec.workerExecTime.average,
                                          chainedCalls,
                                          numDestHosts);
                    SPDLOG_DEBUG(
                      "CoeffEstimator updated for {}/{}: alpha={:.4f}, "
                      "beta={:.4f} (throughput={}, execTime={:.1f}us, "
                      "chained={:.1f}, destHosts={:.0f})",
                      ip,
                      instanceName,
                      coeffEstimator.alpha,
                      coeffEstimator.beta,
                      rec.throughput,
                      static_cast<double>(rec.workerExecTime.average),
                      chainedCalls,
                      numDestHosts);
                }
            }

            int totalQueueSize = 0;
            for (const auto& [instName, qSize] : statsPtr->instancequeuenum()) {
                totalQueueSize += qSize;
            }
            runtimeMetricsHistory[currentTime].hostQueueSize[ip] =
              totalQueueSize;

            if (isWorkerSaturatedUnlocked(ip)) {
                SPDLOG_DEBUG("Worker {} is saturated (queueNum > {}, "
                             "queueTime > {} us)",
                             ip,
                             workerQueueNumThreshold,
                             workerQueueTimeThresholdUs);
            }
        }

        int workersNum = results.size();
        runtimeMetricsHistory[currentTime].workersNum = workersNum;

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
        }

        while (!runtimeMetricsHistory.empty() &&
               runtimeMetricsHistory.begin()->first < cutoffTime) {
            runtimeMetricsHistory.erase(runtimeMetricsHistory.begin());
        }
    }

    // Per-worker capability snapshot.
    // isSaturated requires both signals to be true simultaneously:
    //   1. avgQueueNum > queueThreshold  (queue consistently backed up)
    //   2. totalThroughput < totalInputCount  (processing less than arriving)
    struct WorkerCapability
    {
        int totalInputCount = 0;   // requests arrived in window
        double throughput = 0.0;   // requests processed in window
        double chainedRatio = 0.0; // chained calls per processed request
        int distinctChainedDests = 0;
        double avgQueueNum = 0.0; // avg queue length across instances
        bool isSaturated = false;
    };

    // Saturation = queue is consistently backed up AND worker is falling
    // behind. queueThreshold: minimum sustained avgQueueNum to count as "backed
    // up".
    std::map<std::string, WorkerCapability> computeWorkerCapabilities(
      int windowSec,
      double queueThreshold = 1.0) const
    {
        faabric::util::SharedLock lock(opMx);

        std::map<std::string, WorkerCapability> result;
        time_t nowSec = faabric::util::getGlobalClock().epochSeconds();
        time_t startSec = nowSec - windowSec;

        for (const auto& [ip, workerNode] : clusterWorkerMetrics) {
            WorkerCapability cap;

            // --- Per-instance aggregation ---
            int totalChainedCalls = 0;
            std::set<std::string> chainedDests;
            double queueNumSum = 0.0;
            int queueNumCount = 0;

            for (const auto& [instanceName, timeSeries] :
                 workerNode.instances) {
                for (const auto& [ts, rec] : timeSeries.history) {
                    if (ts < startSec)
                        continue;

                    cap.totalInputCount += rec.inputCount;
                    cap.throughput += rec.throughput;

                    for (const auto& [dest, count] : rec.chainedCallHistory) {
                        totalChainedCalls += count;
                        if (!dest.empty()) {
                            chainedDests.insert(dest);
                        }
                    }

                    if (rec.workerQueueNum.count > 0) {
                        queueNumSum += rec.workerQueueNum.average;
                        queueNumCount++;
                    }
                }
            }

            cap.chainedRatio =
              cap.throughput > 0
                ? static_cast<double>(totalChainedCalls) / cap.throughput
                : 0.0;
            cap.distinctChainedDests = static_cast<int>(chainedDests.size());
            cap.avgQueueNum =
              queueNumCount > 0 ? queueNumSum / queueNumCount : 0.0;

            // Both conditions must hold: sustained queue backlog AND
            // throughput not keeping up with arrivals.
            cap.isSaturated = (cap.avgQueueNum > queueThreshold) &&
                              (cap.throughput < cap.totalInputCount);

            result[ip] = cap;
        }
        return result;
    }

    struct ScalingSignals
    {
        double avgInputRate = 0.0; // req/s, averaged over windowSec seconds
        double avgQueueSize = 0.0; // worker-side queue length, averaged
        int plannerWaitingQueueSize = 0; // current planner waiting queue length
        int64_t plannerWaitingQueueAgeMicros =
          0; // age of oldest waiting msg (us)
        bool plannerQueueSaturated =
          false; // size > threshold AND age > threshold
    };

    ScalingSignals getScalingSignals(int windowSec) const
    {
        faabric::util::SharedLock lock(opMx);
        int64_t nowSec = faabric::util::getGlobalClock().epochSeconds();
        int totalInput = 0, totalQueue = 0, validSecs = 0;

        for (int64_t s = nowSec - windowSec; s < nowSec; s++) {
            auto it = runtimeMetricsHistory.find(s);
            if (it == runtimeMetricsHistory.end()) {
                continue;
            }
            totalInput += it->second.inputRate.load(std::memory_order_relaxed);
            for (const auto& [ip, q] : it->second.hostQueueSize) {
                totalQueue += q;
            }
            validSecs++;
        }

        ScalingSignals sig;
        if (validSecs > 0) {
            sig.avgInputRate = (double)totalInput / validSecs;
            sig.avgQueueSize = (double)totalQueue / validSecs;
        }

        // Latest planner waiting queue snapshot — saturation pre-computed in
        // recordQueueSnapshot, just copy the stored result here.
        if (!runtimeMetricsHistory.empty()) {
            const auto& latest = runtimeMetricsHistory.rbegin()->second;
            sig.plannerWaitingQueueSize = latest.waitingQueueSize;
            sig.plannerWaitingQueueAgeMicros = latest.waitingQueueAgeMicros;
            sig.plannerQueueSaturated = latest.plannerQueueSaturated;
        }
        return sig;
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
    }

  private:
    // Name is User_Func_Par
    mutable std::shared_mutex opMx;
    const std::string appName;
    int period;
    std::map<std::string, std::unique_ptr<InstanceMetrics>> instances;
    // Recorded Metrics
    std::map<int, int> latenciesOverTen; // Latencies in milliseconds
    std::map<int, int>
      latenciesUnderTen; // Latencies under 10ms, recorded in microseconds
    std::map<int, int> totalLatenciesOverTen; // Latencies in milliseconds
    std::map<int, int>
      totalLatenciesUnderTen; // Latencies under 10ms, recorded in microseconds

    long count = 0;
    int64_t startTime = std::numeric_limits<int64_t>::max();
    int64_t endTime = std::numeric_limits<int64_t>::min();
    double avgExecutionOperators = 0.0;
    double avgRunningReqs = 0.0;
    // std::map<int64_t, int> runtimeCountHistory;

    // Online RLS coefficient estimator (alpha, beta in CPU-budget model).
    CoeffEstimator coeffEstimator;

    // thresholds for worker-side saturation detection
    int workerQueueNumThreshold = 500;
    int64_t workerQueueTimeThresholdUs = 100000; // microseconds

    // thresholds for planner-level waiting queue saturation
    int plannerWaitingQueueSizeThreshold = 5000;
    int64_t plannerWaitingQueueAgeThresholdUs = 500000; // 1 second in us

    // Internal helper — caller must already hold opMx (shared or exclusive).
    // Returns true if any instance at the latest recorded second is saturated.
    bool isWorkerSaturatedUnlocked(const std::string& ip) const
    {
        auto workerIt = clusterWorkerMetrics.find(ip);
        if (workerIt == clusterWorkerMetrics.end()) {
            return false;
        }
        const auto& workerNode = workerIt->second;

        time_t latestTs = 0;
        for (const auto& [instanceName, timeSeries] : workerNode.instances) {
            if (!timeSeries.history.empty()) {
                latestTs =
                  std::max(latestTs, timeSeries.history.rbegin()->first);
            }
        }
        if (latestTs == 0) {
            return false;
        }

        for (const auto& [instanceName, timeSeries] : workerNode.instances) {
            auto it = timeSeries.history.find(latestTs);
            if (it != timeSeries.history.end() && it->second.isSaturated) {
                return true;
            }
        }
        return false;
    }

    LatencyHistogramConfig histConfig;

    std::map<int64_t, RuntimeMetricsRecord> runtimeMetricsHistory;

    // MAP<source operator : <successor operator, count>>
    std::map<std::string, std::map<std::string, int>> edgeWeightMap;

    // MAP<timestamp_seconds, version_id>
    std::map<int64_t, int> versionHistory;

    // --------- Metrics Related to Workers ---------
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

/* This helper struct encapsulates the internal state of the planner
 */
struct PlannerState
{
    // Accounting of the hosts that are registered in the system and responsive
    // We deliberately use the host's IP as unique key, but assign a unique host
    // id for redundancy
    std::map<std::string, std::shared_ptr<Host>> hostMap;

    faabric::batch_scheduler::HostMap activeHosts;

    // It is used to lock modification for inFlightApps and appResults.
    std::shared_mutex reqStatusMx;

    // MAP<appId, set<msg_id>> Map of inflight requests.
    std::unordered_map<int, std::set<int32_t>> inFlightApps;

    std::unordered_map<int, int64_t> appStartTimes;

    // Double-map holding the message results. The first key is the app id. For
    // each app id, we keep a map of the message id, and the actual message
    // result
    std::unordered_map<int,
                       std::map<int32_t, std::shared_ptr<faabric::Message>>>
      appResults;

    // Map holding the hosts that have registered interest in getting an app
    // result
    std::map<int, std::vector<std::string>> appResultWaiters;

    // Map keeping track of the requests that are in-flight
    faabric::batch_scheduler::InFlightReqs inFlightReqs;

    // Map keeping track of pre-loaded scheduling decisions that bypass the
    // planner's scheduling
    std::map<int, std::shared_ptr<batch_scheduler::SchedulingDecision>>
      preloadedSchedulingDecisions;

    // Helper coutner of the total number of migrations
    std::atomic<int> numMigrations = 0;

    std::shared_mutex scheduledMsgsMapMx;
    std::map<std::string, std::list<std::unique_ptr<Message>>> scheduledMsgsMap;

    // Metrics
    std::unique_ptr<ApplicationMetrics> applicationMetrics;

    PlannerState()
      : applicationMetrics(
          std::make_unique<ApplicationMetrics>("defaultApp", 1))
    {}
};
}
