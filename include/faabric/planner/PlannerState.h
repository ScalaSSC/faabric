#pragma once

#include <faabric/batch-scheduler/BatchScheduler.h>
#include <faabric/batch-scheduler/SchedulingDecision.h>
#include <faabric/planner/planner.pb.h>
#include <faabric/proto/faabric.pb.h>
#include <faabric/util/queue.h>

#include <cstdint>
#include <limits>
#include <list>
#include <map>
#include <rapidjson/document.h>
#include <rapidjson/prettywriter.h>
#include <rapidjson/stringbuffer.h>
#include <rapidjson/writer.h>
#include <shared_mutex>

namespace faabric::planner {

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
        int newWorkerQueueTime = msg->workerqueuewaittime();
        int newExecutorPrepTime = msg->executorpreparetime();
        int newWorkerExecuteTime =
          msg->workerexecuteend() - msg->workerexecutestart();
        int newExecuteBatchSize = msg->executebatchsize();

        count++;

        avgPlannerQueueTime =
          avgPlannerQueueTime +
          (static_cast<double>(newPlannerQueueTime) - avgPlannerQueueTime) /
            static_cast<double>(count);
        avgPlannerConsumeTime =
          avgPlannerConsumeTime +
          (static_cast<double>(newPlannerConsumeTime) - avgPlannerConsumeTime) /
            static_cast<double>(count);
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
        doc.AddMember("avgPlannerScheduleTime", avgPlannerQueueTime, alloc);
        doc.AddMember("avgPlannerDispatchTime", avgPlannerConsumeTime, alloc);
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

        avgPlannerQueueTime = 0.0;
        avgPlannerConsumeTime = 0.0;
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

    double avgPlannerQueueTime = 0.0;
    double avgPlannerConsumeTime = 0.0;
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
    ApplicationMetrics(std::string appNameIn, int periodIn)
      : appName(appNameIn)
      , period(periodIn) {};

    void record(const std::map<int, std::shared_ptr<faabric::Message>>& msgMap)
    {
        if (msgMap.empty()) {
            return;
        }

        faabric::util::FullLock lock(opMx);
        int64_t tempStartTime = std::numeric_limits<int64_t>::max();
        int64_t tempEndTime = std::numeric_limits<int64_t>::min();
        for (auto& [msgId, msg] : msgMap) {
            std::string instanceName = getName(msg);
            if (!instances.contains(instanceName)) {
                instances[instanceName] =
                  std::make_unique<InstanceMetrics>(instanceName, period);
            }
            instances[instanceName]->record(msg);
            if (msg->plannerqueuetime() < tempStartTime) {
                tempStartTime = msg->plannerqueuetime();
            }
            if (msg->workerexecuteend() > tempEndTime) {
                tempEndTime = msg->workerexecuteend();
            }
        }
        count++;
        int tempLatency = (tempEndTime - tempStartTime) / 1000;
        latencies[tempLatency]++;
        if (tempStartTime < startTime) {
            startTime = tempStartTime;
        }
        if (tempEndTime > endTime) {
            endTime = tempEndTime;
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

    std::string getMetrics() const
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
        doc.AddMember("startTime", startTime, alloc);
        doc.AddMember("endTime", endTime, alloc);

        // Calculate throughput as messages per second.
        double throughput = 0.0;
        if (endTime > startTime) {
            // Assuming startTime and endTime are in Microseconds.
            throughput = static_cast<double>(count) * 1e6 /
                         static_cast<double>(endTime - startTime);
        }
        doc.AddMember("throughput", throughput, alloc);

        // Compute percentiles from the latencies histogram.
        long totalCount = 0;
        for (const auto& [latency, freq] : latencies) {
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

            for (const auto& [latency, freq] : latencies) {
                cumCount += freq;
                if (medianLatency == 0 && cumCount >= medianThreshold) {
                    medianLatency = latency;
                }
                if (p95Latency == 0 && cumCount >= p95Threshold) {
                    p95Latency = latency;
                }
                if (p99Latency == 0 && cumCount >= p99Threshold) {
                    p99Latency = latency;
                    // Once we have p99, we can break out early.
                    break;
                }
            }
        }

        // Add computed percentiles to the JSON document.
        doc.AddMember("medianLatency", medianLatency, alloc);
        doc.AddMember("p95Latency", p95Latency, alloc);
        doc.AddMember("p99Latency", p99Latency, alloc);

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

        // Write out the JSON document to a string.
        rapidjson::StringBuffer buffer;
        rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);
        doc.Accept(writer);
        return buffer.GetString();
    }

    void reset()
    {
        faabric::util::FullLock lock(opMx);

        count = 0;
        startTime = std::numeric_limits<int64_t>::max();
        endTime = std::numeric_limits<int64_t>::min();
        latencies.clear();

        for (auto& [instName, instancePtr] : instances) {
            instancePtr->reset();
        }
    }

  private:
    // Name is User_Func_Par
    mutable std::shared_mutex opMx;
    const std::string appName;
    int period;
    std::map<std::string, std::unique_ptr<InstanceMetrics>> instances;
    // Recorded Metrics
    std::map<int, int> latencies;
    long count = 0;
    int64_t startTime = std::numeric_limits<int64_t>::max();
    int64_t endTime = std::numeric_limits<int64_t>::min();

    std::string getName(const std::shared_ptr<faabric::Message> msg)
    {
        return msg->user() + "_" + msg->function() + "_" +
               std::to_string(msg->parallelismid());
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

    faabric::batch_scheduler::HostMap batchSchedHostMap;

    // It is used to lock modification for inFlightApps and appResults.
    std::shared_mutex reqStatusMx;

    // MAP<appId, in_flight_counting> Map of inflight requests.
    std::map<int, int> inFlightApps;

    // Double-map holding the message results. The first key is the app id. For
    // each app id, we keep a map of the message id, and the actual message
    // result
    std::map<int, std::map<int, std::shared_ptr<faabric::Message>>> appResults;

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
    std::shared_mutex metricsMx;
    std::unique_ptr<ApplicationMetrics> applicationMetrics;

    PlannerState()
      : applicationMetrics(
          std::make_unique<ApplicationMetrics>("defaultApp", 1))
    {}
};
}
