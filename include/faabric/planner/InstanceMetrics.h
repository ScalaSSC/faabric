#pragma once

#include <faabric/proto/faabric.pb.h>
#include <faabric/util/locks.h>
#include <faabric/util/logging.h>

#include <map>
#include <memory>
#include <rapidjson/document.h>
#include <rapidjson/stringbuffer.h>
#include <rapidjson/writer.h>
#include <shared_mutex>
#include <string>

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

}
