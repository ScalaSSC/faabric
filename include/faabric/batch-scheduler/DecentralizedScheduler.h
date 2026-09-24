#pragma once

#include <faabric/batch-scheduler/StateAwareScheduler.h>
#include <faabric/proto/faabric.pb.h>

namespace faabric::batch_scheduler {

class DecentralizedScheduler final : public StateAwareScheduler
{
  public:
    DecentralizedScheduler()
    {
        isplanner = false;
    }

    virtual ~DecentralizedScheduler() = default;

    void resetScheduler() override;

    void setScheuduledOperatorMap(
      const std::map<std::string, ScheduledOperator>& scheuduledOperatorMapIn);

    void syncStatesInfo(
      const std::map<std::string, faabric::batch_scheduler::FunctionStateInfo>&
        statesInfo);

    // Cluster-wide view: the stats the planner fetched from every worker in
    // its previous runtime-stats round, keyed by worker IP. Not cleared by
    // resetScheduler(), as that also runs on every migration.
    void setClusterWorkerStats(
      std::map<std::string, faabric::WorkerStats>&& stats);

    std::map<std::string, faabric::WorkerStats> getClusterWorkerStats();

  private:
    std::shared_mutex clusterWorkerStatsMx;
    std::map<std::string, faabric::WorkerStats> clusterWorkerStats;
};

}