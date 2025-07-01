#pragma once

#include <faabric/batch-scheduler/StateAwareScheduler.h>

namespace faabric::batch_scheduler {

class DecentralizedScheduler final : public StateAwareScheduler
{
  public:
    DecentralizedScheduler()
    {
        localHost = faabric::util::getSystemConfig().endpointHost;
    }

    virtual ~DecentralizedScheduler() = default;

    void resetScheduler() override;

    void setScheuduledOperatorMap(
      const std::map<std::string, ScheduledOperator>& scheuduledOperatorMapIn);

    void syncStatesInfo(
      const std::map<std::string, faabric::batch_scheduler::FunctionStateInfo>&
        statesInfo);

  private:
    std::string localHost;
};

}