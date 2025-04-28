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

    void syncStatesInfo(
      const std::map<std::string, faabric::batch_scheduler::FunctionStateInfo>&
        statesInfo);

    void resetScheduler() override;

  private:
    std::string localHost;
};

}