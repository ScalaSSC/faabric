#pragma once

#include <faabric/batch-scheduler/StateAwareScheduler.h>
#include <faabric/batch-scheduler/WorkersLoadState.h>

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

    void syncStatesInfo(
      const std::map<std::string, faabric::batch_scheduler::FunctionStateInfo>&
        statesInfo);

    std::string scheduleMessage(const HostMap& hostMap,
                                const std::unique_ptr<Message>& msg) override;

    std::string scheduleMessageMode1(const HostMap& hostMap,
                                     const std::unique_ptr<Message>& msg);

    void setScheduleMode(int mode);

    WorkersLoadState& getWorkersLoadState() { return workersLoadState; }

  private:
    std::string localHost;

    int scheduleMode = 0;

    WorkersLoadState workersLoadState;
};

}