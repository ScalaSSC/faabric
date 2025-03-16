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

    void syncStatesInfo(
      const std::map<std::string, faabric::batch_scheduler::FunctionStateInfo>&
        statesInfo);

    std::string scheduleMessage(const HostMap& hostMap,
                                const std::unique_ptr<Message>& msg) override;

    std::string scheduleMessageMode1(const HostMap& hostMap,
                                     const std::unique_ptr<Message>& msg);

    void setScheduleMode(int mode);

  private:
    std::string localHost;

    int scheduleMode = 0;
};

}