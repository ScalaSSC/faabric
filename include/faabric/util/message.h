#pragma once

#include <faabric/batch-scheduler/Application.h>
#include <faabric/batch-scheduler/RuntimeSummary.h>
#include <faabric/planner/planner.pb.h>
#include <faabric/proto/faabric.pb.h>

namespace faabric::util {

std::unique_ptr<batch_scheduler::Application> parseApplicationMsg(
  faabric::planner::RegisterApplicationRequest& rawReq);

void serializeScheduledOperatorMap(
  const std::shared_ptr<faabric::planner::SyncStatesInfoRequest>& reqPtr,
  const std::map<std::string, faabric::batch_scheduler::ScheduledOperator>&
    scheduledOperatorsMap);

std::map<std::string, faabric::batch_scheduler::ScheduledOperator>
parseScheduledOperatorMap(const faabric::planner::SyncStatesInfoRequest& req);
}
