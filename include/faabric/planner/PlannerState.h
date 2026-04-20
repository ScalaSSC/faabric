#pragma once

#include <faabric/batch-scheduler/BatchScheduler.h>
#include <faabric/batch-scheduler/SchedulingDecision.h>
#include <faabric/planner/ApplicationMetrics.h>
#include <faabric/planner/planner.pb.h>
#include <faabric/proto/faabric.pb.h>
#include <faabric/util/queue.h>

#include <atomic>
#include <list>
#include <map>
#include <memory>
#include <set>
#include <shared_mutex>
#include <unordered_map>

namespace faabric::planner {

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

    // Helper counter of the total number of migrations
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
