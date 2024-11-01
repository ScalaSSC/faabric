#pragma once

#include <faabric/endpoint/FaabricEndpoint.h>

#include <climits>

namespace faabric::planner {
class PlannerEndpointHandler final
  : public faabric::endpoint::HttpRequestHandler
  , public std::enable_shared_from_this<PlannerEndpointHandler>
{
  public:
    int maxInflightApps = INT_MAX;
    void onRequest(faabric::endpoint::HttpRequestContext&& ctx,
                   faabric::util::BeastHttpRequest&& request) override;
};
}
