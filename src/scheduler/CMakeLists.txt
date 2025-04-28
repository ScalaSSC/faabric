faabric_lib(scheduler
    FunctionCallClient.cpp
    FunctionCallServer.cpp
    Scheduler.cpp
    InstancesLoadState.cpp
    InstancesRuntimeStats.cpp
)

# FIXME: do we need all these deps here?
target_link_libraries(scheduler PRIVATE
    faabric::scheduling_util
    faabric::snapshot
    faabric::state
    faabric::redis
)
