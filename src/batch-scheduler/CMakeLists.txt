faabric_lib(scheduling_util
    DecisionCache.cpp
    SchedulingDecision.cpp
    Application.cpp
    WorkersLoadState.cpp
    RuntimeSummary.cpp
)

faabric_lib(batch_scheduler
    BatchScheduler.cpp
    BinPackScheduler.cpp
    StateAwareScheduler.cpp
    DecentralizedScheduler.cpp
)

target_link_libraries(batch_scheduler PRIVATE
    faabric::util
    faabric::scheduling_util
)
