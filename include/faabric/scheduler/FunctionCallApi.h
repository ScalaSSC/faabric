#pragma once

namespace faabric::scheduler {
enum FunctionCalls
{
    NoFunctionCall = 0,
    ExecuteFunctions = 1,
    Flush = 2,
    SetMessageResult = 3,
    GetWorkerLoad = 4,
    ResetParameter = 5,
    ExecuteFunctionsBatch = 6,
    SyncStatesInfo = 7,
    MigrateStates = 8,
};
}
