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
    GetRuntimeStats = 9,
    GetPersistentState = 10,
    SetPersistentState = 11,
    RegisterApplication = 12,
    Custom = 13,
    GetWorkerStats = 14,
};
}
