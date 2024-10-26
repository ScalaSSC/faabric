#pragma once

namespace faabric::scheduler {
enum FunctionCalls
{
    NoFunctionCall = 0,
    ExecuteFunctions = 1,
    Flush = 2,
    SetMessageResult = 3,
    ExecuteFunctionsLazy = 4,
    ResetBatchsize = 5,
    ResetMaxReplicas = 6,
    ResetParameter = 7,
    ExecuteFunctionsBatch = 8,
};
}
