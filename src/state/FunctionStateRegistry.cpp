#include <faabric/redis/Redis.h>
#include <faabric/state/FunctionState.h>
#include <faabric/state/FunctionStateRegistry.h>
#include <faabric/util/bytes.h>
#include <faabric/util/locks.h>
#include <faabric/util/logging.h>
#include <faabric/util/state.h>

#include <vector>

#define MAIN_KEY_PREFIX "main_"

namespace faabric::state {
FunctionStateRegistry& getFunctionStateRegistry()
{
    static FunctionStateRegistry reg;
    return reg;
}

static std::string getMasterKey(const std::string& user,
                                const std::string& func,
                                size_t parallelismId)
{
    std::string mainKey =
      MAIN_KEY_PREFIX + user + "_" + func + "_" + std::to_string(parallelismId);
    return mainKey;
}

std::string FunctionStateRegistry::getMasterIP(const std::string& user,
                                               const std::string& func,
                                               int parallelismId)
{
    // Query Redis
    const std::string mainKey = getMasterKey(user, func, parallelismId);
    SPDLOG_DEBUG("Checking Master IP for state {}", mainKey);
    redis::Redis& redis = redis::Redis::getState();
    std::vector<uint8_t> mainIPBytes = redis.get(mainKey);

    if (mainIPBytes.empty()) {
        // No main found and not claiming
        SPDLOG_ERROR("No main found for {}", mainKey);
        throw FunctionStateException("Found no main for state " + mainKey);
    }

    // Cache the result locally
    std::string mainIP = faabric::util::bytesToString(mainIPBytes);

    return mainIP;
}

void FunctionStateRegistry::clear()
{
    faabric::util::FullLock lock(mainMapMutex);
    mainMap.clear();
}

}
