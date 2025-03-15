#include <faabric/state/InMemoryStateKeyValue.h>
#include <faabric/state/RedisStateKeyValue.h>
#include <faabric/state/State.h>
#include <faabric/util/config.h>
#include <faabric/util/locks.h>
#include <faabric/util/logging.h>
#include <faabric/util/serialization.h>
#include <faabric/util/state.h>

#include <unistd.h>

using namespace faabric::util;

#define CHECK_USER_FUNC(user, func)                                            \
    if (user.empty() || func.empty()) {                                        \
        throw std::runtime_error(fmt::format(                                  \
          "Attempting to access state with empty user or func ({}/{})",        \
          user,                                                                \
          func));                                                              \
    }

namespace faabric::state {
State& getGlobalState()
{
    static State s(faabric::util::getSystemConfig().endpointHost);
    return s;
}

State::State(std::string thisIPIn)
  : thisIP(thisIPIn)
{}

void State::forceClearAll(bool global)
{
    std::string stateMode = faabric::util::getSystemConfig().stateMode;
    if (stateMode == "redis") {
        RedisStateKeyValue::clearAll(global);
    } else if (stateMode == "inmemory") {
        InMemoryStateKeyValue::clearAll(global);
    } else {
        throw std::runtime_error("Unrecognised state mode: " + stateMode);
    }
    {
        faabric::util::SharedLock sharedLock(mapMutex);
        kvMap.clear();
    }
    {
        faabric::util::FullLock lock(fsmapMutex);
        fsMap.clear();
    }
}

size_t State::getStateSize(const std::string& user, const std::string& keyIn)
{
    if (user.empty()) {
        throw std::runtime_error("Attempting to access state with empty user");
    }

    std::string lookupKey = faabric::util::keyForUser(user, keyIn);

    // See if we have the value locally
    {
        faabric::util::SharedLock sharedLock(mapMutex);
        if (kvMap.count(lookupKey) > 0) {
            return kvMap[lookupKey]->size();
        }
    }

    // Full lock
    FullLock fullLock(mapMutex);

    // Double check
    if (kvMap.count(lookupKey) > 0) {
        return kvMap[lookupKey]->size();
    }

    // Get from remote
    // TODO - cache this?
    std::string stateMode = faabric::util::getSystemConfig().stateMode;
    if (stateMode == "redis") {
        return RedisStateKeyValue::getStateSizeFromRemote(user, keyIn);
    } else if (stateMode == "inmemory") {
        return InMemoryStateKeyValue::getStateSizeFromRemote(
          user, keyIn, thisIP);
    } else {
        throw std::runtime_error("Unrecognised state mode: " + stateMode);
    }
}

void State::deleteKV(const std::string& userIn, const std::string& keyIn)
{
    std::string stateMode = faabric::util::getSystemConfig().stateMode;
    if (stateMode == "redis") {
        RedisStateKeyValue::deleteFromRemote(userIn, keyIn);
    } else if (stateMode == "inmemory") {
        InMemoryStateKeyValue::deleteFromRemote(userIn, keyIn, thisIP);
    } else {
        throw std::runtime_error("Unrecognised state mode: " + stateMode);
    }

    deleteKVLocally(userIn, keyIn);
}

void State::deleteKVLocally(const std::string& userIn, const std::string& keyIn)
{
    FullLock fullLock(mapMutex);
    std::string lookupKey = faabric::util::keyForUser(userIn, keyIn);
    kvMap.erase(lookupKey);
}

std::shared_ptr<StateKeyValue> State::getKV(const std::string& user,
                                            const std::string& key)
{
    return doGetKV(user, key, true, 0);
}

std::shared_ptr<StateKeyValue> State::getKV(const std::string& user,
                                            const std::string& key,
                                            size_t size)
{
    return doGetKV(user, key, false, size);
}

std::shared_ptr<StateKeyValue> State::doGetKV(const std::string& user,
                                              const std::string& key,
                                              bool sizeless,
                                              size_t size)
{
    if (user.empty() || key.empty()) {
        throw std::runtime_error(fmt::format(
          "Attempting to access state with empty user or key ({}/{})",
          user,
          key));
    }

    std::string lookupKey = faabric::util::keyForUser(user, key);

    // See if we have locally
    {
        SharedLock sharedLock(mapMutex);
        if (kvMap.count(lookupKey) > 0) {
            return kvMap[lookupKey];
        }
    }

    // Full lock
    FullLock fullLock(mapMutex);

    // Double check condition
    if (kvMap.count(lookupKey) > 0) {
        return kvMap[lookupKey];
    }

    // Sanity check on size if not sizeless
    if (!sizeless && size == 0) {
        throw StateKeyValueException(
          "Must specify size for creating key-value " + lookupKey);
    }

    // Create new KV
    std::string stateMode = faabric::util::getSystemConfig().stateMode;
    if (stateMode == "redis") {
        if (sizeless) {
            auto kv = std::make_shared<RedisStateKeyValue>(user, key);
            kvMap.emplace(lookupKey, std::move(kv));
        } else {
            auto kv = std::make_shared<RedisStateKeyValue>(user, key, size);
            kvMap.emplace(lookupKey, std::move(kv));
        }
    } else if (stateMode == "inmemory") {
        // Passing IP here is crucial for testing
        if (sizeless) {
            auto kv =
              std::make_shared<InMemoryStateKeyValue>(user, key, thisIP);
            kvMap.emplace(lookupKey, std::move(kv));
        } else {
            auto kv =
              std::make_shared<InMemoryStateKeyValue>(user, key, size, thisIP);
            kvMap.emplace(lookupKey, std::move(kv));
        }
    } else {
        throw std::runtime_error("Unrecognised state mode: " + stateMode);
    }

    return kvMap[lookupKey];
}

size_t State::getKVCount()
{
    faabric::util::SharedLock lock(mapMutex);
    return kvMap.size();
}

std::string State::getThisIP()
{
    return thisIP;
}

size_t State::getFunctionStateSize(const std::string& user,
                                   const std::string& func,
                                   int32_t parallelismId,
                                   bool lock)
{

    auto targetFs = doGetFS(user, func, parallelismId);

    if (lock) {
        targetFs->lockWrite();
    }
    return targetFs->size();
}

int State::readFuncState(const std::string& user,
                         const std::string& func,
                         int32_t parallelismId,
                         char* buffer)
{
    auto targetFs = doGetFS(user, func, parallelismId);

    targetFs->get(reinterpret_cast<uint8_t*>(buffer));
    return targetFs->size();
}

void State::setFuncState(const std::string& user,
                         const std::string& func,
                         int32_t parallelismId,
                         char* buffer,
                         int32_t bufferLen,
                         bool unlock)
{
    auto targetFs = doGetFS(user, func, parallelismId);

    targetFs->set(reinterpret_cast<uint8_t*>(buffer), bufferLen, unlock);
}

int State::getIndivFuncStateSizeLock(const std::string& user,
                                     const std::string& func,
                                     int32_t parallelismId,
                                     uint8_t* buffer,
                                     std::set<std::string>& keys,
                                     int acquireTimes)
{
    auto targetFs = doGetFS(user, func, parallelismId);

    int size = targetFs->acquireIndivLocks(keys, buffer, acquireTimes);
    return size;
}

void State::readIndivFuncState(const std::string& user,
                               const std::string& func,
                               int32_t parallelismId,
                               char* buffer,
                               int bufferLength,
                               std::set<std::string>& keys)
{
    auto targetFs = doGetFS(user, func, parallelismId);

    auto stateVec = targetFs->readPartitionState(keys);
    // Copy it to the buffer
    size_t stateLength = stateVec.size();
    if (stateLength != bufferLength) {
        SPDLOG_ERROR("Buffer length {} does not match state length {}",
                     bufferLength,
                     stateLength);
        throw std::runtime_error("Buffer length does not match state length");
    }
    std::copy(stateVec.data(), stateVec.data() + stateLength, buffer);
}

void State::writeIndivFuncStateUnlock(const std::string& user,
                                      const std::string& func,
                                      int32_t parallelismId,
                                      std::vector<uint8_t>& data)
{
    auto targetFs = doGetFS(user, func, parallelismId);

    targetFs->writePartitionStateUnlocks(data);
}

std::shared_ptr<FunctionState> State::getFS(const std::string& user,
                                            const std::string& func,
                                            int32_t parallelismId)
{
    return doGetFS(user, func, parallelismId);
}

std::shared_ptr<FunctionState> State::doGetFS(const std::string& user,
                                              const std::string& func,
                                              int32_t parallelismId)
{
    CHECK_USER_FUNC(user, func);

    std::string lookupKey =
      faabric::util::keyForFunction(user, func, parallelismId);

    // See if we have locally
    SharedLock sharedLock(fsmapMutex);
    if (fsMap.count(lookupKey) > 0 && fsMap[lookupKey] != nullptr) {
        return fsMap[lookupKey];
    }

    SPDLOG_ERROR("Function state {} not found locally", lookupKey);
    throw std::runtime_error("Function state not found locally");
}

std::shared_ptr<FunctionState> State::createFS(const std::string& user,
                                               const std::string& func,
                                               int32_t parallelismId,
                                               const bool partitionable)
{
    CHECK_USER_FUNC(user, func);

    std::string lookupKey =
      faabric::util::keyForFunction(user, func, parallelismId);
    SPDLOG_INFO(
      "State::createFS: Creating function state {} for {}", lookupKey, thisIP);

    // If we have it locally, delete it and create new.
    FullLock fullLock(fsmapMutex);
    if (fsMap.count(lookupKey) > 0) {
        fsMap.erase(lookupKey);
    }

    auto fs = std::make_shared<FunctionState>(user, func, parallelismId);
    if (partitionable) {
        SPDLOG_INFO(
          "State::createFS: Setting partition key {} is partitionable",
          lookupKey);
        fs->isPartitioned();
    }
    fsMap.emplace(lookupKey, std::move(fs));
    return fsMap[lookupKey];
}

void State::deleteFS(const std::string& user,
                     const std::string& func,
                     int32_t parallelismId)
{
    CHECK_USER_FUNC(user, func);

    std::string lookupKey =
      faabric::util::keyForFunction(user, func, parallelismId);

    FullLock fullLock(fsmapMutex);
    if (fsMap.count(lookupKey) > 0) {
        fsMap.erase(lookupKey);
    } else {
        SPDLOG_WARN("Function state {} not found for deletion", lookupKey);
    }
}

}
