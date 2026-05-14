#include <faabric/planner/PlannerClient.h>
#include <faabric/scheduler/FunctionCallClient.h>
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

void State::updateHosts(faabric::batch_scheduler::HostMap hostMap)
{
    hosts = hostMap;
}

std::map<std::string, std::map<std::string, std::vector<uint8_t>>>
State::redirectState(
  const std::map<std::string, HashRingPtr>& hashRings,
  const std::map<std::string, faabric::batch_scheduler::FunctionStateInfo>&
    statesInfo)
{
    // state migration map : MAP <IP, <UserFuncPar, serialized states>>
    std::map<std::string, std::map<std::string, std::vector<uint8_t>>>
      migrationStateMap;
    for (const auto& [preFuncName, state] : fsMap) {
        std::string userFunc = state->getUserFunc();
        if (!statesInfo.contains(userFunc)) {
            SPDLOG_ERROR("Function {} not found in states info", userFunc);
            throw std::runtime_error("Function not found in states info");
        }
        const auto& info = statesInfo.at(userFunc);
        std::map<int, std::vector<uint8_t>> migratedState;
        if (state->getIsPartitioned()) {
            if (!hashRings.contains(userFunc)) {
                SPDLOG_ERROR("Hash ring not found for function {}", userFunc);
                throw std::runtime_error("Hash ring not found for function");
            }
            HashRingPtr hashRing = hashRings.at(userFunc);
            auto migratedState = state->redirectLocalParState(hashRing);
        } else {
            auto migratedState = state->redirectLocalState();
        }
        for (const auto& [parIdx, serializedState] : migratedState) {
            std::string ip = info.stateHost.at(parIdx);
            std::string userFuncPar = userFunc + "_" + std::to_string(parIdx);
            migrationStateMap[ip].emplace(userFuncPar,
                                          std::move(serializedState));
        }
    }

    std::ostringstream oss;
    oss << "MigrationStatesMap contents: \n";
    for (const auto& [ip, innerMap] : migrationStateMap) {
        oss << "IP: " << ip << " | Keys: ";
        for (const auto& [userFuncPar, serializedState] : innerMap) {
            oss << userFuncPar << " ";
        }
        oss << "; \n";
    }
    SPDLOG_INFO("{}", oss.str());

    return migrationStateMap;
}

void State::loadMigrateState(
  const std::multimap<std::string, std::vector<uint8_t>>& migrateStates)
{
    faabric::util::FullLock lock(fsmapMutex);

    for (const auto& [userFuncPar, serializedState] : migrateStates) {
        if (!fsMap.contains(userFuncPar)) {
            SPDLOG_ERROR("Function state {} not found for migration",
                         userFuncPar);
            throw std::runtime_error("Function state not found for migration");
        }
        fsMap.at(userFuncPar)->addMigrateState(serializedState);
    }
}

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
        faabric::util::FullLock lock(mapMutex);
        kvMap.clear();
    }
    {
        faabric::util::FullLock lock(fsmapMutex);
        fsMap.clear();
    }
}

void State::clearFS()
{
    faabric::util::FullLock lock(fsmapMutex);
    fsMap.clear();
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

// int State::readFuncState(const std::string& user,
//                          const std::string& func,
//                          int32_t parallelismId,
//                          char* buffer)
// {
//     auto targetFs = doGetFS(user, func, parallelismId);

//     targetFs->get(reinterpret_cast<uint8_t*>(buffer));
//     return targetFs->size();
// }

std::vector<uint8_t> State::readFuncStateLock(const std::string& user,
                                              const std::string& func,
                                              int32_t parallelismId,
                                              bool lock)
{
    auto targetFs = doGetFS(user, func, parallelismId);

    // Lock the state and read it
    std::vector<uint8_t> stateVec = targetFs->getFuncStateLock(lock);
    return stateVec;
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

std::map<std::string, std::vector<uint8_t>> State::readIndivFuncStateLock(
  const std::string& user,
  const std::string& func,
  int32_t parallelismId,
  std::set<std::string>& keys)
{
    auto targetFs = doGetFS(user, func, parallelismId);

    return targetFs->readPartitionStateLock(keys);
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

void State::resetPersistentLockState()
{
    // Release the semaphore if it is currently held, so that the next
    // enable of persistentLock starts from a clean state (semaphore=1).
    if (persistentStateLockHeld.exchange(false)) {
        persistentStateSem.release();
    }
}

std::string State::readPersistentState(const std::string& key)
{
    if (persistentLock) {
        persistentStateSem.acquire();
        persistentStateLockHeld = true;
    }
    std::string value = persistentState.read(key);
    return value;
}

std::vector<std::string> State::readPersistentStateBatch(
  const std::vector<std::string>& keys)
{
    if (persistentLock) {
        persistentStateSem.acquire();
        persistentStateLockHeld = true;
    }
    std::vector<std::string> values = persistentState.readBatch(keys);
    return values;
}

std::string State::readPersistentStateRemote(const std::string& key)
{
    SPDLOG_DEBUG("Reading persistent state key {} from planner", key);
    return faabric::planner::getPlannerClient().getPersistentStateFromWorker(
      key);
}

void State::writePersistentState(std::string& key, std::string& value)
{
    persistentState.write(key, value);
    if (persistentLock && persistentStateLockHeld.exchange(false)) {
        persistentStateSem.release();
    }
}

void State::writePersistentStateBatch(
  const std::map<std::string, std::string>& data)
{
    persistentState.writeBatch(data);
    if (persistentLock && persistentStateLockHeld.exchange(false)) {
        persistentStateSem.release();
    }
}

void State::writePersistentStateRemote(std::string& key, std::string& value)
{
    persistentState.write(key, value);

    SPDLOG_DEBUG("Writing persistent state key {} to planner", key);
    auto req = std::make_shared<faabric::planner::MapMessage>();
    req->mutable_payload()->insert({ key, value });
    faabric::planner::getPlannerClient().setPersistentStateFromWorker(req);
}

void State::flushState()
{
    // WARNING : This function can cause deadlocks. Be careful when calling it
    // (no running executors).
    FullLock fullLock(fsmapMutex);

    for (const auto& [fs, state] : fsMap) {
        state->flush();
    }
}

}
