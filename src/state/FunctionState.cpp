#include <faabric/state/FunctionState.h>
#include <faabric/state/FunctionStateClient.h>
#include <faabric/util/hash.h>
#include <faabric/util/locks.h>
#include <faabric/util/logging.h>
#include <faabric/util/macros.h>
#include <faabric/util/memory.h>
#include <faabric/util/serialization.h>
#include <faabric/util/string_tools.h>
#include <faabric/util/timing.h>
#include <sys/mman.h>

using namespace faabric::util;

namespace faabric::state {

const std::vector<uint8_t>& IndivState::getState() const
{
    std::lock_guard<std::mutex> guard(stateMutex);
    return state;
}

void IndivState::setState(const std::vector<uint8_t>& newState)
{
    std::lock_guard<std::mutex> guard(stateMutex);
    state = newState;
}

FunctionState::FunctionState(const std::string& userIn,
                             const std::string& functionIn,
                             int parallelismIdIn,
                             size_t stateSizeIn)
  : user(userIn)
  , function(functionIn)
  , parallelismId(parallelismIdIn)
  , sem(1)
  , stateSize(stateSizeIn)
  , stateRegistry(getFunctionStateRegistry())
{
    SPDLOG_TRACE("Creating function state for {}/{} with size {} (this "
                 "parallelisimId: {})",
                 user,
                 function,
                 stateSize,
                 parallelismId);

    std::string mainIP =
      stateRegistry.getMasterIP(user, function, parallelismId);
    if (mainIP != faabric::util::getSystemConfig().endpointHost) {
        SPDLOG_ERROR("Function state for {}/{}-{} is not allocated this host",
                     user,
                     function,
                     parallelismId);
        throw std::runtime_error("Function state is not allocated this host");
    }

    // If stateSizeIn is not set, the configure has to be called later.
    if (stateSizeIn > 0) {
        configureSize();
    }
}

FunctionState::FunctionState(const std::string& userIn,
                             const std::string& functionIn,
                             int parallelismIdIn)
  : FunctionState(userIn, functionIn, parallelismIdIn, 0)
{}

void FunctionState::isPartitioned()
{
    partition = true;
}

long FunctionState::lockWrite()
{
    auto startTime = faabric::util::getGlobalClock().epochMicros();
    sem.acquire();
    auto endTime = faabric::util::getGlobalClock().epochMicros();
    tempLockAquireTime = endTime;
    int timeDiff = static_cast<int>(endTime - startTime);
    // Record the locking congestion time
    SPDLOG_TRACE("Gain Lock and Lock Congestion time for {}/{}-{} is {} µs",
                 user,
                 function,
                 parallelismId,
                 timeDiff);
    metrics.lockBlockTimeQueue.add(timeDiff);
    return timeDiff;
}

void FunctionState::unlockWrite()
{
    long long releaseTime = faabric::util::getGlobalClock().epochMicros();
    int timeDiff = static_cast<int>(releaseTime - tempLockAquireTime);
    // Record the holding time of the lock
    SPDLOG_TRACE("Write lock holding time for {}/{}-{} is {} µs",
                 user,
                 function,
                 parallelismId,
                 timeDiff);
    metrics.lockHoldTimeQueue.add(timeDiff);
    sem.release();
}

// Now, we don't lock when retrieving the size of the function state. Because
// only the thread locked the function state can get the size of the function
// state.
size_t FunctionState::size() const
{
    return stateSize;
}

void FunctionState::allocateChunk(long offset, size_t length)
{
    // Ensure storage is reserved
    reserveStorage();

    // Page-align the chunk
    AlignedChunk chunk = getPageAlignedChunk(offset, length);

    // Make sure all the pages involved are writable
    int res = mprotect(
      BYTES(sharedMemory) + chunk.nBytesOffset, chunk.nBytesLength, PROT_WRITE);
    if (res != 0) {
        SPDLOG_DEBUG(
          "Allocating memory for {}/{}-{} of size {} failed: {} ({})",
          user,
          function,
          parallelismId,
          length,
          errno,
          strerror(errno));
        throw std::runtime_error("Failed allocating memory for KV");
    }
}

void FunctionState::configureSize()
{
    // Work out size of required shared memory
    size_t nHostPages = getRequiredHostPages(stateSize);
    sharedMemSize = nHostPages * HOST_PAGE_SIZE;
    sharedMemory = nullptr;
}

void FunctionState::reSize(long length)
{
    stateSize = length;

    // If new length is bigger than the reserved size, reallocate it.
    if (length < sharedMemSize) {
        return;
    }

    SPDLOG_DEBUG("Resizing function state for {}/{}-{} from {} to {} bytes",
                 user,
                 function,
                 parallelismId,
                 sharedMemSize,
                 length);

    // If the sharedMemory is created, but not initialized, the shared
    // memory is null.
    if (sharedMemory != nullptr) {
        if (munmap(sharedMemory, sharedMemSize) == -1) {
            SPDLOG_ERROR("Failed to unmap shared memory: {}", strerror(errno));
            throw std::runtime_error("Failed unmapping memory for FS");
        }
        sharedMemory = nullptr;
    }
    configureSize();

    allocateChunk(0, sharedMemSize);
}

void FunctionState::set(const uint8_t* buffer, long length, bool unlock)
{
    faabric::util::FullLock lock(funcStateMutex);

    reSize(length);
    doSet(buffer);

    if (unlock) {
        unlockWrite();
    }
}

void FunctionState::reserveStorage()
{
    // Check if already reserved
    if (sharedMemory != nullptr) {
        return;
    }

    PROF_START(reserveStorage)

    if (sharedMemSize == 0) {
        throw FunctionStateException("Reserving storage with no size for " +
                                     function);
    }

    // Create shared memory region with no permissions
    sharedMemory = mmap(
      nullptr, sharedMemSize, PROT_NONE, MAP_SHARED | MAP_ANONYMOUS, -1, 0);
    if (sharedMemory == MAP_FAILED) {
        SPDLOG_DEBUG("Mmapping of storage size {} failed. errno: {}",
                     sharedMemSize,
                     errno);

        throw std::runtime_error("Failed mapping memory for FS");
    }

    SPDLOG_DEBUG("Reserved {} pages of shared storage for {}",
                 sharedMemSize / HOST_PAGE_SIZE,
                 function);

    PROF_END(reserveStorage)
}

void FunctionState::doSet(const uint8_t* buffer)
{
    // Set up storage
    allocateChunk(0, sharedMemSize);

    // Copy data into shared region
    std::copy(buffer, buffer + stateSize, BYTES(sharedMemory));
}

void FunctionState::doSet(const std::string& data)
{
    // Set up storage
    allocateChunk(0, sharedMemSize);

    // Copy data into the shared memory region
    std::copy(data.data(), data.data() + data.size(), BYTES(sharedMemory));
}

// Only the Master node can return its data, otherwise pull at first.
void FunctionState::get(uint8_t* buffer)
{
    faabric::util::FullLock lock(funcStateMutex);
    auto bytePtr = BYTES(sharedMemory);
    std::copy(bytePtr, bytePtr + stateSize, buffer);
}

int FunctionState::readPartitionStateSize(std::set<std::string>& keys)
{
    return readPartitionState(keys).size();
}

std::vector<uint8_t> FunctionState::readPartitionState(
  std::set<std::string>& keys)
{
    faabric::util::FullLock lock(funcStateMutex);

    std::map<std::string, std::vector<uint8_t>> filteredMap;
    for (const auto& key : keys) {
        if (!indivStateMap.contains(key)) {
            SPDLOG_ERROR("Key {} is not found when reading", key);
            throw std::runtime_error("Key is not found when reading");
        }
        auto tempState = indivStateMap.at(key).getState();
        if (!tempState.empty()) {
            filteredMap.emplace(key, tempState);
        }
    }
    auto stateVec = faabric::util::serializeParState(filteredMap);
    return stateVec;
}

int FunctionState::acquireIndivLocks(std::set<std::string>& keys,
                                     uint8_t* buffer,
                                     int acquireTimes)
{

    // Get the thread ID
    std::map<std::string, std::vector<uint8_t>> filteredMap;

    std::string acquiredKeysStr;
    auto acquiredKeys = multiKeysLock.tryAcquire(keys);
    faabric::util::FullLock lock(funcStateMutex);
    for (const auto& key : acquiredKeys) {
        if (!acquiredKeysStr.empty()) {
            acquiredKeysStr += "|";
        }
        acquiredKeysStr += key;
        if (!indivStateMap[key].getState().empty()) {
            filteredMap.emplace(key, indivStateMap.at(key).getState());
        }
    }

    std::vector<uint8_t> acquiredKeysVec(acquiredKeysStr.begin(),
                                         acquiredKeysStr.end());
    acquiredKeysVec.push_back('\0');

    // Step 2: Create the vector and copy the locked data into it
    std::copy(acquiredKeysVec.data(),
              acquiredKeysVec.data() + acquiredKeysVec.size(),
              reinterpret_cast<uint8_t*>(buffer));

    // Step3: Calculate the Vec Size
    auto stateVec = faabric::util::serializeParState(filteredMap);
    return stateVec.size();
}

std::map<std::string, std::vector<uint8_t>>
FunctionState::readPartitionStateLock(std::set<std::string>& keys)
{
    // Get the thread ID
    std::map<std::string, std::vector<uint8_t>> filteredMap;

    std::string acquiredKeysStr;
    auto acquiredKeys = multiKeysLock.tryAcquire(keys);
    faabric::util::FullLock lock(funcStateMutex);
    for (const auto& key : acquiredKeys) {
        auto indivState = indivStateMap[key].getState();
        filteredMap.emplace(key, indivState);
    }
    return filteredMap;
}

void FunctionState::writePartitionStateUnlocks(std::vector<uint8_t>& states)
{
    faabric::util::FullLock lock(funcStateMutex);

    std::set<std::string> keys;
    auto stateMap = faabric::util::deserializeParState(states);
    for (auto& [key, value] : stateMap) {
        indivStateMap.at(key).setState(value);
        keys.insert(key);
    }
    // Get the mx and unlock
    multiKeysLock.release(keys);
}

std::map<int, std::string> FunctionState::scheduleParState(
  const std::shared_ptr<faabric::util::ConsistentHashRing>& hashRing,
  const std::map<int, std::string>& stateHost)
{
    faabric::util::FullLock lock(funcStateMutex);
    if (!partition) {
        auto bytePtr = BYTES(sharedMemory);
        std::string result(reinterpret_cast<const char*>(bytePtr), stateSize);
        return { { 0, result } };
    }
    // MAP<ParallelismIdx, <key, value>>
    std::map<int, std::map<std::string, std::vector<uint8_t>>>
      rescheduleStatesMap;
    // Calculation the new location of the state.
    for (const auto& [key, state] : indivStateMap) {
        std::vector<uint8_t> keyVec = faabric::util::stringToBytes(key);
        auto hashAndNode = hashRing->getHashAndNode(keyVec);
        int paraIdx = hashAndNode.second;
        rescheduleStatesMap[paraIdx].emplace(key, state.getState());
    }

    // MAP<parallelismIdx, serialized <key, value>>
    std::map<int, std::string> returnMap;
    for (const auto& [paraIdx, state] : rescheduleStatesMap) {
        // Look up the corresponding IP address from stateHost
        if (!stateHost.contains(paraIdx)) {
            SPDLOG_ERROR(
              "Cannot find the IP address for the parallelism {} in {}",
              paraIdx,
              getUserFunc());
            throw std::runtime_error("Cannot find the IP address for the "
                                     "parallelism");
        }
        // Insert or merge the state for this IP.
        auto serializedState = faabric::util::serializeParStateMap(state);
        returnMap[paraIdx] = std::move(serializedState);
    }

    return returnMap;
}

void FunctionState::addMigrateState(const std::string& serializedState)
{
    faabric::util::FullLock lock(funcStateMutex);

    if (!partition) {
        // update statesize
        reSize(serializedState.size());
        doSet(serializedState);
        return;
    }
    auto migrateState = faabric::util::deserializeParStateMap(serializedState);
    for (const auto& [key, value] : migrateState) {
        indivStateMap[key].setState(value);
    }
}

}