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
                             const std::string& hostIpIn,
                             size_t stateSizeIn)
  : user(userIn)
  , function(functionIn)
  , parallelismId(parallelismIdIn)
  , sem(1)
  , stateSize(stateSizeIn)
  , stateRegistry(getFunctionStateRegistry())
  , hostIp(hostIpIn)
  , masterIp(stateRegistry.getMasterIP(userIn,
                                       functionIn,
                                       parallelismIdIn,
                                       hostIpIn,
                                       true))
{
    SPDLOG_TRACE("Creating function state for {}/{} with size {} (this "
                 "parallelisimId: {})",
                 user,
                 function,
                 stateSize,
                 parallelismId);
    if (hostIp == masterIp) {
        isMaster = true;
    }
    // If stateSizeIn is not set, the configure has to be called later.
    if (stateSizeIn > 0) {
        configureSize();
    }
}

FunctionState::FunctionState(const std::string& userIn,
                             const std::string& functionIn,
                             int parallelismIdIn,
                             const std::string& hostIpIn)
  : FunctionState(userIn, functionIn, parallelismIdIn, hostIpIn, 0)
{}

long FunctionState::lockWrite()
{
    auto startTime = faabric::util::getGlobalClock().epochMicros();
    SPDLOG_TRACE("Waiting Locking write for {}: {}/{}-{}",
                 hostIp,
                 user,
                 function,
                 parallelismId);
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
    SPDLOG_TRACE("Unlocking write for {}: {}/{}-{}",
                 hostIp,
                 user,
                 function,
                 parallelismId);
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

void FunctionState::checkSizeConfigured()
{
    if (stateSize <= 0) {
        throw FunctionStateException(
          fmt::format("{}/{} has no size set", user, function));
    }
}

void FunctionState::configureSize()
{
    // Work out size of required shared memory
    size_t nHostPages = getRequiredHostPages(stateSize);
    sharedMemSize = nHostPages * HOST_PAGE_SIZE;
    sharedMemory = nullptr;
}

size_t FunctionState::size() const
{
    return stateSize;
}

void FunctionState::setPartitionKey(std::string key)
{
    partitionKey = key;
}

void FunctionState::clearAll(bool global)
{
    FunctionStateRegistry& reg = state::getFunctionStateRegistry();
    reg.clear();
}

uint32_t FunctionState::waitOnRedisRemoteLockFunc(const std::string& redisKey)
{
    PROF_START(remoteLock)

    redis::Redis& redis = redis::Redis::getState();
    uint32_t remoteLockId =
      redis.acquireLock(redisKey, REMOTE_LOCK_TIMEOUT_SECS);
    unsigned int retryCount = 0;
    while (remoteLockId == 0) {
        SPDLOG_DEBUG(
          "Waiting on remote lock for {} (loop {})", redisKey, retryCount);

        if (retryCount >= REMOTE_LOCK_MAX_RETRIES) {
            SPDLOG_ERROR("Timed out waiting for lock on {}", redisKey);
            break;
        }

        SLEEP_MS(500);

        remoteLockId = redis.acquireLock(redisKey, REMOTE_LOCK_TIMEOUT_SECS);
        retryCount++;
    }

    PROF_END(remoteLock)
    return remoteLockId;
}

void FunctionState::set(const uint8_t* buffer)
{
    checkSizeConfigured();

    doSet(buffer);
}

void FunctionState::reSize(long length)
{
    stateSize = length;
    checkSizeConfigured();
    // If new length is bigger than the reserved size, reallocate it.
    if (length > sharedMemSize) {
        fullyAllocated = false;
        SPDLOG_DEBUG("The new length is bigger than the reserved size, "
                     "reallocate it.");
        // If the sharedMemory is created, but not initialized, the shared
        // memory is null. In FunctionState, sizeless intialize is not
        // allowed.
        if (sharedMemory != nullptr) {
            if (munmap(sharedMemory, sharedMemSize) == -1) {
                SPDLOG_ERROR("Failed to unmap shared memory: {}",
                             strerror(errno));
                throw std::runtime_error("Failed unmapping memory for FS");
            }
            sharedMemory = nullptr;
        }
        configureSize();
    }
    allocateChunk(0, sharedMemSize);
}

void FunctionState::set(const uint8_t* buffer, long length, bool unlock)
{
    faabric::util::FullLock lock(funcStateMutex);

    reSize(length);
    doSet(buffer);
    if (!isMaster) {
        pushToRemote(unlock);
        return;
    }
    if (isMaster && unlock) {
        unlockWrite();
    }
}

void FunctionState::reserveStorage()
{
    checkSizeConfigured();

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

void FunctionState::allocateChunk(long offset, size_t length)
{
    // Can skip if the whole thing is already allocated
    if (fullyAllocated) {
        return;
    }

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

    // Flag if we've now allocated the whole value
    if (offset == 0 && length == sharedMemSize) {
        fullyAllocated = true;
    }
}

std::vector<StateChunk> FunctionState::getAllChunks()
{
    // Divide the whole value up into chunks
    auto nChunks = uint32_t((stateSize + STATE_STREAMING_CHUNK_SIZE - 1) /
                            STATE_STREAMING_CHUNK_SIZE);

    std::vector<StateChunk> chunks;
    for (uint32_t i = 0; i < nChunks; i++) {
        uint32_t previousChunkEnd = i * STATE_STREAMING_CHUNK_SIZE;
        uint8_t* chunkStart = BYTES(sharedMemory) + previousChunkEnd;
        size_t chunkSize = std::min((size_t)STATE_STREAMING_CHUNK_SIZE,
                                    stateSize - previousChunkEnd);
        chunks.emplace_back(previousChunkEnd, chunkSize, chunkStart);
    }

    return chunks;
}

void FunctionState::doSet(const uint8_t* buffer)
{
    checkSizeConfigured();

    // Set up storage
    allocateChunk(0, sharedMemSize);

    // Copy data into shared region
    std::copy(buffer, buffer + stateSize, BYTES(sharedMemory));
}

void FunctionState::pushToRemote(bool unlock)
{
    if (isMaster) {
        return;
    }
    std::vector<StateChunk> allChunks = getAllChunks();
    FunctionStateClient cli(user, function, parallelismId, masterIp);
    cli.pushChunks(allChunks, stateSize, unlock, partitionKey);
}

// Only the Master node can return its data, otherwise pull at first.
void FunctionState::get(uint8_t* buffer)
{
    faabric::util::FullLock lock(funcStateMutex);
    doPull();

    auto bytePtr = BYTES(sharedMemory);
    std::copy(bytePtr, bytePtr + stateSize, buffer);
}

uint8_t* FunctionState::get()
{
    doPull();
    return BYTES(sharedMemory);
}

void FunctionState::pull()
{
    doPull();
}

// In our function, doPull is called to retrive the data from the master
// node.
void FunctionState::doPull()
{
    if (isMaster) {
        return;
    }
    checkSizeConfigured();
    size_t updatedSize =
      getStateSizeFromRemote(user, function, parallelismId, hostIp);
    // Make sure storage is allocated
    reSize(updatedSize);
    // Do the pull
    pullFromRemote();
}

void FunctionState::pullFromRemote()
{
    if (isMaster) {
        return;
    }
    std::vector<StateChunk> chunks = getAllChunks();
    FunctionStateClient cli(user, function, parallelismId, masterIp);
    cli.pullChunks(chunks, BYTES(sharedMemory));
}

void FunctionState::mapSharedMemory(void* destination,
                                    long pagesOffset,
                                    long nPages)
{
    checkSizeConfigured();

    PROF_START(mapSharedMem)

    if (!isPageAligned(destination)) {
        SPDLOG_ERROR("Non-aligned destination for shared mapping of {}",
                     function);
        throw std::runtime_error("Mapping misaligned shared memory");
    }
    // We don't have to lock there, it it locked when read the State Size.

    // Ensure the underlying memory is allocated
    size_t offset = pagesOffset * faabric::util::HOST_PAGE_SIZE;
    size_t length = nPages * faabric::util::HOST_PAGE_SIZE;
    allocateChunk(offset, length);

    // Add a mapping of the relevant pages of shared memory onto the new
    // region
    void* result = mremap(BYTES(sharedMemory) + offset,
                          0,
                          length,
                          MREMAP_FIXED | MREMAP_MAYMOVE,
                          destination);

    // Handle failure
    if (result == MAP_FAILED) {
        SPDLOG_ERROR("Failed mapping for {} at {} with size {}. errno: {} ({})",
                     function,
                     offset,
                     length,
                     errno,
                     strerror(errno));

        throw std::runtime_error("Failed mapping shared memory");
    }

    // Check the mapping is where we expect it to be
    if (destination != result) {
        SPDLOG_ERROR("New mapped addr for {} doesn't match required {} != {}",
                     function,
                     destination,
                     result);
        throw std::runtime_error("Misaligned shared memory mapping");
    }

    PROF_END(mapSharedMem)
}

void FunctionState::unmapSharedMemory(void* mappedAddr)
{
    if (!isPageAligned(mappedAddr)) {
        SPDLOG_ERROR("Attempting to unmap non-page-aligned memory at {} for {}",
                     mappedAddr,
                     function);
        throw std::runtime_error("Unmapping misaligned shared memory");
    }

    // Unmap the current memory so it can be reused
    int result = munmap(mappedAddr, sharedMemSize);
    if (result == -1) {
        SPDLOG_ERROR(
          "Failed to unmap shared memory at {} with size {}. errno: {}",
          mappedAddr,
          sharedMemSize,
          errno);

        throw std::runtime_error("Failed unmapping shared memory");
    }
}

void FunctionState::setChunk(long offset, const uint8_t* buffer, size_t length)
{
    checkSizeConfigured();

    // Check we're in bounds - note that we permit chunks within the
    // _allocated_ memory
    size_t chunkEnd = offset + length;
    if (chunkEnd > sharedMemSize) {
        SPDLOG_ERROR("Setting chunk out of bounds on {}/{}-{} ({} > {})",
                     user,
                     function,
                     parallelismId,
                     chunkEnd,
                     stateSize);
        throw std::runtime_error("Attempting to set chunk out of bounds");
    }

    // If necessary, allocate the memory
    allocateChunk(offset, length);

    // Do the copy if necessary
    if (buffer != nullptr) {
        std::copy(buffer, buffer + length, BYTES(sharedMemory) + offset);
    }
}

uint8_t* FunctionState::getChunk(long offset, long len)
{
    if (!isMaster) {
        throw FunctionStateException("Only master can get chunks");
    }
    if (offset < 0 || len < 0 || offset + len > sharedMemSize) {
        throw FunctionStateException("Requested chunk is out of bounds");
    }
    return BYTES(sharedMemory) + offset;
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

int FunctionState::readPartitionStateSize(std::set<std::string>& keys)
{
    return readPartitionState(keys).size();
}

int FunctionState::acquireIndivLocks(std::set<std::string>& keys,
                                     uint8_t* buffer,
                                     int acquireTimes)
{

    // Get the thread ID
    std::map<std::string, std::vector<uint8_t>> filteredMap;

    std::string acquiredKeysStr;
    auto acquiredKeys = multiKeyLock.tryAcquire(keys);
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

void FunctionState::writeIndivStateUnlocks(std::vector<uint8_t>& states)
{
    faabric::util::FullLock lock(funcStateMutex);

    std::set<std::string> keys;
    auto stateMap = faabric::util::deserializeParState(states);
    for (auto& [key, value] : stateMap) {
        indivStateMap.at(key).setState(value);
        keys.insert(key);
    }
    // Get the mx and unlock
    multiKeyLock.release(keys);
}

size_t FunctionState::getStateSize()
{
    sem.acquire();
    size_t thisStateSize = stateSize;
    sem.release();
    return thisStateSize;
}

// --------------------------------------------
// Static properties and methods
// --------------------------------------------

size_t FunctionState::getStateSizeFromRemote(const std::string& userIn,
                                             const std::string& funcIn,
                                             int parallelismIdIn,
                                             const std::string& thisIP,
                                             bool lock)
{
    std::string mainIP;
    try {
        // If this Function State is not created by other workers, return 0.
        mainIP = getFunctionStateRegistry().getMasterIPForOtherMaster(
          userIn, funcIn, parallelismIdIn, thisIP);
    } catch (FunctionStateException& ex) {
        return 0;
    }
    FunctionStateClient stateClient(userIn, funcIn, parallelismIdIn, mainIP);
    size_t stateSize = stateClient.stateSize(lock);
    return stateSize;
}
}