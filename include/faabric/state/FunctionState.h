#pragma once

#include <cstdint>
#include <faabric/state/FunctionStateMetrics.h>
#include <faabric/state/FunctionStateRegistry.h>
#include <faabric/state/StateKeyValue.h>
#include <map>
#include <semaphore>
#include <set>
#include <shared_mutex>
#include <string>
#include <unordered_map>
#include <vector>

namespace faabric::state {

class IndivState
{
  public:
    const std::vector<uint8_t>& getState() const;
    void setState(const std::vector<uint8_t>& newState);

  private:
    std::vector<uint8_t> state;
    mutable std::mutex stateMutex; // Mutex to protect the state vector
};

class FunctionState
{
  public:
    FunctionState(const std::string& userIn,
                  const std::string& functionIn,
                  int parallelismIdIn,
                  const std::string& hostIpIn,
                  size_t stateSizeIn);

    FunctionState(const std::string& userIn,
                  const std::string& functionIn,
                  int parallelismIdIn,
                  const std::string& hostIpIn);

    static size_t getStateSizeFromRemote(const std::string& userIn,
                                         const std::string& funcIn,
                                         int parallelismIdIn,
                                         const std::string& thisIPIn,
                                         bool lock = false);
    /***
     * Functions related to the lock of the function state
     */
    // lock the function state and return the time to acquire the lock (ms)
    long lockWrite();
    void unlockWrite();

    size_t size() const;
    void set(const uint8_t* buffer);
    void set(const uint8_t* buffer, long length, bool unlock = false);
    void setPartitionKey(std::string key);

    void reSize(long length);
    void get(uint8_t* buffer);
    uint8_t* get();
    void pull();

    // Map the sharedMemory to WASM module
    void mapSharedMemory(void* destination, long pagesOffset, long nPages);
    void unmapSharedMemory(void* mappedAddr);

    // setChunk function is only used for mastser to receive updated data
    void setChunk(long offset, const uint8_t* buffer, size_t length);
    // getChunk function is only used for mastser to transfer data
    uint8_t* getChunk(long offset, long len);
    static uint32_t waitOnRedisRemoteLockFunc(const std::string& redisKey);
    static void clearAll(bool global);

    // Local-Tier Parition State
    std::vector<uint8_t> readPartitionState(std::set<std::string>& keys);
    int readPartitionStateSize(std::set<std::string>& keys);

    int acquireIndivLocks(std::set<std::string>& keys,
                          uint8_t* buffer,
                          int acquireTimes);
    void writeIndivStateUnlocks(std::vector<uint8_t>& states);

    const std::string user;
    const std::string function;
    // the default parallelism ID is 0
    int parallelismId;
    bool isMaster = false;

    size_t getStateSize();

  private:
    // In function state, we sue counting_semaphore to lock data. Since mutex
    // must be lock and unlock by the same thread.
    // TODO - the order to gain sem is not guarenteded. How to solve thread
    // starvation.
    std::counting_semaphore<1> sem;

    std::shared_mutex funcStateMutex;

    faabric::util::MultiKeyLock multiKeyLock;
    std::map<std::string, IndivState> indivStateMap;

    size_t stateSize;
    FunctionStateRegistry& stateRegistry;
    // The host IP is the local IP
    const std::string hostIp;
    // The master IP is the IP of the master node
    const std::string masterIp;

    // the key of keyValue with is partition state which is not partition input.
    std::string partitionKey;
    std::atomic<bool> fullyAllocated = false;

    // The shared memory is used to store the state of the function.
    size_t sharedMemSize = 0;
    void* sharedMemory = nullptr;

    long long tempLockAquireTime = 0;

    // std::unordered_map<std::string, std::vector<uint8_t>> state;
    FunctionStateMetrics metrics;
    // Configure the Size of State by using Chunks.
    void checkSizeConfigured();
    void configureSize();
    void reserveStorage();
    void allocateChunk(long offset, size_t length);
    std::vector<StateChunk> getAllChunks();

    void doSet(const uint8_t* data);
    void pushToRemote(bool unlock = false);
    void doPull();
    void pullFromRemote();

};

class FunctionStateException : public std::runtime_error
{
  public:
    explicit FunctionStateException(const std::string& message)
      : runtime_error(message)
    {}
};
}