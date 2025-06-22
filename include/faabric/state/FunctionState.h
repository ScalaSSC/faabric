#pragma once

#include <faabric/state/FunctionStateMetrics.h>
#include <faabric/state/FunctionStateRegistry.h>
#include <faabric/state/StateKeyValue.h>
#include <faabric/util/hash.h>

#include <cstdint>
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
                  size_t stateSizeIn);

    FunctionState(const std::string& userIn,
                  const std::string& functionIn,
                  int parallelismIdIn);

    void isPartitioned();

    /***
     * Functions used by the Stateful Function Operator
     */
    long lockWrite();
    void unlockWrite();
    size_t size() const;
    void get(uint8_t* buffer);
    void set(const uint8_t* buffer, long length, bool unlock = false);

    /***
     * Functions used by the Partitioned Stateful Function Operator
     */
    int acquireIndivLocks(std::set<std::string>& keys,
                          uint8_t* buffer,
                          int acquireTimes);
    int readPartitionStateSize(std::set<std::string>& keys);
    std::vector<uint8_t> readPartitionState(std::set<std::string>& keys);
    std::map<std::string, std::vector<uint8_t>> readPartitionStateLock(
      std::set<std::string>& keys);
    void writePartitionStateUnlocks(std::vector<uint8_t>& states);

    // Reschedule partition states
    // MAP<IP, serialized state>
    std::map<int, std::string> scheduleParState(
      const std::shared_ptr<faabric::util::ConsistentHashRing>& hashRing,
      const std::map<int, std::string>& stateHost);
    void addMigrateState(const std::string& serializedState);

    bool getIsPartitioned() { return partition; }
    std::string getUserFunc() { return user + "_" + function; };

  private:
    std::shared_mutex funcStateMutex;

    // Basic information
    const std::string user;
    const std::string function;
    int parallelismId;
    bool partition = false;

    // ----------------------------------------
    // Function State
    // ----------------------------------------

    // The shared memory is used to store the state of the function.
    size_t sharedMemSize = 0;
    void* sharedMemory = nullptr;

    /* In function state, we sue counting_semaphore to lock data. Since mutex
     * must be lock and unlock by the same thread.
     * TODO - the order to gain sem is not guarenteded. How to solve
     * threadstarvation.*/
    std::counting_semaphore<1> sem;
    size_t stateSize;

    // Configure the Size of State by using Chunks.
    void configureSize();
    // Reserve the storage for the shared memory.
    void reserveStorage();
    // Intialize memory space if needed. Check the sharedmemory is writable.
    void allocateChunk(long offset, size_t length);
    void reSize(long length);
    void doSet(const uint8_t* data);
    void doSet(const std::string& data);

    // ----------------------------------------
    // Partitioned Function State
    // ----------------------------------------

    // The Lock for each indivual partitioned function state.
    faabric::util::MultiKeyLock multiKeysLock;
    std::map<std::string, IndivState> indivStateMap;

    FunctionStateRegistry& stateRegistry;

    long long tempLockAquireTime = 0;

    // ----------------------------------------
    // Metrics
    // ----------------------------------------

    // std::unordered_map<std::string, std::vector<uint8_t>> state;
    FunctionStateMetrics metrics;
};

class FunctionStateException : public std::runtime_error
{
  public:
    explicit FunctionStateException(const std::string& message)
      : runtime_error(message)
    {}
};
}