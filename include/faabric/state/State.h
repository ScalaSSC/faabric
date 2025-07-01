#pragma once

#include <faabric/batch-scheduler/StateAwareScheduler.h>
#include <faabric/state/FunctionState.h>
#include <faabric/state/PersistentState.h>
#include <faabric/state/StateKeyValue.h>

#include <shared_mutex>
#include <string>

#define LOCK_BLOCK_TIME "lockBlockTime"
#define LOCK_HOLD_TIME "lockHoldTime"

namespace faabric::state {

// State client-server API
enum StateCalls
{
    NoStateCall = 0,
    Pull = 1,
    Push = 2,
    Size = 3,
    Append = 4,
    ClearAppended = 5,
    PullAppended = 6,
    Delete = 7,
    FunctionSize = 8,
    FunctionPull = 9,
    FunctionPush = 10,
    FunctionRepartition = 11,
    // Add the new partitioned function state data
    FunctionParAdd = 12,
    FunctionParCombine = 13,
    FunctionLock = 14,
    FunctionUnlock = 15,
    FunctionCreate = 16,
    FunctionRuntimeMetrics = 17,
};

class State
{
  public:
    explicit State(std::string thisIPIn);

    size_t getStateSize(const std::string& user, const std::string& keyIn);

    std::shared_ptr<StateKeyValue> getKV(const std::string& user,
                                         const std::string& key,
                                         size_t size);

    std::shared_ptr<StateKeyValue> getKV(const std::string& user,
                                         const std::string& key);

    void backupAll();

    void cleanBackup();

    std::map<std::string, std::multimap<std::string, std::string>>
    schedulePreStates(
      const std::map<std::string,
                     std::shared_ptr<faabric::util::ConsistentHashRing>>&
        hashRings,
      const std::map<std::string, faabric::batch_scheduler::FunctionStateInfo>&
        statesInfo);

    void loadMigrateState(
      const std::multimap<std::string, std::string>& migrateStates);

    void forceClearAll(bool global);

    void deleteKV(const std::string& userIn, const std::string& keyIn);

    void deleteKVLocally(const std::string& userIn, const std::string& keyIn);

    size_t getKVCount();

    //---------------------
    // Function State API
    //---------------------
    size_t getFunctionStateSize(const std::string& user,
                                const std::string& func,
                                int32_t parallelismId,
                                bool lock = false);

    int readFuncState(const std::string& user,
                      const std::string& func,
                      int32_t parallelismId,
                      char* buffer);

    std::vector<uint8_t> readFuncStateLock(const std::string& user,
                                           const std::string& func,
                                           int32_t parallelismId,
                                           bool lock);

    void setFuncState(const std::string& user,
                      const std::string& func,
                      int32_t parallelismId,
                      char* buffer,
                      int32_t bufferLen,
                      bool unlock = false);

    //---------------------
    // Stateful Function State API
    //---------------------

    int getIndivFuncStateSizeLock(const std::string& user,
                                  const std::string& func,
                                  int32_t parallelismId,
                                  uint8_t* buffer,
                                  std::set<std::string>& keys,
                                  int acquireTimes);

    void readIndivFuncState(const std::string& user,
                            const std::string& func,
                            int32_t parallelismId,
                            char* buffer,
                            int bufferLength,
                            std::set<std::string>& keys);

    std::map<std::string, std::vector<uint8_t>> readIndivFuncStateLock(
      const std::string& user,
      const std::string& func,
      int32_t parallelismId,
      std::set<std::string>& keys);

    void writeIndivFuncStateUnlock(const std::string& user,
                                   const std::string& func,
                                   int32_t parallelismId,
                                   std::vector<uint8_t>& data);

    //---------------------
    // Persistent State API
    //---------------------

    std::string readPersistentState(std::string& key);

    std::vector<std::string> readPersistentStateBatch(
      const std::vector<std::string>& keys);

    void writePersistentState(std::string& key, std::string& value);

    void writePersistentStateBatch(
      const std::map<std::string, std::string>& data);

    std::string getThisIP();

    std::shared_ptr<FunctionState> createFS(const std::string& user,
                                            const std::string& func,
                                            int32_t parallelismId,
                                            const bool partitionable);

    std::shared_ptr<FunctionState> getFS(const std::string& user,
                                         const std::string& func,
                                         int32_t parallelismId);

    void deleteFS(const std::string& user,
                  const std::string& func,
                  int32_t parallelismId);

  private:
    const std::string thisIP;

    std::unordered_map<std::string, std::shared_ptr<StateKeyValue>> kvMap;
    std::unordered_map<std::string, std::shared_ptr<FunctionState>> fsMap;
    std::unordered_map<std::string, std::shared_ptr<FunctionState>> backupMap;
    PersistentState persistentState;

    std::shared_mutex mapMutex;
    std::shared_mutex fsmapMutex;
    std::shared_mutex backupMapMutex;

    std::shared_ptr<StateKeyValue> doGetKV(const std::string& user,
                                           const std::string& key,
                                           bool sizeless,
                                           size_t size);

    std::shared_ptr<FunctionState> doGetFS(const std::string& user,
                                           const std::string& func,
                                           int32_t parallelismId);
};

State& getGlobalState();
}
