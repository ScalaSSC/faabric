#pragma once

#include <faabric/state/StateKeyValue.h>

#include <faabric/redis/Redis.h>
#include <faabric/util/clock.h>
#include <faabric/util/locks.h>

#include <map>
#include <set>
#include <string>
#include <vector>

namespace faabric::state {
class RedisStateKeyValue final : public StateKeyValue
{
  public:
    RedisStateKeyValue(const std::string& userIn,
                       const std::string& keyIn,
                       size_t sizeIn);

    RedisStateKeyValue(const std::string& userIn, const std::string& keyIn);

    static size_t getStateSizeFromRemote(const std::string& userIn,
                                         const std::string& keyIn);

    static void deleteFromRemote(const std::string& userIn,
                                 const std::string& keyIn);

    static void clearAll(bool global);

    static std::map<std::string, std::vector<uint8_t>> readKeysFromRemote(
      const std::string& user,
      const std::string& func,
      int parallelismId,
      const std::set<std::string>& keys);

    static void setKeysToRemote(const std::string& user,
                                const std::string& func,
                                int parallelismId,
                                std::vector<uint8_t>& data);

    static std::vector<uint8_t> readFuncStateFromRemote(
      const std::string& user,
      const std::string& func,
      int parallelismId);

    static void setFuncStateToRemote(const std::string& user,
                                     const std::string& func,
                                     int parallelismId,
                                     const uint8_t* buffer,
                                     size_t bufferLen);

  private:
    const std::string joinedKey;

    void pullFromRemote() override;

    void pullChunkFromRemote(long offset, size_t length) override;

    void pushToRemote() override;

    void pushPartialToRemote(
      const std::vector<StateChunk>& dirtyChunks) override;

    void appendToRemote(const uint8_t* data, size_t length) override;

    void pullAppendedFromRemote(uint8_t* data,
                                size_t length,
                                long nValues) override;

    void clearAppendedFromRemote() override;
};
}
