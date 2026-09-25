#pragma once

#include <faabric/util/config.h>
#include <faabric/util/exception.h>

#include <hiredis/hiredis.h>
#include <map>
#include <memory>
#include <mutex>
#include <set>
#include <string>
#include <string_view>
#include <thread>
#include <unordered_set>
#include <vector>

namespace faabric::redis {
enum RedisRole
{
    QUEUE,
    STATE,
};

class RedisInstance
{
  public:
    explicit RedisInstance(RedisRole role);

    std::string delifeqSha;
    std::string schedPublishSha;
    std::string claimSha;
    std::string transferOwnersSha;

    std::string ip;
    std::string hostname;
    int port;

  private:
    RedisRole role;

    std::mutex scriptsLock;

    std::string loadScript(redisContext* context,
                           const std::string_view scriptBody);

    // Script to delete a key if it equals a given value
    const std::string_view delifeqCmd = R"---(
if redis.call('GET', KEYS[1]) == ARGV[1] then
    return redis.call('DEL', KEYS[1])
else
    return 0
end
)---";

    // Script to push and expire function execution results avoiding extra
    // copies and round-trips
    const std::string_view schedPublishCmd = R"---(
local key = KEYS[1]
local status_key = KEYS[2]
local result = ARGV[1]
local result_expiry = tonumber(ARGV[2])
local status_expiry = tonumber(ARGV[3])
redis.call('RPUSH', key, result)
redis.call('EXPIRE', key, result_expiry)
redis.call('SET', status_key, result)
redis.call('EXPIRE', status_key, status_expiry)
return 0
)---";

    // Script to claim ownership of a key. Sets it to our proposed value if
    // nobody holds it yet, and returns the current owner either way, so a
    // caller that loses the race still learns where the thing lives.
    const std::string_view claimCmd = R"---(
if redis.call('SETNX', KEYS[1], ARGV[1]) == 1 then
    return ARGV[1]
end
return redis.call('GET', KEYS[1])
)---";

    // Script to hand ownership of several keys from one holder to another,
    // all or nothing. KEYS come in (owner, epoch) pairs; ARGV[1] is the
    // current holder, ARGV[2] the new one, and ARGV[2 + i] the epoch pair i
    // must still be at. Moves every owner and bumps every epoch only if all
    // pairs pass both checks. Returns 1 if it moved them, 0 otherwise.
    const std::string_view transferOwnersCmd = R"---(
local n = #KEYS / 2
for i = 1, n do
    if redis.call('GET', KEYS[2 * i - 1]) ~= ARGV[1] then
        return 0
    end
    local epoch = tonumber(redis.call('GET', KEYS[2 * i]) or '0')
    if epoch ~= tonumber(ARGV[2 + i]) then
        return 0
    end
end
for i = 1, n do
    redis.call('SET', KEYS[2 * i - 1], ARGV[2])
    redis.call('SET', KEYS[2 * i], tonumber(ARGV[2 + i]) + 1)
end
return 1
)---";
};

using UniqueRedisReply =
  std::unique_ptr<redisReply, decltype(&freeReplyObject)>;

class Redis
{

  public:
    ~Redis();

    /**
     *  ------ Factories ------
     */

    static Redis& getQueue();

    static Redis& getState();

    /**
     *  ------ Standard Redis commands ------
     */
    void ping();

    std::vector<uint8_t> get(const std::string& key);

    size_t strlen(const std::string& key);

    void get(const std::string& key, uint8_t* buffer, size_t size);

    void set(const std::string& key, const std::vector<uint8_t>& value);

    void set(const std::string& key, const uint8_t* value, size_t size);

    void del(const std::string& key);

    long getCounter(const std::string& key);

    long incr(const std::string& key);

    long decr(const std::string& key);

    long incrByLong(const std::string& key, long val);

    long decrByLong(const std::string& key, long val);

    void setRange(const std::string& key,
                  long offset,
                  const uint8_t* value,
                  size_t size);

    void setRangePipeline(const std::string& key,
                          long offset,
                          const uint8_t* value,
                          size_t size);

    void flushPipeline(long pipelineLength);

    void getRange(const std::string& key,
                  uint8_t* buffer,
                  size_t bufferLen,
                  long start,
                  long end);

    void sadd(const std::string& key, const std::string& value);

    void srem(const std::string& key, const std::string& value);

    long scard(const std::string& key);

    bool sismember(const std::string& key, const std::string& value);

    std::string srandmember(const std::string& key);

    std::set<std::string> smembers(const std::string& key);

    std::set<std::string> sdiff(const std::string& keyA,
                                const std::string& keyB);

    std::set<std::string> sinter(const std::string& keyA,
                                 const std::string& keyB);

    int lpushLong(const std::string& key, long val);

    int rpushLong(const std::string& key, long val);

    void flushAll();

    long listLength(const std::string& queueName);

    long getTtl(const std::string& key);

    void expire(const std::string& key, long expiry);

    void refresh();

    /**
     *  ------ Locking ------
     */

    uint32_t acquireLock(const std::string& key, int expirySeconds);

    void acquireLockBlocking(const std::string& lockKey);

    void releaseLock(const std::string& key, uint32_t lockId);

    void delIfEq(const std::string& key, uint32_t value);

    bool setnxex(const std::string& key, long value, int expirySeconds);

    /**
     * Atomically claims `key` for `proposedValue` if it is unset, and returns
     * the value that ends up stored. A caller whose claim loses the race gets
     * back the winner's value rather than an error.
     */
    std::string claimOrGet(const std::string& key,
                           const std::string& proposedValue);

    struct OwnerTransfer
    {
        std::string ownerKey;
        std::string epochKey;
        // A missing epoch key reads as 0.
        int64_t expectedEpoch;
    };

    /**
     * Atomically moves every `ownerKey` in `transfers` from `fromValue` to
     * `toValue` and bumps each epoch by one -- provided `fromValue` still
     * holds all of them and every epoch still reads its expected value. All
     * or nothing: returns false and changes nothing if any check fails.
     */
    bool transferOwners(const std::vector<OwnerTransfer>& transfers,
                        const std::string& fromValue,
                        const std::string& toValue);

    std::vector<uint8_t> getAndLock(const std::string& key);

    void setAndUnlock(const std::string& key, const std::vector<uint8_t>& value);

    // Non-blocking multi-key variants: keys already locked by someone else
    // are silently skipped rather than waited on.
    std::map<std::string, std::vector<uint8_t>> getAndLockMulti(
      const std::set<std::string>& keys);

    void setAndUnlockMulti(
      const std::map<std::string, std::vector<uint8_t>>& values);

    long getLong(const std::string& key);

    void setLong(const std::string& key, long value);

    /**
     * ------ Queueing ------
     */
    void enqueue(const std::string& queueName, const std::string& value);

    void enqueueBytes(const std::string& queueName,
                      const std::vector<uint8_t>& value);

    void enqueueBytes(const std::string& queueName,
                      const uint8_t* buffer,
                      size_t bufferLen);

    std::string dequeue(const std::string& queueName,
                        int timeout = DEFAULT_TIMEOUT);

    std::vector<uint8_t> dequeueBytes(const std::string& queueName,
                                      int timeout = DEFAULT_TIMEOUT);

    size_t dequeueBytes(const std::string& queueName,
                        uint8_t* buffer,
                        size_t bufferLen,
                        int timeout = DEFAULT_TIMEOUT);

    void dequeueMultiple(const std::string& queueName,
                         uint8_t* buff,
                         long buffLen,
                         long nElems);

    // Scheduler result publish
    void publishSchedulerResult(const std::string& key,
                                const std::string& status_key,
                                const std::vector<uint8_t>& result);

  private:
    explicit Redis(const RedisInstance& instance);

    redisContext* context;

    const RedisInstance& instance;

    std::map<std::string, uint32_t> lockOwners;

    UniqueRedisReply dequeueBase(const std::string& queueName, int timeout);
};

class RedisNoResponseException : public faabric::util::FaabricException
{
  public:
    explicit RedisNoResponseException(std::string message)
      : FaabricException(std::move(message))
    {}
};
};
