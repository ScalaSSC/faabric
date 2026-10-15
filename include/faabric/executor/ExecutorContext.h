#pragma once

#include <faabric/executor/Executor.h>
#include <faabric/proto/faabric.pb.h>
#include <faabric/util/exception.h>

namespace faabric::executor {

class ExecutorContextException : public faabric::util::FaabricException
{
  public:
    explicit ExecutorContextException(std::string message)
      : FaabricException(std::move(message))
    {}
};

/**
 * Globally-accessible wrapper that allows executing applications to query
 * their execution context. The context is thread-local, so applications can
 * query which specific message they are executing.
 */
class ExecutorContext
{
  public:
    ExecutorContext(Executor* executorIn,
                    std::shared_ptr<faabric::BatchExecuteRequest> reqIn,
                    int msgIdx);

    std::vector<std::string> chainedFunctionName;
    std::vector<int> chainedMsgId;
    // Held as the message's input field is, so it moves straight in.
    std::vector<std::string> chainedInput;

    static bool isSet();

    static void set(Executor* executorIn,
                    std::shared_ptr<faabric::BatchExecuteRequest> reqIn,
                    int msgIdxIn);

    static void unset();

    static std::shared_ptr<ExecutorContext> get();

    Executor* getExecutor() { return executor; }

    std::shared_ptr<faabric::BatchExecuteRequest> getBatchRequest()
    {
        return req;
    }

    faabric::Message& getMsg()
    {
        if (req == nullptr) {
            throw std::runtime_error(
              "Getting message when no request set in context");
        }
        return req->mutable_messages()->at(msgIdx);
    }

    faabric::BatchExecuteRequest& getBatch()
    {
        if (req == nullptr) {
            throw std::runtime_error(
              "Getting batch when no request set in context");
        }
        return *req;
    }

    int getMsgIdx() { return msgIdx; }
    int incrementLockAcquireTimes() { return ++lockAcquireTimes; }

  private:
    Executor* executor = nullptr;
    std::shared_ptr<faabric::BatchExecuteRequest> req = nullptr;
    int msgIdx = 0;
    int lockAcquireTimes = 0;
};
}
