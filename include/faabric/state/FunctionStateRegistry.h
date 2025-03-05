#pragma once

#include <shared_mutex>
#include <string>
#include <unordered_map>

namespace faabric::state {
class FunctionStateRegistry
{
  public:
    FunctionStateRegistry() = default;

    std::string getMasterIP(const std::string& user,
                            const std::string& func,
                            int parallelismId);

    void clear();

  private:
    std::unordered_map<std::string, std::string> mainMap;
    std::shared_mutex mainMapMutex;
};

FunctionStateRegistry& getFunctionStateRegistry();
}
