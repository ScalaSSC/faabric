#include <faabric/state/PersistentState.h>

namespace faabric::state {

bool PersistentState::write(const std::string& key, const std::string& value)
{
    faabric::util::FullLock lock(mutex_);
    store_[key] = value;
    return true;
}

bool PersistentState::writeBatch(const std::map<std::string, std::string>& data)
{
    faabric::util::FullLock lock(mutex_);
    for (const auto& [k, v] : data) {
        store_[k] = v;
    }
    return true;
}

std::string PersistentState::read(const std::string& key) const
{
    faabric::util::SharedLock lock(mutex_);
    auto it = store_.find(key);
    if (it == store_.end()) {
        return "not_found";
    }
    return it->second;
}

std::vector<std::string> PersistentState::readBatch(
  const std::vector<std::string>& keys) const
{
    std::vector<std::string> results;
    results.reserve(keys.size());
    faabric::util::SharedLock lock(mutex_);
    for (const auto& key : keys) {
        auto it = store_.find(key);
        if (it == store_.end()) {
            results.emplace_back("not_found");
        } else {
            results.emplace_back(it->second);
        }
    }
    return results;
}

bool PersistentState::remove(const std::string& key)
{
    faabric::util::FullLock lock(mutex_);
    return store_.erase(key) > 0;
}

void PersistentState::clear()
{
    faabric::util::FullLock lock(mutex_);
    store_.clear();
}

}