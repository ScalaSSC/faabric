#pragma once

#include <faabric/util/locks.h>

#include <map>
#include <optional>
#include <shared_mutex>
#include <string>

namespace faabric::state {

// PersistentState: a thread-safe in-memory key-value store with batch
// operations
class PersistentState
{
  public:
    // Store or update a single key-value pair
    bool write(const std::string& key, const std::string& value);

    // Store or update multiple key-value pairs at once
    bool writeBatch(const std::map<std::string, std::string>& data);

    // Retrieve a value by key;
    std::string read(const std::string& key) const;

    std::vector<std::string> readBatch(
      const std::vector<std::string>& keys) const;

    // Remove a key-value pair; returns true if removed
    bool remove(const std::string& key);

    // Clear all entries
    void clear();

  private:
    mutable std::shared_mutex mutex_;
    const std::string emptyString = "None";
    std::unordered_map<std::string, std::string> store_;
};
}