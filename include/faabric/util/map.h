#pragma once

#include <faabric/batch-scheduler/BatchScheduler.h>
#include <faabric/util/logging.h>

#include <condition_variable>
#include <iostream>
#include <map>
#include <mutex>   // For std::unique_lock and std::lock
#include <numeric> // For std::accumulate
#include <shared_mutex>
#include <sstream>
#include <utility> // For std::move

namespace faabric::util {

template<typename K, typename V>
// getOrThrow function returns a copy of value.
inline V getOrThrow(const std::map<K, V>& map, const K& key)
{
    auto it = map.find(key);
    if (it == map.end()) {
        std::stringstream ss;
        ss << "Cannot find key '" << key << "' in map";
        // For demonstration without the library, we print to cerr.
        SPDLOG_ERROR(ss.str());

        // Build a comma-separated list of all available keys
        std::string available_keys;
        for (const auto& pair : map) {
            if (!available_keys.empty()) {
                available_keys += ", ";
            }
            available_keys += pair.first;
        }

        SPDLOG_INFO("Available keys in map: [{}]", available_keys);

        throw std::runtime_error("Key not found in map");
    }
    return it->second;
}

inline std::map<std::string, double> calculateProportions(
  const std::map<std::string, int>& data)
{
    // --- 1. Calculate the total sum of all values in the map ---
    // We use std::accumulate for a concise way to sum the values.
    // The initial sum is 0.0 (a double) to ensure floating-point arithmetic.
    double totalSum = std::accumulate(
      data.begin(),
      data.end(),
      0.0,
      [](double currentSum, const std::pair<const std::string, int>& pair) {
          return currentSum + pair.second;
      });

    // --- 2. Create the map to store the results ---
    std::map<std::string, double> proportions;

    // --- 3. Handle the edge case where the total sum is zero ---
    if (totalSum == 0.0) {
        return proportions; // Return an empty map if total sum is zero.
    }

    // --- 4. Calculate the proportion for each entry ---
    for (const auto& pair : data) {
        // The key from the original map.
        const std::string& host = pair.first;
        // The count for that key.
        const int count = pair.second;

        // Calculate the proportion and store it in the new map.
        // We cast the count to double to ensure floating-point division.
        proportions[host] = static_cast<double>(count) / totalSum;
    }

    return proportions;
}

inline faabric::batch_scheduler::HostMap getFirstNElements(
  const faabric::batch_scheduler::HostMap& originalMap,
  size_t count)
{
    // Create a new map to store the filtered results.
    faabric::batch_scheduler::HostMap filteredMap;

    // Get an iterator to the beginning of the original map.
    auto it = originalMap.begin();

    // Loop for 'count' times, or until we reach the end of the map,
    // whichever comes first.
    for (size_t i = 0; i < count && it != originalMap.end(); ++i) {
        // Insert the key-value pair the iterator is pointing to into the new
        // map. C++17's insert_or_assign or C++11's insert can be used. Using
        // `insert` is simple and effective here.
        filteredMap.insert(*it);

        // Move the iterator to the next element.
        ++it;
    }

    return filteredMap;
}

template<typename K, typename V>
inline K getNthKey(const std::map<K, V>& map, std::size_t n)
{
    if (n >= map.size()) {
        throw std::out_of_range("Index out of range");
    }

    auto it = map.begin();
    std::advance(it, n);
    return it->first;
}

template<typename K, typename V>
class ThreadSafeMap
{
  private:
    std::map<K, V> map;
    mutable std::shared_mutex mtx;

  public:
    // Default constructor
    ThreadSafeMap() = default;

    // Delete copy constructor and copy assignment operator
    ThreadSafeMap(const ThreadSafeMap&) = delete;
    ThreadSafeMap& operator=(const ThreadSafeMap&) = delete;

    // Provide move constructor and move assignment operator
    ThreadSafeMap(ThreadSafeMap&& other) noexcept
    {
        std::unique_lock lock(other.mtx);
        map = std::move(other.map);
    }

    ThreadSafeMap& operator=(ThreadSafeMap&& other) noexcept
    {
        if (this != &other) {
            std::unique_lock lock1(mtx, std::defer_lock);
            std::unique_lock lock2(other.mtx, std::defer_lock);
            std::lock(lock1, lock2);
            map = std::move(other.map);
        }
        return *this;
    }

    void insert(const K& key, const V& value)
    {
        std::unique_lock lock(mtx);
        map[key] = value;
    }

    bool get(const K& key, V& value) const
    {
        std::shared_lock lock(mtx);
        auto it = map.find(key);
        if (it != map.end()) {
            value = it->second;
            return true;
        }
        return false;
    }

    void erase(const K& key)
    {
        std::unique_lock lock(mtx);
        map.erase(key);
    }

    bool contains(const K& key) const
    {
        std::shared_lock lock(mtx);
        return map.find(key) != map.end();
    }

    size_t size() const
    {
        std::shared_lock lock(mtx);
        return map.size();
    }

    // Add find method
    typename std::map<K, V>::iterator find(const K& key)
    {
        std::unique_lock lock(mtx);
        return map.find(key);
    }

    typename std::map<K, V>::const_iterator find(const K& key) const
    {
        std::shared_lock lock(mtx);
        return map.find(key);
    }

    // Add end method
    typename std::map<K, V>::iterator end()
    {
        std::unique_lock lock(mtx);
        return map.end();
    }

    typename std::map<K, V>::const_iterator end() const
    {
        std::shared_lock lock(mtx);
        return map.end();
    }

    // Add begin method
    typename std::map<K, V>::iterator begin()
    {
        std::unique_lock lock(mtx);
        return map.begin();
    }

    typename std::map<K, V>::const_iterator begin() const
    {
        std::shared_lock lock(mtx);
        return map.begin();
    }

    // Add operator[] for safe access
    V& operator[](const K& key)
    {
        std::unique_lock lock(mtx);
        return map[key];
    }

    const V& operator[](const K& key) const
    {
        std::shared_lock lock(mtx);
        // Use at() for const access to ensure exception on missing key
        return map.at(key);
    }

    // Add at method for bounds-checked access
    V& at(const K& key)
    {
        std::unique_lock lock(mtx);
        return map.at(key);
    }

    const V& at(const K& key) const
    {
        std::shared_lock lock(mtx);
        return map.at(key);
    }

    // Add clear method to clear the map
    void clear()
    {
        std::unique_lock lock(mtx);
        map.clear();
    }
};

}
