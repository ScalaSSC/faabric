#include <faabric/util/serialization.h>

#include <cstring> // For memcpy
#include <functional>
#include <iomanip> // For std::setw and std::setfill

// THERE MUST BE SAME AS CPP LIMFAASM UTIL SERIALIZATION
namespace faabric::util {

// Helper function for combining hash values
inline void hashCombine(std::size_t& seed, std::size_t value)
{
    seed ^= value + 0x9e3779b9 + (seed << 6) + (seed >> 2);
}

std::size_t murmurhash(const uint8_t* key, std::size_t len, uint32_t seed)
{
    const uint32_t c1 = 0xcc9e2d51;
    const uint32_t c2 = 0x1b873593;
    const uint32_t r1 = 15;
    const uint32_t r2 = 13;
    const uint32_t m = 5;
    const uint32_t n = 0xe6546b64;

    uint32_t hash = seed;

    const int nblocks = len / 4;
    const uint32_t* blocks = (const uint32_t*)(key);
    for (int i = 0; i < nblocks; i++) {
        uint32_t k = blocks[i];
        k *= c1;
        k = (k << r1) | (k >> (32 - r1));
        k *= c2;

        hash ^= k;
        hash = (hash << r2) | (hash >> (32 - r2));
        hash = hash * m + n;
    }

    const uint8_t* tail = (const uint8_t*)(key + nblocks * 4);
    uint32_t k1 = 0;

    switch (len & 3) {
        case 3:
            k1 ^= tail[2] << 16;
        case 2:
            k1 ^= tail[1] << 8;
        case 1:
            k1 ^= tail[0];
            k1 *= c1;
            k1 = (k1 << r1) | (k1 >> (32 - r1));
            k1 *= c2;
            hash ^= k1;
    };

    hash ^= len;
    hash ^= (hash >> 16);
    hash *= 0x85ebca6b;
    hash ^= (hash >> 13);
    hash *= 0xc2b2ae35;
    hash ^= (hash >> 16);

    return hash;
}

// Function definition
std::size_t hashVector(const std::vector<uint8_t>& vec)
{
    return murmurhash(vec.data(), vec.size(), 0);
}

// Transforms a uint8_t vector of bytes into a uint32_t
uint32_t uint8VToUint32(const std::vector<uint8_t>& bytes)
{
    uint32_t value = 0;
    for (int i = 0; i < 4; i++) {
        value = (value << 8) | bytes[i];
    }
    return value;
}
// Transforms unint32_t of bytes into a uint8_t vector
std::vector<uint8_t> uint32ToUint8V(uint32_t value)
{
    std::vector<uint8_t> bytes(4);
    for (int i = 0; i < 4; i++) {
        bytes[3 - i] = (value >> (i * 8)) & 0xFF;
    }
    return bytes;
}

// Transforms a map <string, int> into a uint8_t vector of bytes.
std::vector<uint8_t> mapIntToUint8V(const std::map<std::string, int>& map)
{
    std::vector<uint8_t> bytes;
    for (const auto& pair : map) {
        std::string key = pair.first;
        int value = pair.second;
        bytes.push_back(static_cast<uint8_t>(key.size())); // Key length
        bytes.insert(bytes.end(), key.begin(), key.end()); // Key
        // Append 'int' value as four bytes
        for (int i = 3; i >= 0; --i) {
            bytes.push_back((value >> (i * 8)) & 0xFF);
        }
    }
    return bytes;
}

// Transforms a uint8_t vector of bytes into a map <string, int>.
std::map<std::string, int> uint8VToMapInt(const std::vector<uint8_t>& bytes)
{
    std::map<std::string, int> map;
    size_t i = 0;
    while (i < bytes.size()) {
        uint8_t keyLen = bytes[i++];
        std::string key(bytes.begin() + i, bytes.begin() + i + keyLen);
        i += keyLen;
        // Read 'int' value from four bytes
        int value = 0;
        for (int j = 0; j < 4; ++j) {
            value |= (static_cast<int>(bytes[i + j]) << ((3 - j) * 8));
        }
        i += 4;
        map[key] = value;
    }
    return map;
}

std::vector<uint8_t> serializeMapBinary(
  const std::map<std::string, std::string>& map)
{
    std::vector<uint8_t> buffer;

    for (const auto& [key, value] : map) {
        // Serialize key size
        uint32_t keySize = key.size();
        uint8_t* keySizeBytes = reinterpret_cast<uint8_t*>(&keySize);
        buffer.insert(
          buffer.end(), keySizeBytes, keySizeBytes + sizeof(keySize));

        // Serialize key
        buffer.insert(buffer.end(), key.begin(), key.end());

        // Serialize value size
        uint32_t valueSize = value.size();
        uint8_t* valueSizeBytes = reinterpret_cast<uint8_t*>(&valueSize);
        buffer.insert(
          buffer.end(), valueSizeBytes, valueSizeBytes + sizeof(valueSize));

        // Serialize value
        buffer.insert(buffer.end(), value.begin(), value.end());
    }

    return buffer;
}

std::map<std::string, std::string> deserializeMapBinary(
  const std::vector<uint8_t>& buffer)
{
    std::map<std::string, std::string> map;
    size_t index = 0;

    while (index < buffer.size()) {
        // Deserialize key size
        uint32_t keySize;
        std::copy_n(&buffer[index],
                    sizeof(keySize),
                    reinterpret_cast<uint8_t*>(&keySize));
        index += sizeof(keySize);

        // Deserialize key
        std::string key(&buffer[index], &buffer[index] + keySize);
        index += keySize;

        // Deserialize value size
        uint32_t valueSize;
        std::copy_n(&buffer[index],
                    sizeof(valueSize),
                    reinterpret_cast<uint8_t*>(&valueSize));
        index += sizeof(valueSize);

        // Deserialize value
        std::string value(&buffer[index], &buffer[index] + valueSize);
        index += valueSize;

        map[std::move(key)] = std::move(value);
    }

    return map;
}

void serializeUInt32(std::vector<uint8_t>& vec, uint32_t value)
{
    vec.insert(vec.end(),
               { static_cast<uint8_t>(value >> 24),
                 static_cast<uint8_t>(value >> 16),
                 static_cast<uint8_t>(value >> 8),
                 static_cast<uint8_t>(value) });
}

std::vector<uint8_t> serializeFuncState(
  const std::map<std::string, std::vector<uint8_t>>& map)
{
    std::vector<uint8_t> serialized;
    // Pre-calculate required capacity to minimize reallocations
    size_t totalSize = 0;
    for (const auto& [key, valueVec] : map) {
        totalSize += 4 + key.size() + 4 + valueVec.size();
    }
    serialized.reserve(totalSize);

    for (const auto& [key, valueVec] : map) {
        serializeUInt32(serialized, key.size());
        serialized.insert(serialized.end(), key.begin(), key.end());
        serializeUInt32(serialized, valueVec.size());
        serialized.insert(serialized.end(), valueVec.begin(), valueVec.end());
    }

    return serialized;
}

uint32_t deserializeUInt32(const std::vector<uint8_t>& vec, size_t& index)
{
    uint32_t value = (static_cast<uint32_t>(vec[index]) << 24) |
                     (static_cast<uint32_t>(vec[index + 1]) << 16) |
                     (static_cast<uint32_t>(vec[index + 2]) << 8) |
                     (static_cast<uint32_t>(vec[index + 3]));
    index += 4;
    return value;
}

std::map<std::string, std::vector<uint8_t>> deserializeFuncState(
  const std::vector<uint8_t>& serialized)
{
    std::map<std::string, std::vector<uint8_t>> map;
    size_t index = 0;
    while (index < serialized.size()) {
        auto keyLength = deserializeUInt32(serialized, index);
        std::string key(serialized.begin() + index,
                        serialized.begin() + index + keyLength);
        index += keyLength;

        auto valueLength = deserializeUInt32(serialized, index);
        std::vector<uint8_t> valueVec(serialized.begin() + index,
                                      serialized.begin() + index + valueLength);
        index += valueLength;

        map.emplace(std::move(key), std::move(valueVec));
    }
    return map;
}

std::vector<uint8_t> serializeParState(
  const std::map<std::string, std::vector<uint8_t>>& map)
{
    return serializeFuncState(map);
}
std::map<std::string, std::vector<uint8_t>> deserializeParState(
  const std::vector<uint8_t>& bytes)
{
    return deserializeFuncState(bytes);
}

// Helper function to append an unsigned 32-bit integer to the buffer
void appendUint32(std::vector<uint8_t>& buffer, uint32_t value)
{
    uint8_t temp[4];
    std::memcpy(temp, &value, 4);
    buffer.insert(buffer.end(), temp, temp + 4);
}

// Serialize a string into a vector of uint8_t
void serializeString(std::vector<uint8_t>& buffer, const std::string& str)
{
    appendUint32(buffer, static_cast<uint32_t>(str.size())); // Length of string
    buffer.insert(buffer.end(), str.begin(), str.end()); // String characters
}

// Deserialize a string from a vector of uint8_t
std::string deserializeString(const std::vector<uint8_t>& buffer, size_t& index)
{
    uint32_t length = *reinterpret_cast<const uint32_t*>(&buffer[index]);
    index += 4;
    std::string str(buffer.begin() + index, buffer.begin() + index + length);
    index += length;
    return str;
}

// Serialize a map of strings
void serializeMap(std::vector<uint8_t>& buffer,
                  const std::map<std::string, std::string>& map)
{
    appendUint32(buffer, static_cast<uint32_t>(map.size())); // Number of pairs
    for (const auto& pair : map) {
        serializeString(buffer, pair.first);  // Serialize key
        serializeString(buffer, pair.second); // Serialize value
    }
}

// Deserialize a map of strings
std::map<std::string, std::string> deserializeMap(
  const std::vector<uint8_t>& buffer,
  size_t& index)
{
    // Handle empty buffer or index out of range
    if (buffer.size() < index + 4) {
        return {}; // Return an empty map if there's not enough data
    }

    uint32_t numPairs = *reinterpret_cast<const uint32_t*>(&buffer[index]);
    index += 4;
    std::map<std::string, std::string> map;

    for (uint32_t i = 0; i < numPairs; ++i) {
        if (buffer.size() < index + 1) {
            throw std::runtime_error(
              "Buffer too small for expected number of pairs");
        }
        std::string key = deserializeString(buffer, index);
        std::string value = deserializeString(buffer, index);
        map[std::move(key)] = std::move(value);
    }
    return map;
}

// Serialize a nested map
void serializeNestedMap(
  std::vector<uint8_t>& buffer,
  const std::map<std::string, std::map<std::string, std::string>>& nestedMap)
{
    appendUint32(
      buffer, static_cast<uint32_t>(nestedMap.size())); // Number of outer pairs
    for (const auto& pair : nestedMap) {
        serializeString(buffer, pair.first); // Serialize outer key
        serializeMap(buffer, pair.second);   // Serialize inner map
    }
}

// Deserialize a nested map
std::map<std::string, std::map<std::string, std::string>> deserializeNestedMap(
  const std::vector<uint8_t>& buffer,
  size_t& index)
{
    if (buffer.size() < index + 4) {
        return {}; // Return an empty nested map if there's not enough data
    }

    uint32_t numPairs = *reinterpret_cast<const uint32_t*>(&buffer[index]);
    index += 4;
    std::map<std::string, std::map<std::string, std::string>> nestedMap;

    for (uint32_t i = 0; i < numPairs; ++i) {
        if (buffer.size() < index + 1) {
            throw std::runtime_error(
              "Buffer too small for expected number of outer pairs");
        }
        std::string key = deserializeString(buffer, index);
        std::map<std::string, std::string> valueMap =
          deserializeMap(buffer, index);
        nestedMap[std::move(key)] = std::move(valueMap);
    }
    return nestedMap;
}

std::string serializeParStateMap(
  const std::map<std::string, std::vector<uint8_t>>& m)
{
    std::string buffer;

    // Write the number of pairs as a 32-bit unsigned integer.
    uint32_t numPairs = static_cast<uint32_t>(m.size());
    buffer.append(reinterpret_cast<const char*>(&numPairs), sizeof(numPairs));

    // Iterate over each key-value pair.
    for (const auto& [key, vec] : m) {
        // Write key length.
        uint32_t keyLen = static_cast<uint32_t>(key.size());
        buffer.append(reinterpret_cast<const char*>(&keyLen), sizeof(keyLen));
        // Write key characters (if any).
        if (keyLen > 0) {
            buffer.append(key.data(), key.size());
        }

        // Write the vector size.
        uint32_t vecSize = static_cast<uint32_t>(vec.size());
        buffer.append(reinterpret_cast<const char*>(&vecSize), sizeof(vecSize));
        // Write vector data if not empty.
        if (vecSize > 0) {
            buffer.append(reinterpret_cast<const char*>(vec.data()),
                          vec.size());
        }
    }

    return buffer;
}

// Deserialize a binary string back into a map of <string, vector<uint8_t>>.
std::map<std::string, std::vector<uint8_t>> deserializeParStateMap(
  const std::string& data)
{
    std::map<std::string, std::vector<uint8_t>> m;
    size_t pos = 0;
    size_t dataSize = data.size();

    // Check that we have enough data for the number of pairs.
    if (dataSize < sizeof(uint32_t)) {
        throw std::runtime_error("Data too short to contain map size");
    }

    // Read the number of pairs.
    uint32_t numPairs;
    std::memcpy(&numPairs, data.data() + pos, sizeof(numPairs));
    pos += sizeof(numPairs);

    // Loop through each pair.
    for (uint32_t i = 0; i < numPairs; ++i) {
        // Read key length.
        if (pos + sizeof(uint32_t) > dataSize) {
            throw std::runtime_error("Data too short for key length");
        }
        uint32_t keyLen;
        std::memcpy(&keyLen, data.data() + pos, sizeof(keyLen));
        pos += sizeof(keyLen);

        // Read key string.
        if (pos + keyLen > dataSize) {
            throw std::runtime_error("Data too short for key string");
        }
        std::string key(data.data() + pos, keyLen);
        pos += keyLen;

        // Read vector length.
        if (pos + sizeof(uint32_t) > dataSize) {
            throw std::runtime_error("Data too short for vector length");
        }
        uint32_t vecSize;
        std::memcpy(&vecSize, data.data() + pos, sizeof(vecSize));
        pos += sizeof(vecSize);

        // Read vector data.
        std::vector<uint8_t> vec;
        if (vecSize > 0) {
            if (pos + vecSize > dataSize) {
                throw std::runtime_error("Data too short for vector data");
            }
            vec.resize(vecSize);
            std::memcpy(vec.data(), data.data() + pos, vecSize);
            pos += vecSize;
        }

        m.emplace(std::move(key), std::move(vec));
    }

    return m;
}

}