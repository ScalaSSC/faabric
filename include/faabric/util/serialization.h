#pragma once

#include <cstdint>
#include <map>
#include <string>
#include <vector>

// THERE MUST BE SAME AS CPP LIMFAASM UTIL SERIALIZATION
// TODO - Ensure the consistency with client.cpp/serialization.h.
// Except the serializeParStateMap and deserializeParStateMap

namespace faabric::util {

std::size_t hashVector(const std::vector<uint8_t>& vec);

// Serialiazion and Deserialization from uint8_t vetors to uint32_t
uint32_t uint8VToUint32(const std::vector<uint8_t>& bytes);
std::vector<uint8_t> uint32ToUint8V(uint32_t value);

// Serialiazion and Deserialization from uint8_t vetors to Map<string,string>
std::vector<uint8_t> serializeMapBinary(
  const std::map<std::string, std::string>& map);
std::map<std::string, std::string> deserializeMapBinary(
  const std::vector<uint8_t>& bytes);

// Serialiazion and Deserialization from uint8_t vetors to Map<string,int>
std::vector<uint8_t> mapIntToUint8V(const std::map<std::string, int>& map);
std::map<std::string, int> uint8VToMapInt(const std::vector<uint8_t>& bytes);

// Serialiazion and Deserialization from function State to uint8_t vetors
std::vector<uint8_t> serializeFuncState(
  const std::map<std::string, std::vector<uint8_t>>& map);
std::map<std::string, std::vector<uint8_t>> deserializeFuncState(
  const std::vector<uint8_t>& bytes);

// Serialiazion and Deserialization of Paritioned State Input (same as
// FuncState)
std::vector<uint8_t> serializeParState(
  const std::map<std::string, std::vector<uint8_t>>& map);
std::map<std::string, std::vector<uint8_t>> deserializeParState(
  const std::vector<uint8_t>& bytes);

// Serialize a string into a vector of uint8_t
void serializeString(std::vector<uint8_t>& buffer, const std::string& str);

// Deserialize a string from a vector of uint8_t
std::string deserializeString(const std::vector<uint8_t>& buffer,
                              size_t& index);

// Serialize a map of strings
void serializeMap(std::vector<uint8_t>& buffer,
                  const std::map<std::string, std::string>& map);

// Deserialize a map of strings
std::map<std::string, std::string> deserializeMap(
  const std::vector<uint8_t>& buffer,
  size_t& index);

// Serialize a nested map
void serializeNestedMap(
  std::vector<uint8_t>& buffer,
  const std::map<std::string, std::map<std::string, std::string>>& nestedMap);

// Deserialize a nested map
std::map<std::string, std::map<std::string, std::string>> deserializeNestedMap(
  const std::vector<uint8_t>& buffer,
  size_t& index);

// Helper function to append an unsigned 32-bit integer to the buffer
void appendUint32(std::vector<uint8_t>& buffer, uint32_t value);

// Helper to append data to the vector
template<typename T>
void appendToBuffer(std::vector<uint8_t>& buffer, const T& value);

std::vector<uint8_t> serializeParStateMap(
  const std::map<std::string, std::vector<uint8_t>>& m);

std::map<std::string, std::vector<uint8_t>> deserializeParStateMap(
  const std::vector<uint8_t>& data);

}