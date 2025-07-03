#pragma once

#include <map>
#include <sstream>
#include <string>
#include <vector>

namespace faabric::util {
bool isAllWhitespace(const std::string& input);

bool startsWith(const std::string& input, const std::string& subStr);

bool endsWith(const std::string& input, const std::string& subStr);

bool contains(const std::string& input, const std::string& subStr);

std::string removeSubstr(const std::string& input, const std::string& toErase);

bool stringIsInt(const std::string& input);

template<class T>
std::string vectorToString(std::vector<T> vec)
{
    std::stringstream ss;

    ss << "[";
    for (int i = 0; i < vec.size(); i++) {
        ss << vec.at(i);

        if (i < vec.size() - 1) {
            ss << ", ";
        }
    }
    ss << "]";

    return ss.str();
}

std::pair<std::string, std::string> splitUserFunc(const std::string& input);

std::tuple<std::string, std::string, std::string> splitUserFuncPar(
  const std::string& input);

template<typename K, typename V>
std::string mapToString(const std::map<K, V>& map_to_print)
{
    std::stringstream ss;
    ss << "{";

    bool is_first = true;
    for (const auto& pair : map_to_print) {
        if (!is_first) {
            ss << ", ";
        }
        ss << "\"" << pair.first << "\": " << pair.second;
        is_first = false;
    }

    ss << "}";
    return ss.str();
}
}
