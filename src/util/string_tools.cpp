#include <faabric/util/string_tools.h>

#include <algorithm>
#include <string>

namespace faabric::util {

bool isAllWhitespace(const std::string& input)
{
    return std::all_of(input.begin(), input.end(), isspace);
}

bool startsWith(const std::string& input, const std::string& subStr)
{
    if (subStr.empty()) {
        return false;
    }

    return input.rfind(subStr, 0) == 0;
}

bool endsWith(std::string const& value, std::string const& ending)
{
    if (ending.empty()) {
        return false;
    } else if (ending.size() > value.size()) {
        return false;
    }
    return std::equal(ending.rbegin(), ending.rend(), value.rbegin());
}

bool contains(const std::string& input, const std::string& subStr)
{
    if (input.find(subStr) != std::string::npos) {
        return true;
    } else {
        return false;
    }
}

std::string removeSubstr(const std::string& input, const std::string& toErase)
{
    std::string output = input;

    size_t pos = output.find(toErase);

    if (pos != std::string::npos) {
        output.erase(pos, toErase.length());
    }

    return output;
}

bool stringIsInt(const std::string& input)
{
    return !input.empty() &&
           input.find_first_not_of("0123456789") == std::string::npos;
}

std::pair<std::string, std::string> splitUserFunc(const std::string& input)
{
    size_t pos = input.find('_');
    if (pos != std::string::npos) {
        std::string user = input.substr(0, pos);
        std::string function = input.substr(pos + 1);
        return { user, function };
    } else {
        // If no underscore is found, return empty strings (or handle as needed)
        return { "", "" };
    }
}

/**
 * Partition a stream_function_state_0 into user, name and parallelismIdx
 *
 * "stream" corresponds to the first part before the first underscore.
 * "function_state" corresponds to the second part spanning from after the first
 * underscore to the second underscore.
 * "0" corresponds to the numerical part after the last underscore.
 */
std::tuple<std::string, std::string, std::string> splitUserFuncPar(
  const std::string& input)
{
    size_t firstUnderscorePos = input.find('_');
    if (firstUnderscorePos == std::string::npos) {
        throw std::invalid_argument(
          "Input string format is incorrect (missing underscores).");
    }

    // Locate the last underscore
    size_t lastUnderscorePos = input.rfind('_');
    if (lastUnderscorePos == std::string::npos ||
        lastUnderscorePos == firstUnderscorePos) {
        throw std::invalid_argument(
          "Input string format is incorrect (missing last underscore).");
    }

    // Extract the segments
    std::string part1 = input.substr(0, firstUnderscorePos);
    std::string part2 = input.substr(
      firstUnderscorePos + 1, lastUnderscorePos - firstUnderscorePos - 1);
    std::string part3 = input.substr(lastUnderscorePos + 1);

    return std::make_tuple(part1, part2, part3);
}
}
