#pragma once

#include <chrono>
#include <cstdint>
#include <ctime>
#include <iostream>

namespace faabric::util {
typedef std::chrono::steady_clock::time_point TimePoint;

class Clock
{
  public:
    Clock();

    const TimePoint now();

    const long epochMillis();

    const long long epochMicros();

    const long long epochNanos();

    const long long ntpMicros();

    const long timeDiff(const TimePoint& t1, const TimePoint& t2);

    const long timeDiffNano(const TimePoint& t1, const TimePoint& t2);

    const long timeDiffMicro(const TimePoint& t1, const TimePoint& t2);
};

Clock& getGlobalClock();

const int64_t getCpuTimeNano();

const int64_t getCpuTimeNano(const clockid_t clk);

}
