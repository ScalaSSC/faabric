#pragma once

namespace faabric::planner {

// The Latency accuracy is dynamic to balance the accuracy and the memory cost.
// From 0 - 50, the accuracy is 1ms, from 50 - 500ms, the accuracy is 5ms, etc.
struct LatencyHistogramConfig
{
    int limit1 = 50;    // 0 - 50ms
    int limit2 = 500;   // 50 - 500ms
    int limit3 = 2000;  // 500 - 2000ms
    int limit4 = 10000; // 2000 - 10000ms

    int step1 = 1;   // 1ms
    int step2 = 5;   // 5ms
    int step3 = 50;  // 50ms
    int step4 = 500; // 500ms

    int offset1 = 0;
    int offset2 = limit1 / step1;
    int offset3 = offset2 + (limit2 - limit1) / step2;
    int offset4 = offset3 + (limit3 - limit2) / step3;
    int overflowBin = offset4 + (limit4 - limit3) / step4;
    int totalBins = overflowBin + 1;

    inline int getBinIdx(int latencyMs) const
    {
        if (latencyMs < 0)
            return 0;
        if (latencyMs < limit1)
            return offset1 + latencyMs / step1;
        if (latencyMs < limit2)
            return offset2 + (latencyMs - limit1) / step2;
        if (latencyMs < limit3)
            return offset3 + (latencyMs - limit2) / step3;
        if (latencyMs < limit4)
            return offset4 + (latencyMs - limit3) / step4;
        return overflowBin;
    }

    inline int getLatencyMs(int binIdx) const
    {
        if (binIdx < offset2)
            return binIdx * step1;
        if (binIdx < offset3)
            return limit1 + (binIdx - offset2) * step2;
        if (binIdx < offset4)
            return limit2 + (binIdx - offset3) * step3;
        if (binIdx < overflowBin)
            return limit3 + (binIdx - offset4) * step4;
        return limit4;
    }
};

}
