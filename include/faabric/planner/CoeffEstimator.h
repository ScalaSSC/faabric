#pragma once

#include <faabric/util/logging.h>

#include <algorithm>

namespace faabric::planner {

// -----------------------------------------------------------------------
// CPU-budget model (per worker, at saturation):
//
//   C = processedNum × execTime + alpha × chainedCalls + beta × numDestHosts
//
// C is the total CPU budget per second (us/s), shared across all workers.
// alpha and beta capture per-call chaining and per-host fan-out overhead.
//
// maxProcessed = (C - alpha×chainedCalls - beta×numDestHosts) / execTime
//
// To make the 3-parameter RLS well-conditioned, all regression variables
// are normalized before fitting:
//
//   y_norm = processedNum × execTime / Y_NORM              (~600)
//   x      = [1, -chainedCalls/CHAIN_NORM, -numDestHosts/HOST_NORM]  (~[1,-1,-1])
//
// Stored theta = [C_n, alphaS, betaS] are the normalized coefficients:
//   C     = C_n * Y_NORM
//   alpha = alphaS * Y_NORM / CHAIN_NORM
//   beta  = betaS  * Y_NORM / HOST_NORM
//
// Based on observed values: processedNum~2000, execTime~300us,
//   chainedCalls~3000, numDestHosts~0-10.
// -----------------------------------------------------------------------
struct CoeffEstimator
{
    static constexpr double Y_NORM = 1000.0;     // us → ms scale for y
    static constexpr double CHAIN_NORM = 3000.0; // typical chainedCalls/s
    static constexpr double HOST_NORM = 5.0;     // typical numDestHosts

    // Normalized RLS state: theta = [C_n, alphaS, betaS]
    double C_n = 0.0;
    double alphaS = 0.0;
    double betaS = 0.0;
    double lambda = 0.97; // RLS forgetting factor
    // 3×3 RLS covariance matrix, row-major
    double P[9] = { 1e6, 0, 0, 0, 1e6, 0, 0, 0, 1e6 };

    // Physical coefficients (derived from normalized state)
    double C() const { return C_n * Y_NORM; }
    double alpha() const { return alphaS * Y_NORM / CHAIN_NORM; }
    double beta() const { return betaS * Y_NORM / HOST_NORM; }

    // Update estimates with one saturated-worker observation.
    // processedNum : requests processed in this second
    // execTime     : avg execution time per request (microseconds)
    // chainedCalls : total chained-call count for this second
    // numDestHosts : number of distinct destination hosts (>= 0)
    void update(double processedNum,
                double execTime,
                double chainedCalls,
                double numDestHosts)
    {
        if (processedNum <= 0.0 || execTime <= 0.0)
            return;

        double y = (processedNum * execTime) / Y_NORM;
        double x[3] = { 1.0,
                         -(chainedCalls / CHAIN_NORM),
                         -(numDestHosts / HOST_NORM) };

        // Px = P · x  (3-vector)
        double Px[3] = { 0.0, 0.0, 0.0 };
        for (int i = 0; i < 3; i++)
            for (int j = 0; j < 3; j++)
                Px[i] += P[i * 3 + j] * x[j];

        // denom = lambda + x' · Px
        double denom = lambda;
        for (int i = 0; i < 3; i++)
            denom += x[i] * Px[i];
        if (denom < 1e-12)
            return;

        // K = Px / denom
        double K[3] = { Px[0] / denom, Px[1] / denom, Px[2] / denom };

        // prediction = C_n - alphaS×(chainedCalls/CHAIN_NORM) - betaS×(numDestHosts/HOST_NORM)
        double pred = x[0] * C_n + x[1] * alphaS + x[2] * betaS;
        double err = y - pred;

        // Unconstrained update — clamping inside RLS breaks P↔theta
        // consistency. Non-negativity enforced only in maxProcessed().
        C_n = C_n + K[0] * err;
        alphaS = alphaS + K[1] * err;
        betaS = betaS + K[2] * err;

        // P = (P - K·Px') / lambda  (rank-1 update)
        for (int i = 0; i < 3; i++)
            for (int j = 0; j < 3; j++)
                P[i * 3 + j] = (P[i * 3 + j] - K[i] * Px[j]) / lambda;

        SPDLOG_DEBUG(
          "CoeffEstimator: C={:.0f}us/s, alpha={:.3f}us/call, "
          "beta={:.3f}us/host "
          "(y_norm={:.1f}, err={:.2f}, "
          "processedNum={:.0f}, execTime={:.1f}us, "
          "chained={:.0f}, destHosts={:.0f})",
          C(),
          alpha(),
          beta(),
          y,
          err,
          processedNum,
          execTime,
          chainedCalls,
          numDestHosts);
    }

    // Maximum sustainable processed-requests per second.
    // execTime     : avg execution time per request (microseconds)
    // chainedCalls : expected total chained calls in this second
    // numDestHosts : expected number of distinct destination hosts
    double maxProcessed(double execTime,
                        double chainedCalls,
                        double numDestHosts) const
    {
        if (execTime <= 0.0 || C_n <= 0.0)
            return 0.0;
        double overhead = std::max(0.0, alphaS) * (chainedCalls / CHAIN_NORM) +
                          std::max(0.0, betaS) * (numDestHosts / HOST_NORM);
        double budget = C_n - overhead;
        if (budget <= 0.0)
            return 0.0;
        return (budget * Y_NORM) / execTime;
    }
};

}
