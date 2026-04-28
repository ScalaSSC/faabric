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
// Normalized form (multiplicative convention):
//
//   C_n × Y_NORM = processedNum × execTime
//                + alphaS × chainedCalls × CHAIN_NORM
//                + betaS  × numDestHosts × HOST_NORM
//
// Mapping between physical and normalized coefficients (multiply):
//
//   C     = C_n    × Y_NORM
//   alpha = alphaS × CHAIN_NORM
//   beta  = betaS  × HOST_NORM
//
// In RLS form (divide both sides by Y_NORM so y stays bounded):
//
//   y = processedNum × execTime / Y_NORM
//   x = [ 1,
//         -chainedCalls × CHAIN_NORM / Y_NORM,
//         -numDestHosts × HOST_NORM / Y_NORM ]
//   theta = [C_n, alphaS, betaS]
//
// Convention: NORM constants are set to the typical PHYSICAL value of each
// quantity, so the corresponding normalized coefficient is initialised at
// 1.0 ("ratio to typical"):
//
//   Y_NORM     = typical (processedNum × execTime) = 1000 × 400 = 400000
//   CHAIN_NORM = typical alpha = 40   us/call
//   HOST_NORM  = typical beta  = 8000 us/host
//
// Observed ranges: processedNum~1000, execTime 300-500us,
//   chainedCalls~400, numDestHosts 1-10 (avg 5).
//
// Prior-informed initial values (execTime ≈ 400us midpoint):
//   C     ≈ 450000 us/s     → C_n    = 450000 / Y_NORM     = 1.125
//   alpha ≈ 40 us/call      → alphaS = alpha / CHAIN_NORM  = 1.0
//   beta  ≈ 8000 us/host    → betaS  = beta  / HOST_NORM   = 1.0
//
// Where the prior comes from:
//   - alpha = 0.1 × execTime    (10% chaining overhead rule)
//   - beta  = 20  × execTime    (1→2 host: processedNum drops by 20
//                                 ⇒ beta  = 20 × execTime)
//   - C     = (1000 + 40×chain/processed + beta×host) × execTime
// -----------------------------------------------------------------------
struct CoeffEstimator
{
    static constexpr double Y_NORM = 400000.0;   // typical processedNum × execTime
    static constexpr double CHAIN_NORM = 40.0;   // typical alpha (us/call)
    static constexpr double HOST_NORM = 8000.0;  // typical beta  (us/host)

    // Hard bounds on each normalized coefficient (as ratios to prior).
    // Guards against single-observation overshoots and stuck-at-zero
    // behavior. C is bounded tighter because CPU capacity is structural;
    // alpha/beta are workload-dependent and allowed wider drift.
    static constexpr double C_N_MIN    = 0.5,  C_N_MAX    = 3.0;  // C  : 200K-1350K us/s
    static constexpr double ALPHAS_MIN = 0.25, ALPHAS_MAX = 4.0;  // α  : 10-160 us/call
    static constexpr double BETAS_MIN  = 0.25, BETAS_MAX  = 4.0;  // β  : 2000-32000 us/host

    // Normalized RLS state: theta = [C_n, alphaS, betaS]
    // Each coefficient = 1.0 means "exactly at the typical/prior value".
    // Initialised with prior estimates to avoid the long cold-start climb
    // from zero and the wild first-update overshoot from an uninformed
    // prior.
    double C_n = 1.125;    // C     ≈ 450000 us/s   (C_n     = C / Y_NORM)
    double alphaS = 1.0;   // alpha ≈ 40 us/call    (alphaS  = alpha / CHAIN_NORM)
    double betaS = 1.0;    // beta  ≈ 8000 us/host  (betaS   = beta  / HOST_NORM)
    double lambda = 0.97;  // RLS forgetting factor
    // 3×3 RLS covariance matrix, row-major. With theta now O(1), absolute
    // P values are also O(1). Asymmetric diagonal reflects our confidence:
    //   - C_n   moderately certain (execTime range 300-500 → ±25%)
    //   - alphaS strong prior (10% rule) AND x[1]=-0.04 signal is weak,
    //     so we keep P[1,1] tiny — αS is effectively held at the prior.
    //   - betaS  strong prior (1→2 host measurement); x[2]=-0.1 has decent
    //     signal so P[2,2] can be slightly larger.
    double P[9] = {
        0.1,  0,    0,
        0,    0.01, 0,
        0,    0,    0.05,
    };

    // Physical coefficients (derived from normalized state via multiplication)
    double C() const { return C_n * Y_NORM; }
    double alpha() const { return alphaS * CHAIN_NORM; }
    double beta() const { return betaS * HOST_NORM; }

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

        // When numDestHosts ≤ 1 the fan-out feature is a constant, making it
        // collinear with the bias term and causing betaS / C_n to diverge.
        // Treat it as 0 so betaS is only estimated under genuine multi-host
        // traffic (> 1 distinct destination hosts).
        double effectiveDestHosts = numDestHosts > 1.0 ? numDestHosts : 0.0;

        double y = (processedNum * execTime) / Y_NORM;
        double x[3] = { 1.0,
                        -(chainedCalls * CHAIN_NORM / Y_NORM),
                        -(effectiveDestHosts * HOST_NORM / Y_NORM) };

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

        // prediction = C_n - alphaS×(chain×CHAIN_NORM/Y_NORM)
        //                  - betaS ×(host ×HOST_NORM /Y_NORM)
        double pred = x[0] * C_n + x[1] * alphaS + x[2] * betaS;
        double err = y - pred;

        // Hard-clamp projection after each RLS step. Strictly this breaks
        // P↔theta consistency, but RLS adapts in subsequent steps and the
        // bounds prevent (a) negative/runaway estimates from a noisy single
        // observation and (b) stuck-at-zero behavior when the unconstrained
        // RLS would drive a coefficient below its physical minimum.
        C_n    = std::clamp(C_n    + K[0] * err, C_N_MIN,    C_N_MAX);
        alphaS = std::clamp(alphaS + K[1] * err, ALPHAS_MIN, ALPHAS_MAX);
        betaS  = std::clamp(betaS  + K[2] * err, BETAS_MIN,  BETAS_MAX);

        // P = (P - K·Px') / lambda  (rank-1 update)
        for (int i = 0; i < 3; i++)
            for (int j = 0; j < 3; j++)
                P[i * 3 + j] = (P[i * 3 + j] - K[i] * Px[j]) / lambda;

        SPDLOG_DEBUG("CoeffEstimator: C={:.0f}us/s, alpha={:.3f}us/call, "
                     "beta={:.3f}us/host "
                     "(y_norm={:.3f}, err={:.3f}, "
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
                     effectiveDestHosts);
    }

    // Set a physical coefficient by name ("coeff_c", "coeff_a", "coeff_b").
    // Returns false if key is unrecognized.
    bool set(const std::string& key, double value)
    {
        if (key == "coeff_c") {
            C_n = std::clamp(value / Y_NORM, C_N_MIN, C_N_MAX);
        } else if (key == "coeff_a") {
            alphaS = std::clamp(value / CHAIN_NORM, ALPHAS_MIN, ALPHAS_MAX);
        } else if (key == "coeff_b") {
            betaS = std::clamp(value / HOST_NORM, BETAS_MIN, BETAS_MAX);
        } else {
            return false;
        }
        return true;
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
        // alphaS and betaS are bounded ≥ MIN > 0 by construction.
        double overhead = alphaS * (chainedCalls * CHAIN_NORM / Y_NORM) +
                          betaS * (numDestHosts * HOST_NORM / Y_NORM);
        double budget = C_n - overhead;
        if (budget <= 0.0)
            return 0.0;
        return (budget * Y_NORM) / execTime;
    }
};

}
