#pragma once

#include <faabric/util/logging.h>

#include <algorithm>

namespace faabric::planner {

// -----------------------------------------------------------------------
// Worker capacity model (per worker, at saturation):
//
//   W = processedNum × execTime + alpha × localChainedCalls
//                                + beta  × remoteChainedCalls
//
// W (work rate, us/s) is the total CPU work a worker can sustain per second.
// It is treated as an unknown but CONSTANT across all observations — different
// observations only redistribute the budget among the three terms.
// alpha captures per-call overhead for local chained calls (same host);
// beta  captures per-call overhead for remote chained calls (cross-host,
//       includes network round-trip cost, so beta >> alpha).
//
// Because W is an unknown constant (nuisance parameter), we eliminate it by
// differencing consecutive observations:
//
//   Δ(N×t_e) = -alpha × Δ(n_local) - beta × Δ(n_remote)
//
// Normalized RLS form (divide by Y_NORM so features stay O(1)):
//
//   y     = Δ(N×t_e) / Y_NORM
//   x     = [ -Δ(n_local)×CHAIN_NORM /Y_NORM,
//              -Δ(n_remote)×REMOTE_NORM/Y_NORM ]
//   theta = [alphaS, betaS]
//
// After each RLS step, W is re-estimated from the current observation using
// the freshly updated alpha/beta and tracked via EMA:
//
//   W_obs = N×t_e + alpha×n_local + beta×n_remote  (direct measurement at
//   saturation) W_est = (1 - wEma) × W_est + wEma × W_obs
//
// Normalisation constants (set to typical physical values so each
// normalized coefficient is initialised at 1.0):
//
//   Y_NORM      = typical (processedNum × execTime) = 2000 × 400 = 800000
//   CHAIN_NORM  = typical alpha = 40  us/local-call
//   REMOTE_NORM = typical beta  = 300 us/remote-call (network RTT + scheduling)
//
// Prior-informed initial values:
//   alpha ≈ 40  us/local-call  → alphaS = 1.0   (light IPC overhead)
//   beta  ≈ 300 us/remote-call → betaS  = 1.0   (network round-trip dominated)
//   W     ≈ 880000 us/s        → W_est  = 880000 (N=2000, t_e=400, small
//   overhead)
// -----------------------------------------------------------------------
struct CoeffEstimator
{
    static constexpr double Y_NORM =
      800000.0; // typical processedNum × execTime
    static constexpr double CHAIN_NORM = 40.0; // typical alpha (us/local-call)
    static constexpr double REMOTE_NORM =
      300.0; // typical beta  (us/remote-call)

    // Max normalized change per RLS step (prevents a single noisy diff from
    // causing a large parameter jump in one shot).
    static constexpr double MAX_RLS_STEP = 0.15;

    // Normalized RLS state: theta = [alphaS, betaS]
    double alphaS =
      1.0; // alpha ≈ 40  us/local-call  (alphaS = alpha / CHAIN_NORM)
    double betaS =
      1.0; // beta  ≈ 300 us/remote-call  (betaS  = beta  / REMOTE_NORM)
    double lambda = 0.97; // RLS forgetting factor

    // 2×2 RLS covariance matrix, row-major.
    double P[4] = {
        0.1,
        0.0,
        0.0,
        0.1,
    };

    // EMA estimate of W (us/s), updated each observation using current
    // alpha/beta estimates. Used only in maxProcessed(); not a RLS parameter.
    double W_est = 880000.0;
    double wEma = 0.05;

    // Per-step cap on alphaS/betaS change (normalized units). Prevents P
    // matrix inflation from causing a parameter collapse on a single outlier.
    double maxRlsStep = MAX_RLS_STEP;
    // Per-dimension gate (calls/s). When localChainedCalls < threshold, K[0]
    // is zeroed so alphaS is not updated; similarly K[1]/betaS for remote.
    // Prevents noisy RLS updates when chained-call volume is too sparse to
    // carry a reliable signal. 0 = disabled (always update).
    double minChainedForRls = 10.0;

    // Previous observation stored for differencing.
    // prevNtE < 0 signals "no prior observation yet".
    double prevNtE = -1.0;
    double prevLocalChained = 0.0;
    double prevRemoteChained = 0.0;

    // Physical coefficients
    double C() const { return W_est; } // work rate estimate (us/s)
    double alpha() const { return alphaS * CHAIN_NORM; }
    double beta() const { return betaS * REMOTE_NORM; }

    // Update estimates with one saturated-worker observation.
    // processedNum       : requests processed in this second
    // execTime           : avg execution time per request (microseconds)
    // localChainedCalls  : chained calls dispatched to the same host
    // remoteChainedCalls : chained calls dispatched to remote hosts
    void update(double processedNum,
                double execTime,
                double localChainedCalls,
                double remoteChainedCalls)
    {
        if (processedNum <= 0.0 || execTime <= 0.0)
            return;

        double NtE = processedNum * execTime;

        if (prevNtE < 0.0) {
            prevNtE = NtE;
            prevLocalChained = localChainedCalls;
            prevRemoteChained = remoteChainedCalls;
            // Update W_est on first observation using prior alpha/beta.
            double W_obs = NtE + alphaS * localChainedCalls * CHAIN_NORM +
                           betaS * remoteChainedCalls * REMOTE_NORM;
            W_est = (1.0 - wEma) * W_est + wEma * W_obs;
            return;
        }

        // Differences between consecutive observations (W cancels out)
        double dNtE = NtE - prevNtE;
        double dLocal = localChainedCalls - prevLocalChained;
        double dRemote = remoteChainedCalls - prevRemoteChained;

        prevNtE = NtE;
        prevLocalChained = localChainedCalls;
        prevRemoteChained = remoteChainedCalls;

        // When features don't vary, skip RLS (uninformative) but still
        // update W_est — it is a direct measurement at saturation regardless.
        if (std::abs(dLocal) < 1e-6 && std::abs(dRemote) < 1e-6) {
            double W_obs = NtE + alphaS * localChainedCalls * CHAIN_NORM +
                           betaS * remoteChainedCalls * REMOTE_NORM;
            W_est = (1.0 - wEma) * W_est + wEma * W_obs;
            return;
        }

        double y = dNtE / Y_NORM;
        double x[2] = { -(dLocal * CHAIN_NORM / Y_NORM),
                        -(dRemote * REMOTE_NORM / Y_NORM) };

        // Px = P · x  (2-vector)
        double Px[2] = { P[0] * x[0] + P[1] * x[1], P[2] * x[0] + P[3] * x[1] };

        // denom = lambda + x' · Px
        double denom = lambda + x[0] * Px[0] + x[1] * Px[1];
        if (denom < 1e-12)
            return;

        double K[2] = { Px[0] / denom, Px[1] / denom };

        // Per-dimension gate: if a chained-call count is below minChainedForRls
        // its signal is noise-dominated — zero that gain so the corresponding
        // parameter is not updated (alphaS frozen when local is sparse,
        // betaS frozen when remote is sparse).
        // Pxeff mirrors the zeroing so the P rank-1 update K·Pxeff' stays
        // symmetric; zeroing only K while leaving Px intact would break P
        // symmetry (P_new[0,1] != P_new[1,0]).
        double Pxeff[2] = { Px[0], Px[1] };
        if (minChainedForRls > 0.0) {
            if (localChainedCalls < minChainedForRls) {
                K[0] = 0.0;
                Pxeff[0] = 0.0;
            }
            if (remoteChainedCalls < minChainedForRls) {
                K[1] = 0.0;
                Pxeff[1] = 0.0;
            }
        }

        double pred = x[0] * alphaS + x[1] * betaS;
        double err = y - pred;

        // Per-step clamp: cap |ΔalphaS|/|ΔbetaS| to maxRlsStep so a single
        // noisy diff cannot cause a large parameter jump in one shot.
        alphaS += std::clamp(K[0] * err, -maxRlsStep, maxRlsStep);
        betaS += std::clamp(K[1] * err, -maxRlsStep, maxRlsStep);

        // Update W_est using freshly updated alpha/beta.
        double W_obs = NtE + alphaS * localChainedCalls * CHAIN_NORM +
                       betaS * remoteChainedCalls * REMOTE_NORM;
        W_est = (1.0 - wEma) * W_est + wEma * W_obs;

        // P = (P - K·Pxeff') / lambda  (rank-1 update, uses Pxeff to keep P
        // symmetric)
        for (int i = 0; i < 2; i++)
            for (int j = 0; j < 2; j++)
                P[i * 2 + j] = (P[i * 2 + j] - K[i] * Pxeff[j]) / lambda;

        SPDLOG_DEBUG("CoeffEstimator: alpha={:.3f}us/local-call, "
                     "beta={:.3f}us/remote-call, "
                     "W_est={:.0f}us/s W_obs={:.0f}us/s "
                     "(dy={:.4f}, err={:.4f}, dLocal={:.0f}, dRemote={:.0f})",
                     alpha(),
                     beta(),
                     W_est,
                     W_obs,
                     y,
                     err,
                     dLocal,
                     dRemote);
    }

    // Set an RLS tuning parameter by name.
    // rls_lambda      : forgetting factor            (0.9 – 0.9999)
    // rls_p           : reset P diagonal to value    (> 0)
    // rls_w_ema       : W EMA smoothing factor       (0.001 – 0.5)
    // rls_max_step    : per-step normalized cap       (> 0.01)
    // rls_min_chained : min total chained calls/s to run RLS (0 = disabled)
    // Returns false if key is unrecognized.
    bool setRlsParam(const std::string& key, double value)
    {
        if (key == "rls_lambda") {
            lambda = std::clamp(value, 0.9, 0.9999);
        } else if (key == "rls_p") {
            double v = std::max(value, 1e-6);
            P[0] = v;
            P[1] = 0.0;
            P[2] = 0.0;
            P[3] = v;
        } else if (key == "rls_w_ema") {
            wEma = std::clamp(value, 0.001, 0.5);
        } else if (key == "rls_max_step") {
            maxRlsStep = std::max(value, 0.01);
        } else if (key == "rls_min_chained") {
            minChainedForRls = std::max(value, 0.0);
        } else {
            return false;
        }
        return true;
    }

    // Set a physical coefficient by name ("coeff_a", "coeff_b", "coeff_c").
    // Returns false if key is unrecognized.
    bool set(const std::string& key, double value)
    {
        if (key == "coeff_c") {
            W_est = value;
        } else if (key == "coeff_a") {
            alphaS = value / CHAIN_NORM;
        } else if (key == "coeff_b") {
            betaS = value / REMOTE_NORM;
        } else {
            return false;
        }
        return true;
    }

    // Maximum sustainable processed-requests per second.
    // execTime           : avg execution time per request (microseconds)
    // localChainedCalls  : expected local chained calls in this second
    // remoteChainedCalls : expected remote chained calls in this second
    double maxProcessed(double execTime,
                        double localChainedCalls,
                        double remoteChainedCalls) const
    {
        if (execTime <= 0.0)
            return 0.0;
        double overhead =
          alpha() * localChainedCalls + beta() * remoteChainedCalls;
        double budget = W_est - overhead;
        if (budget <= 0.0)
            return 0.0;
        return budget / execTime;
    }
};

}
