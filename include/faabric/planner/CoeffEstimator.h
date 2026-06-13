#pragma once

#include <faabric/util/logging.h>

#include <algorithm>

namespace faabric::planner {

// -----------------------------------------------------------------------
// Worker capacity model (per worker, at saturation):
//
//   W = processedNum × execTime + alpha × localChainedCalls
//                                + beta  × remoteChainedCalls
//                                + gamma × numRemoteHosts
//
// W (work rate, us/s) is the total CPU work a worker can sustain per second.
// It is treated as an unknown but CONSTANT across all observations — different
// observations only redistribute the budget among the four terms.
// alpha captures per-call overhead for local chained calls (same host);
// beta  captures per-call overhead for remote chained calls (cross-host,
//       includes network round-trip cost, so beta >> alpha);
// gamma captures per-destination-host overhead — a FIXED cost paid once per
//       distinct remote worker this host fanned out to (e.g. connection /
//       bookkeeping cost), independent of how many calls went to that host.
//
// Because W is an unknown constant (nuisance parameter), we eliminate it by
// differencing consecutive observations:
//
//   Δ(N×t_e) = -alpha × Δ(n_local) - beta × Δ(n_remote) - gamma × Δ(n_hosts)
//
// Normalized RLS form (divide by Y_NORM so features stay O(1)):
//
//   y     = Δ(N×t_e) / Y_NORM
//   x     = [ -Δ(n_local) ×CHAIN_NORM /Y_NORM,
//              -Δ(n_remote)×REMOTE_NORM/Y_NORM,
//              -Δ(n_hosts) ×HOST_NORM  /Y_NORM ]
//   theta = [alphaS, betaS, gammaS]
//
// After each RLS step, W is re-estimated from the current observation using
// the freshly updated alpha/beta/gamma and tracked via EMA:
//
//   W_obs = N×t_e + alpha×n_local + beta×n_remote + gamma×n_hosts  (direct
//   measurement at saturation)
//   W_est = (1 - wEma) × W_est + wEma × W_obs
//
// Normalisation constants (set to typical physical values so each
// normalized coefficient is initialised at 1.0):
//
//   Y_NORM      = typical (processedNum × execTime) = 2000 × 400 = 800000
//   CHAIN_NORM  = typical alpha = 38.5  us/local-call
//   REMOTE_NORM = typical beta  = 275   us/remote-call (network RTT +
//                 scheduling)
//   HOST_NORM   = typical gamma = 51500 us/dest-host  (per-host fixed overhead)
//
// Prior-informed initial values:
//   alpha ≈ 38.5  us/local-call → alphaS = 1.0  (light IPC overhead)
//   beta  ≈ 275   us/remote-call→ betaS  = 1.0  (network round-trip dominated)
//   gamma ≈ 51500 us/dest-host  → gammaS = 1.0  (per-destination fixed cost)
//   W     ≈ 880000 us/s        → W_est  = 880000 (N=2000, t_e=400, small
//   overhead)
// -----------------------------------------------------------------------
struct CoeffEstimator
{
    // Normalisation constants. Runtime-tunable via setRlsParam (rls_y_norm /
    // rls_chain_norm / rls_remote_norm / rls_host_norm) so the feature scaling
    // and coefficient priors can be retuned without a rebuild. NOTE: changing a
    // NORM rescales the corresponding physical coefficient, since
    // alpha()=alphaS×CHAIN_NORM etc. (alphaS/betaS/gammaS are left untouched).
    double Y_NORM = 800000.0;   // typical processedNum × execTime
    double CHAIN_NORM = 38.5;   // typical alpha (us/local-call)
    double REMOTE_NORM = 275.0; // typical beta  (us/remote-call)
    double HOST_NORM = 51500.0; // typical gamma (us/dest-host)

    // Max normalized change per RLS step (prevents a single noisy diff from
    // causing a large parameter jump in one shot).
    static constexpr double MAX_RLS_STEP = 0.15;

    // Normalized RLS state: theta = [alphaS, betaS, gammaS]
    double alphaS =
      1.0; // alpha ≈ 38.5 us/local-call (alphaS = alpha / CHAIN_NORM)
    double betaS =
      1.0; // beta  ≈ 275  us/remote-call (betaS  = beta  / REMOTE_NORM)
    double gammaS =
      1.0; // gamma ≈ 51500 us/dest-host (gammaS = gamma / HOST_NORM)
    double lambda = 0.97; // RLS forgetting factor

    // 3×3 RLS covariance matrix, row-major.
    double P[9] = {
        0.1, 0.0, 0.0, //
        0.0, 0.1, 0.0, //
        0.0, 0.0, 0.1, //
    };

    // EMA estimate of W (us/s), updated each observation using current
    // alpha/beta/gamma estimates. Used only in maxProcessed(); not a RLS
    // parameter.
    double W_est = 880000.0;
    double wEma = 0.05;

    // Per-step cap on alphaS/betaS/gammaS change (normalized units). Prevents P
    // matrix inflation from causing a parameter collapse on a single outlier.
    double maxRlsStep = MAX_RLS_STEP;
    // Whole-observation remote gate (remote calls/s). When remoteChainedCalls
    // < threshold, the entire RLS step is skipped and prev* is frozen, so
    // differencing only ever happens between remote-heavy (fan-out) hosts.
    // Set between the non-fan-out and fan-out remote modes (e.g. ~100 when
    // fan-out hosts do ~1000+ remote calls/s). 0 = disabled (always update).
    double minChainedForRls = 100.0;

    // When true, update() returns immediately without modifying any estimates.
    bool coeffParamFix = false;

    // Previous observation stored for differencing.
    // prevNtE < 0 signals "no prior observation yet".
    double prevNtE = -1.0;
    double prevLocalChained = 0.0;
    double prevRemoteChained = 0.0;
    double prevRemoteHosts = 0.0;

    // Physical coefficients
    double C() const { return W_est; } // work rate estimate (us/s)
    double alpha() const { return alphaS * CHAIN_NORM; }
    double beta() const { return betaS * REMOTE_NORM; }
    double gamma() const { return gammaS * HOST_NORM; }

    // Direct W measurement at saturation using current alpha/beta/gamma.
    double observeW(double NtE,
                    double localChainedCalls,
                    double remoteChainedCalls,
                    double remoteHostCount) const
    {
        return NtE + alphaS * localChainedCalls * CHAIN_NORM +
               betaS * remoteChainedCalls * REMOTE_NORM +
               gammaS * remoteHostCount * HOST_NORM;
    }

    // Update estimates with one saturated-worker observation.
    // processedNum       : requests processed in this second
    // execTime           : avg execution time per request (microseconds)
    // localChainedCalls  : chained calls dispatched to the same host
    // remoteChainedCalls : chained calls dispatched to remote hosts
    // remoteHostCount    : distinct remote hosts this worker fanned out to
    void update(double processedNum,
                double execTime,
                double localChainedCalls,
                double remoteChainedCalls,
                double remoteHostCount)
    {
        if (coeffParamFix)
            return;

        if (processedNum <= 0.0 || execTime <= 0.0)
            return;

        double NtE = processedNum * execTime;

        // Whole-observation gate. A host with too few remote chained calls is
        // not a fan-out host and carries no reliable beta/gamma signal. Skip
        // the ENTIRE RLS step WITHOUT touching prev* — this freezes prev at the
        // last qualifying (remote-heavy) observation, so differencing only ever
        // happens between fan-out hosts.
        if (minChainedForRls > 0.0 && remoteChainedCalls < minChainedForRls) {
            double W_obs = observeW(
              NtE, localChainedCalls, remoteChainedCalls, remoteHostCount);
            W_est = (1.0 - wEma) * W_est + wEma * W_obs;
            return;
        }

        if (prevNtE < 0.0) {
            prevNtE = NtE;
            prevLocalChained = localChainedCalls;
            prevRemoteChained = remoteChainedCalls;
            prevRemoteHosts = remoteHostCount;
            // Update W_est on first observation using prior alpha/beta/gamma.
            double W_obs = observeW(
              NtE, localChainedCalls, remoteChainedCalls, remoteHostCount);
            W_est = (1.0 - wEma) * W_est + wEma * W_obs;
            return;
        }

        // Differences between consecutive observations (W cancels out)
        double dNtE = NtE - prevNtE;
        double dLocal = localChainedCalls - prevLocalChained;
        double dRemote = remoteChainedCalls - prevRemoteChained;
        double dHosts = remoteHostCount - prevRemoteHosts;

        prevNtE = NtE;
        prevLocalChained = localChainedCalls;
        prevRemoteChained = remoteChainedCalls;
        prevRemoteHosts = remoteHostCount;

        // When features don't vary, skip RLS (uninformative) but still
        // update W_est — it is a direct measurement at saturation regardless.
        if (std::abs(dLocal) < 1e-6 && std::abs(dRemote) < 1e-6 &&
            std::abs(dHosts) < 1e-6) {
            double W_obs = observeW(
              NtE, localChainedCalls, remoteChainedCalls, remoteHostCount);
            W_est = (1.0 - wEma) * W_est + wEma * W_obs;
            return;
        }

        double y = dNtE / Y_NORM;
        double x[3] = { -(dLocal * CHAIN_NORM / Y_NORM),
                        -(dRemote * REMOTE_NORM / Y_NORM),
                        -(dHosts * HOST_NORM / Y_NORM) };

        // Px = P · x  (3-vector)
        double Px[3];
        for (int i = 0; i < 3; i++)
            Px[i] =
              P[i * 3 + 0] * x[0] + P[i * 3 + 1] * x[1] + P[i * 3 + 2] * x[2];

        // denom = lambda + x' · Px
        double denom = lambda + x[0] * Px[0] + x[1] * Px[1] + x[2] * Px[2];
        if (denom < 1e-12)
            return;

        double K[3] = { Px[0] / denom, Px[1] / denom, Px[2] / denom };

        double pred = x[0] * alphaS + x[1] * betaS + x[2] * gammaS;
        double err = y - pred;

        // Per-step clamp: cap |ΔalphaS|/|ΔbetaS|/|ΔgammaS| to maxRlsStep so a
        // single noisy diff cannot cause a large parameter jump in one shot.
        alphaS += std::clamp(K[0] * err, -maxRlsStep, maxRlsStep);
        betaS += std::clamp(K[1] * err, -maxRlsStep, maxRlsStep);
        gammaS += std::clamp(K[2] * err, -maxRlsStep, maxRlsStep);

        // Update W_est using freshly updated alpha/beta/gamma.
        double W_obs =
          observeW(NtE, localChainedCalls, remoteChainedCalls, remoteHostCount);
        W_est = (1.0 - wEma) * W_est + wEma * W_obs;

        // P = (P - K·Px') / lambda  (rank-1 update)
        for (int i = 0; i < 3; i++)
            for (int j = 0; j < 3; j++)
                P[i * 3 + j] = (P[i * 3 + j] - K[i] * Px[j]) / lambda;

        SPDLOG_DEBUG("CoeffEstimator: alpha={:.3f}us/local-call, "
                     "beta={:.3f}us/remote-call, gamma={:.3f}us/dest-host, "
                     "W_est={:.0f}us/s W_obs={:.0f}us/s "
                     "(dy={:.4f}, err={:.4f}, dLocal={:.0f}, dRemote={:.0f}, "
                     "dHosts={:.0f})",
                     alpha(),
                     beta(),
                     gamma(),
                     W_est,
                     W_obs,
                     y,
                     err,
                     dLocal,
                     dRemote,
                     dHosts);
    }

    // Set an RLS tuning parameter by name.
    // rls_lambda      : forgetting factor            (0.9 – 0.9999)
    // rls_p           : reset P diagonal to value    (> 0)
    // rls_w_ema       : W EMA smoothing factor       (0.001 – 0.5)
    // rls_max_step    : per-step normalized cap       (> 0.01)
    // rls_min_chained : min remote chained calls/s to run RLS; below this the
    //                   whole observation is skipped and prev frozen (0 = off)
    // rls_y_norm      : Y_NORM      normalisation constant         (> 0)
    // rls_chain_norm  : CHAIN_NORM  normalisation constant (alpha) (> 0)
    // rls_remote_norm : REMOTE_NORM normalisation constant (beta)  (> 0)
    // rls_host_norm   : HOST_NORM   normalisation constant (gamma) (> 0)
    // Returns false if key is unrecognized.
    bool setRlsParam(const std::string& key, double value)
    {
        if (key == "rls_lambda") {
            lambda = std::clamp(value, 0.9, 0.9999);
        } else if (key == "rls_p") {
            double v = std::max(value, 1e-6);
            for (int i = 0; i < 9; i++)
                P[i] = 0.0;
            P[0] = v;
            P[4] = v;
            P[8] = v;
        } else if (key == "rls_w_ema") {
            wEma = std::clamp(value, 0.001, 0.5);
        } else if (key == "rls_max_step") {
            maxRlsStep = std::max(value, 0.01);
        } else if (key == "rls_min_chained") {
            minChainedForRls = std::max(value, 0.0);
        } else if (key == "rls_y_norm") {
            Y_NORM = std::max(value, 1e-6);
        } else if (key == "rls_chain_norm") {
            CHAIN_NORM = std::max(value, 1e-6);
        } else if (key == "rls_remote_norm") {
            REMOTE_NORM = std::max(value, 1e-6);
        } else if (key == "rls_host_norm") {
            HOST_NORM = std::max(value, 1e-6);
        } else {
            return false;
        }
        return true;
    }

    // Set a physical coefficient by name
    // ("coeff_a", "coeff_b", "coeff_c", "coeff_g").
    // Returns false if key is unrecognized.
    bool set(const std::string& key, double value)
    {
        if (key == "coeff_c") {
            W_est = value;
        } else if (key == "coeff_a") {
            alphaS = value / CHAIN_NORM;
        } else if (key == "coeff_b") {
            betaS = value / REMOTE_NORM;
        } else if (key == "coeff_g") {
            gammaS = value / HOST_NORM;
        } else if (key == "coeff_param_fix") {
            coeffParamFix = (value != 0.0);
        } else {
            return false;
        }
        return true;
    }

    // Maximum sustainable processed-requests per second.
    // execTime           : avg execution time per request (microseconds)
    // localChainedCalls  : expected local chained calls in this second
    // remoteChainedCalls : expected remote chained calls in this second
    // remoteHostCount    : expected distinct remote dest hosts in this second
    double maxProcessed(double execTime,
                        double localChainedCalls,
                        double remoteChainedCalls,
                        double remoteHostCount) const
    {
        if (execTime <= 0.0)
            return 0.0;
        double overhead = alpha() * localChainedCalls +
                          beta() * remoteChainedCalls +
                          gamma() * remoteHostCount;
        double budget = W_est - overhead;
        if (budget <= 0.0)
            return 0.0;
        return budget / execTime;
    }
};

}
