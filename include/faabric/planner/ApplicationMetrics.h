#pragma once

#include <faabric/planner/CoeffEstimator.h>
#include <faabric/planner/InstanceMetrics.h>
#include <faabric/planner/LatencyHistogramConfig.h>
#include <faabric/planner/planner.pb.h>
#include <faabric/proto/faabric.pb.h>
#include <faabric/scheduler/InstancesRuntimeStats.h>
#include <faabric/util/clock.h>
#include <faabric/util/locks.h>
#include <faabric/util/logging.h>
#include <faabric/util/string_tools.h>

#include <algorithm>
#include <atomic>
#include <cmath>
#include <cstdint>
#include <limits>
#include <map>
#include <memory>
#include <rapidjson/document.h>
#include <rapidjson/prettywriter.h>
#include <rapidjson/stringbuffer.h>
#include <rapidjson/writer.h>
#include <set>
#include <shared_mutex>
#include <string>
#include <vector>

namespace faabric::planner {

// ModeFlux, planner: what one stats round cost -- CPU time (ns) of building
// the snapshot, the offload advice and the merge advice; the round's wall
// time (us) from asking the workers to the advice being ready; and the
// bytes sent to and received from the workers.
struct FluxControlPlaneRound
{
    int workers = 0;
    int64_t snapshotCpuNs = 0;
    int64_t offloadCpuNs = 0;
    int64_t mergeCpuNs = 0;
    int64_t wallUs = 0;
    int64_t bytesOut = 0;
    int64_t bytesIn = 0;
};

class ApplicationMetrics
{
  public:
    ApplicationMetrics(std::string appNameIn, int periodIn)
      : appName(appNameIn)
      , period(periodIn) {};

    void record(const std::map<int, std::shared_ptr<faabric::Message>>& msgMap,
                int runningReqs,
                int64_t recordStartTime)
    {
        int msgMapSize = msgMap.size();
        if (msgMapSize == 0) {
            return;
        }

        faabric::util::FullLock lock(opMx);

        avgExecutionOperators =
          avgExecutionOperators +
          (static_cast<double>(msgMapSize) - avgExecutionOperators) /
            (count + 1);
        avgRunningReqs =
          avgRunningReqs +
          (static_cast<double>(runningReqs) - avgRunningReqs) / (count + 1);

        int64_t tempStartTime = std::numeric_limits<int64_t>::max();
        int64_t tempEndTime = std::numeric_limits<int64_t>::min();
        // When the input reached the planner, before any admission wait.
        // Every request it sets off inherits it.
        int64_t tempArrivalTime = std::numeric_limits<int64_t>::max();
        // Whether any request it set off was migrated or forwarded.
        bool anyForwarded = false;
        for (auto& [msgId, msg] : msgMap) {
            if (msg->forwardcount() > 0) {
                anyForwarded = true;
            }
            std::string operatorName = getOperatorName(msg);
            std::string instanceName = getInstanceName(msg);
            if (!instances.contains(instanceName)) {
                instances[instanceName] =
                  std::make_unique<InstanceMetrics>(instanceName, period);
            }
            instances[instanceName]->record(msg);

            // Update the edge weight map based on chained messages.
            for (int32_t chainedMsgId : msg->chainedmsgids()) {
                if (!msgMap.count(chainedMsgId)) {
                    continue;
                }
                auto chainedMsg = msgMap.at(chainedMsgId);
                std::string chainedOpt = getOperatorName(chainedMsg);
                edgeWeightMap[operatorName][chainedOpt]++;
            }

            if (msg->plannerqueuetime() < tempStartTime) {
                tempStartTime = msg->plannerqueuetime();
            }
            if (msg->inputarrivaltime() > 0 &&
                msg->inputarrivaltime() < tempArrivalTime) {
                tempArrivalTime = msg->inputarrivaltime();
            }
            if (msg->workerexecuteend() > tempEndTime) {
                tempEndTime = msg->workerexecuteend();
            }
        }
        count++;
        int64_t tempLatencyMicro = tempEndTime - tempStartTime;

        // Latency in microseconds is less than 10ms (10,000 us)
        if (tempLatencyMicro < 10000) {
            latenciesUnderTen[static_cast<int>(tempLatencyMicro)]++;
        } else {
            // Otherwise, record in milliseconds
            int tempLatencyMilli = tempLatencyMicro / 1000;
            latenciesOverTen[tempLatencyMilli]++;
        }

        int64_t tempTotalLatencyMicro;
        if (recordStartTime > 0) {
            tempTotalLatencyMicro =
              faabric::util::getGlobalClock().epochMicros() - recordStartTime;
        } else {
            tempTotalLatencyMicro =
              faabric::util::getGlobalClock().epochMicros() - tempStartTime;
        }
        // Latency in microseconds is less than 10ms (10,000 us)
        if (tempTotalLatencyMicro < 10000) {
            totalLatenciesUnderTen[static_cast<int>(tempTotalLatencyMicro)]++;
        } else {
            // Otherwise, record in milliseconds
            int tempLatencyMilli = tempTotalLatencyMicro / 1000;
            totalLatenciesOverTen[tempLatencyMilli]++;
        }

        // Arrival latency: from the input's arrival at the planner to the end
        // of the last request it set off. Unlike the two above, it counts the
        // wait for admission.
        bool hasArrival =
          tempArrivalTime != std::numeric_limits<int64_t>::max() &&
          tempEndTime >= tempArrivalTime;
        int64_t arrivalLatencyMicro =
          hasArrival ? tempEndTime - tempArrivalTime : 0;
        if (hasArrival) {
            if (arrivalLatencyMicro < 10000) {
                arrivalLatenciesUnderTen[static_cast<int>(
                  arrivalLatencyMicro)]++;
            } else {
                arrivalLatenciesOverTen[static_cast<int>(arrivalLatencyMicro /
                                                         1000)]++;
            }
            // The same again, split by whether migration touched the input.
            addLatency(anyForwarded ? arrivalForwardedLatencies
                                    : arrivalDirectLatencies,
                       arrivalLatencyMicro);

            // And again for the run once warmed up: inputs that reached the
            // planner stableWarmupSec or more after the first one did.
            firstArrivalTime = std::min(firstArrivalTime, tempArrivalTime);
            if (tempArrivalTime - firstArrivalTime >=
                static_cast<int64_t>(stableWarmupSec) * 1000000) {
                addLatency(arrivalStableLatencies, arrivalLatencyMicro);
                addLatency(anyForwarded ? arrivalForwardedStableLatencies
                                        : arrivalDirectStableLatencies,
                           arrivalLatencyMicro);
            }
        }

        if (tempStartTime < startTime) {
            startTime = tempStartTime;
        }
        if (tempEndTime > endTime) {
            endTime = tempEndTime;
        }

        if (tempEndTime != std::numeric_limits<int64_t>::min()) {
            // tempEndTime is in microseconds.
            int64_t secondKey = tempEndTime / 1000000;

            runtimeMetricsHistory[secondKey].init(histConfig.totalBins);
            runtimeMetricsHistory[secondKey].count++;

            int latencyMs = static_cast<int>(tempTotalLatencyMicro / 1000);
            int binIdx = histConfig.getBinIdx(latencyMs);
            runtimeMetricsHistory[secondKey].latencyBins[binIdx]++;
            if (hasArrival) {
                auto& record = runtimeMetricsHistory[secondKey];
                record.arrivalLatencyBins[histConfig.getBinIdx(
                  static_cast<int>(arrivalLatencyMicro / 1000))]++;
                record.arrivalCount++;
            }

            // Accumulate the per-second average latency separately: this map
            // is never trimmed, so the percentiles below always cover the
            // whole run rather than the trailing window kept in
            // runtimeMetricsHistory.
            auto& secondAccum = latencyBySecond[secondKey];
            secondAccum.sumMicros += tempTotalLatencyMicro;
            secondAccum.count++;
        }
    }

    std::map<std::string, long> getWorkloads()
    {
        faabric::util::FullLock lock(opMx);

        std::map<std::string, long> workloads;
        for (const auto& [instName, instancePtr] : instances) {
            workloads[instName] = instancePtr->getCount();
        }

        return workloads;
    }

    std::map<std::string, long> getOptWorkloads()
    {
        faabric::util::FullLock lock(opMx);

        std::map<std::string, long> instWorkloadMap;
        for (const auto& [instName, instancePtr] : instances) {
            instWorkloadMap[instName] = instancePtr->getCount();
        }

        std::map<std::string, long> operatorWorkloadMap;

        for (const auto& [instName, count] : instWorkloadMap) {
            auto userFuncParTuple = util::splitUserFuncPar(instName);
            std::string funcName = std::get<0>(userFuncParTuple) + "_" +
                                   std::get<1>(userFuncParTuple);
            operatorWorkloadMap[funcName] += count;
        }

        return operatorWorkloadMap;
    }

    std::map<std::string, std::map<std::string, int>> getEdgeWeightMap() const
    {
        faabric::util::FullLock lock(opMx);
        return edgeWeightMap;
    }

    struct RuntimeMetricsRecord
    {
        int count = 0;
        std::map<std::string, int> hostQueueSize; // <host_ip, total_queue_size>

        // Currently the initialization size is 1000, which means recording
        // latencies up to 1 second in microsecond granularity.
        std::vector<int> latencyBins;
        // The same, from each input's arrival at the planner -- admission
        // wait included -- to the end of the last request it set off. Only
        // inputs carrying an arrival time are counted, in arrivalCount.
        std::vector<int> arrivalLatencyBins;
        int arrivalCount = 0;
        void init(int totalBins)
        {
            if (latencyBins.empty()) {
                latencyBins.resize(totalBins, 0);
            }
            if (arrivalLatencyBins.empty()) {
                arrivalLatencyBins.resize(totalBins, 0);
            }
        }
        int workersNum = 0;
        // Executors running cluster-wide during this second, summed over hosts.
        int totalExecutors = 0;
        std::atomic<int> inputRate = 0;
        int waitingQueueSize = 0; // planner waiting queue length snapshot
        int64_t waitingQueueAgeMicros = 0; // age of oldest waiting message (us)
        bool plannerQueueSaturated = false;
        // CoeffEstimator snapshot at this second
        double coeffC = 0.0; // physical CPU budget C (us/s)
        double alpha = 0.0; // local chained-call overhead coefficient (us/call)
        double beta = 0.0; // remote chained-call overhead coefficient (us/call)
        double gamma =
          0.0; // per-dest-host fixed overhead coefficient (us/host)
        double maxProcessed = 0.0; // C / avgExecTime — baseline max req/s
        double avgExecTime =
          0.0; // weighted avg exec time across instances (us)
        double avgChainedRatio = 0.0; // chained calls per processed request
        double avgLocalChainedRatio =
          0.0; // local chained calls per processed request
        double avgRemoteChainedRatio =
          0.0; // remote chained calls per processed request
        double avgNumDestHosts = 0.0; // avg distinct dest hosts per worker
        // Each entry records one coeffEstimator.update() call (one per
        // saturated host per second): throughput / execTime / local / remote /
        // distinct remote hosts.
        struct RlsUpdateEntry
        {
            double throughput;
            double execTime;
            double localChained;
            double remoteChained;
            double remoteHosts;
        };
        std::vector<RlsUpdateEntry> rlsUpdates;
    };

    // ModeFlux: per worker and per second, what migration and forwarding
    // cost (fluxMigrations), and totals per worker and for the cluster
    // (fluxMigrationSummary), also logged. The shares put them next to the
    // work done: requests moved, and requests sent on because routing was
    // off, per request processed. Caller holds opMx.
    // ModeFlux: what scheduling, rebalancing and the stats rounds cost --
    // per worker as each reports it, and on the planner per round -- with
    // their totals and averages. New keys only: nothing above changes.
    // Caller holds opMx.
    void addFluxOverheadMetrics(rapidjson::Document& doc) const
    {
        auto& alloc = doc.GetAllocator();

        // ---- Workers ----
        rapidjson::Value perWorkerObj(rapidjson::kObjectType);
        rapidjson::Value summaryObj(rapidjson::kObjectType);
        faabric::FluxOverheadStats clusterTotal;
        long long clusterBytesSum = 0;
        long long clusterBytesReports = 0;
        int64_t clusterBytesMax = 0;
        bool anyWorker = false;

        auto summarise = [&alloc](const faabric::FluxOverheadStats& total,
                                  long long bytesSum,
                                  long long bytesReports,
                                  int64_t bytesMax) {
            rapidjson::Value obj(rapidjson::kObjectType);
            for (const auto& field : fluxOverheadFields()) {
                obj.AddMember(rapidjson::StringRef(field.name),
                              static_cast<int64_t>((total.*field.get)()),
                              alloc);
            }
            auto ratio = [](double num, double den) {
                return den > 0 ? num / den : 0.0;
            };
            obj.AddMember(
              "routing_us_per_request",
              ratio(total.routingcpuns() / 1000.0, total.routedrequests()),
              alloc);
            obj.AddMember(
              "rebalance_us_per_round",
              ratio(total.rebalancecpuns() / 1000.0, total.rebalancerounds()),
              alloc);
            obj.AddMember(
              "stats_us_per_round",
              ratio(total.statscpuns() / 1000.0, total.statsrounds()),
              alloc);
            obj.AddMember("stats_bytes_mean",
                          ratio(static_cast<double>(bytesSum), bytesReports),
                          alloc);
            obj.AddMember("stats_bytes_max", bytesMax, alloc);
            double handovers = static_cast<double>(total.handovers());
            obj.AddMember("prepare_us_per_handover",
                          ratio(total.prepareus(), handovers),
                          alloc);
            obj.AddMember(
              "wait_us_per_handover", ratio(total.waitus(), handovers), alloc);
            obj.AddMember(
              "pack_us_per_handover", ratio(total.packus(), handovers), alloc);
            obj.AddMember(
              "send_us_per_handover", ratio(total.sendus(), handovers), alloc);
            obj.AddMember(
              "flip_us_per_handover", ratio(total.flipus(), handovers), alloc);
            obj.AddMember("forward_us_per_handover",
                          ratio(total.forwardus(), handovers),
                          alloc);
            obj.AddMember("parked_per_handover",
                          ratio(total.parkedrequests(), handovers),
                          alloc);
            return obj;
        };

        for (const auto& [ip, workerNode] : clusterWorkerMetrics) {
            if (workerNode.fluxOverheadHistory.empty()) {
                continue;
            }
            anyWorker = true;

            rapidjson::Value workerArr(rapidjson::kArrayType);
            faabric::FluxOverheadStats previous;
            faabric::FluxOverheadStats total;
            long long bytesSum = 0;
            long long bytesReports = 0;
            int64_t bytesMax = 0;
            for (const auto& [second, report] :
                 workerNode.fluxOverheadHistory) {
                // What the counters grew by since the report before; one
                // that went down was reset by a flush, so all of it is new.
                faabric::FluxOverheadStats delta;
                for (const auto& field : fluxOverheadFields()) {
                    int64_t now = (report.*field.get)();
                    int64_t before = (previous.*field.get)();
                    (delta.*field.set)(now >= before ? now - before : now);
                    (total.*field.set)((total.*field.get)() +
                                       (delta.*field.get)());
                }
                previous = report;
                int64_t bytes = report.laststatsbytes();
                if (bytes > 0) {
                    bytesSum += bytes;
                    bytesReports++;
                    bytesMax = std::max(bytesMax, bytes);
                }

                std::string entry = std::to_string(second);
                for (const auto& field : fluxOverheadFields()) {
                    int64_t value = (delta.*field.get)();
                    // CPU time reads better in microseconds.
                    if (field.nanos) {
                        value /= 1000;
                    }
                    entry += " / " + std::to_string(value);
                }
                entry += " / " + std::to_string(bytes);
                workerArr.PushBack(
                  rapidjson::Value(entry.c_str(), alloc).Move(), alloc);
            }

            perWorkerObj.AddMember(
              rapidjson::Value(ip.c_str(), alloc).Move(), workerArr, alloc);
            summaryObj.AddMember(
              rapidjson::Value(ip.c_str(), alloc).Move(),
              summarise(total, bytesSum, bytesReports, bytesMax),
              alloc);
            for (const auto& field : fluxOverheadFields()) {
                (clusterTotal.*field.set)((clusterTotal.*field.get)() +
                                          (total.*field.get)());
            }
            clusterBytesSum += bytesSum;
            clusterBytesReports += bytesReports;
            clusterBytesMax = std::max(clusterBytesMax, bytesMax);
        }

        if (anyWorker) {
            summaryObj.AddMember("cluster",
                                 summarise(clusterTotal,
                                           clusterBytesSum,
                                           clusterBytesReports,
                                           clusterBytesMax),
                                 alloc);
            doc.AddMember("fluxOverhead: second / routing_cpu_us / routed / "
                          "rebalance_cpu_us / rebalance_rounds / stats_cpu_us "
                          "/ stats_rounds / handovers / prepare_us / wait_us / "
                          "pack_us / send_us / flip_us / forward_us / parked "
                          "/ stats_bytes",
                          perWorkerObj,
                          alloc);
            doc.AddMember("fluxOverheadSummary", summaryObj, alloc);
        }

        // ---- Planner ----
        if (fluxControlPlaneRounds.empty() && plannerRouting.empty()) {
            return;
        }
        rapidjson::Value roundsArr(rapidjson::kArrayType);
        struct MeanMax
        {
            double sum = 0;
            double max = 0;
            void add(double v)
            {
                sum += v;
                max = std::max(max, v);
            }
        };
        MeanMax snapshotUs, offloadUs, mergeUs, wallMs, bytesOut, bytesIn;
        for (const auto& [second, round] : fluxControlPlaneRounds) {
            std::string entry =
              std::to_string(second) + " / " + std::to_string(round.workers) +
              " / " + std::to_string(round.snapshotCpuNs / 1000) + " / " +
              std::to_string(round.offloadCpuNs / 1000) + " / " +
              std::to_string(round.mergeCpuNs / 1000) + " / " +
              std::to_string(round.wallUs / 1000) + " / " +
              std::to_string(round.bytesOut) + " / " +
              std::to_string(round.bytesIn);
            roundsArr.PushBack(rapidjson::Value(entry.c_str(), alloc).Move(),
                               alloc);
            snapshotUs.add(round.snapshotCpuNs / 1000.0);
            offloadUs.add(round.offloadCpuNs / 1000.0);
            mergeUs.add(round.mergeCpuNs / 1000.0);
            wallMs.add(round.wallUs / 1000.0);
            bytesOut.add(static_cast<double>(round.bytesOut));
            bytesIn.add(static_cast<double>(round.bytesIn));
        }
        doc.AddMember("fluxControlPlane: second / workers / snapshot_cpu_us / "
                      "offload_cpu_us / merge_cpu_us / round_wall_ms / "
                      "bytes_out / bytes_in",
                      roundsArr,
                      alloc);

        rapidjson::Value routingArr(rapidjson::kArrayType);
        int64_t routingNs = 0;
        int64_t routedInputs = 0;
        for (const auto& [second, routing] : plannerRouting) {
            std::string entry = std::to_string(second) + " / " +
                                std::to_string(routing.cpuNs / 1000) + " / " +
                                std::to_string(routing.inputs);
            routingArr.PushBack(rapidjson::Value(entry.c_str(), alloc).Move(),
                                alloc);
            routingNs += routing.cpuNs;
            routedInputs += routing.inputs;
        }
        doc.AddMember(
          "fluxPlannerRouting: second / cpu_us / inputs", routingArr, alloc);

        rapidjson::Value planner(rapidjson::kObjectType);
        double rounds = static_cast<double>(fluxControlPlaneRounds.size());
        auto mean = [rounds](const MeanMax& m) {
            return rounds > 0 ? m.sum / rounds : 0.0;
        };
        planner.AddMember("rounds", static_cast<int64_t>(rounds), alloc);
        planner.AddMember("snapshot_cpu_us_mean", mean(snapshotUs), alloc);
        planner.AddMember("snapshot_cpu_us_max", snapshotUs.max, alloc);
        planner.AddMember("offload_cpu_us_mean", mean(offloadUs), alloc);
        planner.AddMember("offload_cpu_us_max", offloadUs.max, alloc);
        planner.AddMember("merge_cpu_us_mean", mean(mergeUs), alloc);
        planner.AddMember("merge_cpu_us_max", mergeUs.max, alloc);
        planner.AddMember("round_wall_ms_mean", mean(wallMs), alloc);
        planner.AddMember("round_wall_ms_max", wallMs.max, alloc);
        planner.AddMember("bytes_out_mean", mean(bytesOut), alloc);
        planner.AddMember("bytes_out_max", bytesOut.max, alloc);
        planner.AddMember("bytes_in_mean", mean(bytesIn), alloc);
        planner.AddMember("bytes_in_max", bytesIn.max, alloc);
        planner.AddMember("routing_us_per_input",
                          routedInputs > 0 ? routingNs / 1000.0 /
                                               static_cast<double>(routedInputs)
                                           : 0.0,
                          alloc);
        planner.AddMember("routed_inputs", routedInputs, alloc);
        doc.AddMember("fluxControlPlaneSummary", planner, alloc);
    }

    void addFluxMigrationMetrics(rapidjson::Document& doc) const
    {
        auto& alloc = doc.GetAllocator();
        if (runtimeMetricsHistory.empty()) {
            return;
        }
        int64_t startSec = runtimeMetricsHistory.begin()->first;
        int64_t endSec = runtimeMetricsHistory.rbegin()->first;

        rapidjson::Value perSecondObj(rapidjson::kObjectType);
        rapidjson::Value perSecondExtraObj(rapidjson::kObjectType);
        rapidjson::Value summaryObj(rapidjson::kObjectType);
        faabric::FluxMigrationStats clusterTotal;
        long long clusterProcessed = 0;
        bool anyWorker = false;

        auto summarise = [&alloc](const faabric::FluxMigrationStats& total,
                                  long long processed) {
            rapidjson::Value obj(rapidjson::kObjectType);
            for (const auto* fields :
                 { &fluxCounterFields(), &fluxExtraCounterFields() }) {
                for (const auto& field : *fields) {
                    obj.AddMember(rapidjson::StringRef(field.name),
                                  static_cast<int64_t>((total.*field.get)()),
                                  alloc);
                }
            }
            obj.AddMember("held_units", total.heldunits(), alloc);
            obj.AddMember("cost_us", total.costus(), alloc);
            obj.AddMember("processed", static_cast<int64_t>(processed), alloc);
            double moved =
              static_cast<double>(total.outstatereqs() + total.outstateless());
            double extra = static_cast<double>(
              total.fwdtombstone() + total.fwdowner() + total.rerouted());
            obj.AddMember(
              "moved_share", processed > 0 ? moved / processed : 0.0, alloc);
            obj.AddMember("extra_forward_share",
                          processed > 0 ? extra / processed : 0.0,
                          alloc);
            return obj;
        };

        for (const auto& [ip, workerNode] : clusterWorkerMetrics) {
            if (workerNode.fluxMigrationHistory.empty()) {
                continue;
            }
            anyWorker = true;

            rapidjson::Value workerArr(rapidjson::kArrayType);
            rapidjson::Value workerExtraArr(rapidjson::kArrayType);
            faabric::FluxMigrationStats previous;
            faabric::FluxMigrationStats total;
            long long processed = 0;
            for (int64_t s = startSec; s <= endSec; ++s) {
                for (const auto& [instanceName, timeSeries] :
                     workerNode.instances) {
                    auto recIt = timeSeries.history.find(s);
                    if (recIt != timeSeries.history.end()) {
                        processed += recIt->second.throughput;
                    }
                }

                faabric::FluxMigrationStats delta;
                auto it = workerNode.fluxMigrationHistory.find(s);
                if (it != workerNode.fluxMigrationHistory.end()) {
                    delta = fluxMigrationDelta(it->second, previous);
                    addFluxMigration(total, delta);
                    previous = it->second;
                } else {
                    // No report this second: nothing changed that we know of.
                    delta.set_heldunits(previous.heldunits());
                }

                std::string entry;
                for (const auto& field : fluxCounterFields()) {
                    int64_t value = (delta.*field.get)();
                    // Milliseconds read better per second than microseconds.
                    if (field.get == &faabric::FluxMigrationStats::migrateus) {
                        value /= 1000;
                    }
                    entry += std::to_string(value) + " / ";
                }
                entry += std::to_string(delta.heldunits());
                workerArr.PushBack(
                  rapidjson::Value(entry.c_str(), alloc).Move(), alloc);

                std::string extraEntry;
                for (const auto& field : fluxExtraCounterFields()) {
                    if (!extraEntry.empty()) {
                        extraEntry += " / ";
                    }
                    extraEntry += std::to_string((delta.*field.get)());
                }
                workerExtraArr.PushBack(
                  rapidjson::Value(extraEntry.c_str(), alloc).Move(), alloc);
            }

            perSecondObj.AddMember(
              rapidjson::Value(ip.c_str(), alloc).Move(), workerArr, alloc);
            perSecondExtraObj.AddMember(
              rapidjson::Value(ip.c_str(), alloc).Move(),
              workerExtraArr,
              alloc);
            summaryObj.AddMember(rapidjson::Value(ip.c_str(), alloc).Move(),
                                 summarise(total, processed),
                                 alloc);

            SPDLOG_INFO("Flux migration on {}: {} hand-overs ({} failed), "
                        "{} units / {} state requests / {} stateless / {} "
                        "bytes out in {} ms, {} units / {} requests in, "
                        "{} requests sent on by stale routing, {} units held; "
                        "{} of {} processed requests moved",
                        ip,
                        total.handovers(),
                        total.failed(),
                        total.outunits(),
                        total.outstatereqs(),
                        total.outstateless(),
                        total.outstatebytes(),
                        total.migrateus() / 1000,
                        total.inunits(),
                        total.instatereqs() + total.instateless(),
                        total.fwdtombstone() + total.fwdowner() +
                          total.rerouted(),
                        total.heldunits(),
                        total.outstatereqs() + total.outstateless(),
                        processed);

            addFluxMigration(clusterTotal, total);
            clusterTotal.set_heldunits(0);
            clusterProcessed += processed;
        }

        if (!anyWorker) {
            return;
        }

        // Cluster-wide: held units add up across workers.
        int clusterHeld = 0;
        for (const auto& [ip, workerNode] : clusterWorkerMetrics) {
            if (!workerNode.fluxMigrationHistory.empty()) {
                clusterHeld +=
                  workerNode.fluxMigrationHistory.rbegin()->second.heldunits();
            }
        }
        clusterTotal.set_heldunits(clusterHeld);
        clusterTotal.set_costus(0);
        summaryObj.AddMember(
          "cluster", summarise(clusterTotal, clusterProcessed), alloc);

        doc.AddMember("fluxMigrations: handovers / failed / out_units / "
                      "out_state_reqs / out_stateless / out_state_bytes / "
                      "migrate_ms / in_units / in_state_reqs / in_stateless / "
                      "in_state_bytes / fwd_tombstone / fwd_owner / rerouted "
                      "/ held_units",
                      perSecondObj,
                      alloc);
        doc.AddMember("fluxMigrationsExtra: split_units / merge_units / "
                      "remigrations / spilled",
                      perSecondExtraObj,
                      alloc);
        doc.AddMember("fluxMigrationSummary", summaryObj, alloc);
    }

    rapidjson::Document getMetrics() const
    {
        faabric::util::FullLock lock(opMx);

        rapidjson::Document doc;
        doc.SetObject();
        auto& alloc = doc.GetAllocator();

        // Add basic application-level metrics.
        doc.AddMember(
          "appName", rapidjson::Value(appName.c_str(), alloc), alloc);
        doc.AddMember("period", period, alloc);
        doc.AddMember("count", count, alloc);
        doc.AddMember("avgRunningReqs", avgRunningReqs, alloc);
        doc.AddMember("startTime", startTime, alloc);
        doc.AddMember("endTime", endTime, alloc);
        doc.AddMember("avgExecutionOperators", avgExecutionOperators, alloc);

        // Calculate throughput as messages per second.
        double throughput = 0.0;
        if (endTime > startTime) {
            // Assuming startTime and endTime are in Microseconds.
            throughput = static_cast<double>(count) * 1e6 /
                         static_cast<double>(endTime - startTime);
        }
        doc.AddMember("throughput", throughput, alloc);

        // Compute percentiles from the latencies histogram.
        std::map<int, int> combinedLatencies = latenciesUnderTen;
        for (const auto& [latencyMilli, freq] : latenciesOverTen) {
            combinedLatencies[latencyMilli * 1000] += freq;
        }

        long totalCount = 0;
        for (const auto& [latency, freq] : combinedLatencies) {
            totalCount += freq;
        }

        int medianLatency = 0;
        int p95Latency = 0;
        int p99Latency = 0;

        if (totalCount > 0) {
            long cumCount = 0;
            // Calculate thresholds for each percentile.
            double medianThreshold = totalCount * 0.50;
            double p95Threshold = totalCount * 0.95;
            double p99Threshold = totalCount * 0.99;

            for (const auto& [latencyMicro, freq] : combinedLatencies) {
                cumCount += freq;
                if (medianLatency == 0 && cumCount >= medianThreshold) {
                    medianLatency = latencyMicro;
                }
                if (p95Latency == 0 && cumCount >= p95Threshold) {
                    p95Latency = latencyMicro;
                }
                if (p99Latency == 0 && cumCount >= p99Threshold) {
                    p99Latency = latencyMicro;
                    break; // Optimization: exit once all percentiles are found.
                }
            }
        }

        // Add computed percentiles to the JSON document.
        doc.AddMember("medianLatency", medianLatency, alloc);
        doc.AddMember("p95Latency", p95Latency, alloc);
        doc.AddMember("p99Latency", p99Latency, alloc);

        // Compute percentiles from the latencies histogram.
        std::map<int, int> combinedTotalLatencies = totalLatenciesUnderTen;
        for (const auto& [latencyMilli, freq] : totalLatenciesOverTen) {
            combinedTotalLatencies[latencyMilli * 1000] += freq;
        }

        long totalLatencyCount = 0;
        for (const auto& [latency, freq] : combinedTotalLatencies) {
            totalLatencyCount += freq;
        }

        int medianTotalLatency = 0;
        int p95TotalLatency = 0;
        int p99TotalLatency = 0;

        if (totalLatencyCount > 0) {
            long cumCount = 0;
            // Calculate thresholds for each percentile.
            double medianTotalThreshold = totalLatencyCount * 0.50;
            double p95TotalThreshold = totalLatencyCount * 0.95;
            double p99TotalThreshold = totalLatencyCount * 0.99;

            for (const auto& [latencyMicro, freq] : combinedTotalLatencies) {
                cumCount += freq;
                if (medianTotalLatency == 0 &&
                    cumCount >= medianTotalThreshold) {
                    medianTotalLatency = latencyMicro;
                }
                if (p95TotalLatency == 0 && cumCount >= p95TotalThreshold) {
                    p95TotalLatency = latencyMicro;
                }
                if (p99TotalLatency == 0 && cumCount >= p99TotalThreshold) {
                    p99TotalLatency = latencyMicro;
                    break; // Optimization: exit once all percentiles are found.
                }
            }
        }

        // Add computed percentiles to the JSON document.
        doc.AddMember("medianTotalLatency", medianTotalLatency, alloc);
        doc.AddMember("p95TotalLatency", p95TotalLatency, alloc);
        doc.AddMember("p99TotalLatency", p99TotalLatency, alloc);

        // Arrival latency (us): from each input's arrival at the planner,
        // admission wait included, to the end of the last request it set off.
        {
            std::map<int, long> combined;
            for (const auto& [latencyMicro, freq] : arrivalLatenciesUnderTen) {
                combined[latencyMicro] += freq;
            }
            for (const auto& [latencyMilli, freq] : arrivalLatenciesOverTen) {
                combined[latencyMilli * 1000] += freq;
            }
            long arrivalCountAll = 0;
            for (const auto& [latency, freq] : combined) {
                arrivalCountAll += freq;
            }
            int medianArrival = 0;
            int p95Arrival = 0;
            int p99Arrival = 0;
            if (arrivalCountAll > 0) {
                long cumCount = 0;
                for (const auto& [latencyMicro, freq] : combined) {
                    cumCount += freq;
                    if (medianArrival == 0 &&
                        cumCount >= arrivalCountAll * 0.50) {
                        medianArrival = latencyMicro;
                    }
                    if (p95Arrival == 0 && cumCount >= arrivalCountAll * 0.95) {
                        p95Arrival = latencyMicro;
                    }
                    if (p99Arrival == 0 && cumCount >= arrivalCountAll * 0.99) {
                        p99Arrival = latencyMicro;
                        break;
                    }
                }
            }
            doc.AddMember("medianArrivalLatency", medianArrival, alloc);
            doc.AddMember("p95ArrivalLatency", p95Arrival, alloc);
            doc.AddMember("p99ArrivalLatency", p99Arrival, alloc);
        }

        // Arrival latency of the inputs any of whose requests was migrated
        // or forwarded, and of those none of whose were; then all three again
        // over the run once warmed up (*Stable), leaving out the inputs that
        // reached the planner within stableWarmupSec of the first one.
        doc.AddMember("stableWarmupSec", stableWarmupSec, alloc);
        for (const auto& [prefix, histogram] :
             { std::pair{ "Forwarded", &arrivalForwardedLatencies },
               std::pair{ "Direct", &arrivalDirectLatencies },
               std::pair{ "Stable", &arrivalStableLatencies },
               std::pair{ "ForwardedStable", &arrivalForwardedStableLatencies },
               std::pair{ "DirectStable", &arrivalDirectStableLatencies } }) {
            auto [median, p95, p99, n] =
              percentilesOf(histogram->first, histogram->second);
            std::string tag(prefix);
            doc.AddMember(
              rapidjson::Value(("medianArrivalLatency" + tag).c_str(), alloc)
                .Move(),
              median,
              alloc);
            doc.AddMember(
              rapidjson::Value(("p95ArrivalLatency" + tag).c_str(), alloc)
                .Move(),
              p95,
              alloc);
            doc.AddMember(
              rapidjson::Value(("p99ArrivalLatency" + tag).c_str(), alloc)
                .Move(),
              p99,
              alloc);
            doc.AddMember(
              rapidjson::Value(("arrivalCount" + tag).c_str(), alloc).Move(),
              static_cast<int64_t>(n),
              alloc);
        }

        std::vector<int64_t> secondAvgLatencies;
        secondAvgLatencies.reserve(latencyBySecond.size());
        for (const auto& [secondKey, accum] : latencyBySecond) {
            if (accum.count > 0) {
                secondAvgLatencies.push_back(accum.sumMicros / accum.count);
            }
        }
        std::sort(secondAvgLatencies.begin(), secondAvgLatencies.end());

        rapidjson::Value latencyBySecondObj(rapidjson::kObjectType);
        latencyBySecondObj.AddMember(
          "seconds", static_cast<int64_t>(secondAvgLatencies.size()), alloc);
        latencyBySecondObj.AddMember(
          "medianLatencyUs", nearestRank(secondAvgLatencies, 0.50), alloc);
        latencyBySecondObj.AddMember(
          "p95LatencyUs", nearestRank(secondAvgLatencies, 0.95), alloc);
        latencyBySecondObj.AddMember(
          "p99LatencyUs", nearestRank(secondAvgLatencies, 0.99), alloc);
        doc.AddMember("latencyBySecond", latencyBySecondObj, alloc);

        // MODIFICATION: Add the runtime count history as a JSON array with
        // version markers.
        rapidjson::Value historyArray(rapidjson::kArrayType);

        if (!runtimeMetricsHistory.empty()) {
            int64_t startSec = runtimeMetricsHistory.begin()->first;
            int64_t endSec = runtimeMetricsHistory.rbegin()->first;

            int currentVersion = 0;
            auto it = versionHistory.upper_bound(startSec);
            if (it != versionHistory.begin()) {
                auto prevIt = it;
                --prevIt;
                currentVersion = prevIt->second;
            }

            std::string initialVStr =
              "version_" + std::to_string(currentVersion);
            rapidjson::Value initialVal;
            initialVal.SetString(
              initialVStr.c_str(), initialVStr.length(), alloc);
            historyArray.PushBack(initialVal, alloc);

            for (int64_t s = startSec; s <= endSec; ++s) {
                if (versionHistory.count(s)) {
                    int newVersion = versionHistory.at(s);

                    if (newVersion != currentVersion) {
                        currentVersion = newVersion;
                        std::string vStr =
                          "version_" + std::to_string(currentVersion);
                        rapidjson::Value vVal;
                        vVal.SetString(vStr.c_str(), vStr.length(), alloc);
                        historyArray.PushBack(vVal, alloc);
                    }
                }
                int inputRate = 0;
                int currentCount = 0;
                int totalQueueSize = 0;

                int secMedianLat = 0;
                int secP95Lat = 0;
                int secP99Lat = 0;

                int workersNum = 0;
                bool plannerSaturated = false;
                double coeffC = 0.0;
                double alpha = 0.0;
                double beta = 0.0;
                double gamma = 0.0;
                double maxProcessed = 0.0;

                if (runtimeMetricsHistory.count(s)) {
                    const auto& record = runtimeMetricsHistory.at(s);
                    inputRate = record.inputRate;
                    currentCount = record.count;

                    workersNum = record.workersNum;
                    plannerSaturated = record.plannerQueueSaturated;
                    coeffC = record.coeffC;
                    alpha = record.alpha;
                    beta = record.beta;
                    gamma = record.gamma;
                    maxProcessed = record.maxProcessed;

                    for (const auto& [hostIp, qSize] : record.hostQueueSize) {
                        totalQueueSize += qSize;
                    }

                    if (currentCount > 0) {
                        long target50 = currentCount * 0.50;
                        long target95 = currentCount * 0.95;
                        long target99 = currentCount * 0.99;

                        long cumCount = 0;
                        for (int i = 0; i < histConfig.totalBins; ++i) {
                            if (record.latencyBins[i] == 0)
                                continue;

                            cumCount += record.latencyBins[i];

                            int currentLatMs = histConfig.getLatencyMs(i);

                            if (secMedianLat == 0 && cumCount >= target50)
                                secMedianLat = currentLatMs;
                            if (secP95Lat == 0 && cumCount >= target95)
                                secP95Lat = currentLatMs;
                            if (secP99Lat == 0 && cumCount >= target99) {
                                secP99Lat = currentLatMs;
                                break;
                            }
                        }
                    }
                }

                long long totalWaitSum = 0;
                long totalWaitCount = 0;
                long long totalExecSum = 0;
                long totalExecCount = 0;

                int totalExecutors = 0;
                double totalCpuLoad = 0.0;
                int hostCountWithCpu = 0;

                for (const auto& [ip, workerNode] : clusterWorkerMetrics) {
                    for (const auto& [instanceName, timeSeries] :
                         workerNode.instances) {
                        auto it = timeSeries.history.find(s);
                        if (it != timeSeries.history.end()) {
                            const auto& metrics = it->second;

                            totalWaitSum += static_cast<long long>(
                                              metrics.workerQueueTime.average) *
                                            metrics.workerQueueTime.count;
                            totalWaitCount += metrics.workerQueueTime.count;

                            totalExecSum += static_cast<long long>(
                                              metrics.workerExecTime.average) *
                                            metrics.workerExecTime.count;
                            totalExecCount += metrics.workerExecTime.count;
                        }
                    }
                    auto execIt = workerNode.executorsHistory.find(s);
                    if (execIt != workerNode.executorsHistory.end()) {
                        totalExecutors += execIt->second;
                    }

                    auto cpuIt = workerNode.cpuLoadHistory.find(s);
                    if (cpuIt != workerNode.cpuLoadHistory.end()) {
                        totalCpuLoad += cpuIt->second;
                        hostCountWithCpu++;
                    }
                }

                double avgExecutors = workersNum > 0
                                        ? static_cast<double>(totalExecutors) /
                                            static_cast<double>(workersNum)
                                        : 0.0;

                int avgWaitTime =
                  totalWaitCount > 0
                    ? static_cast<int>(totalWaitSum / totalWaitCount)
                    : 0;
                int avgExecTime =
                  totalExecCount > 0
                    ? static_cast<int>(totalExecSum / totalExecCount)
                    : 0;

                double avgCpuLoad = hostCountWithCpu > 0
                                      ? (totalCpuLoad / hostCountWithCpu)
                                      : 0.0;
                char cpuBuffer[32], coeffCBuf[32], alphaBuf[32], betaBuf[32],
                  gammaBuf[32], maxProcBuf[32];
                snprintf(cpuBuffer, sizeof(cpuBuffer), "%.2f", avgCpuLoad);
                snprintf(coeffCBuf, sizeof(coeffCBuf), "%.0f", coeffC);
                snprintf(alphaBuf, sizeof(alphaBuf), "%.4f", alpha);
                snprintf(betaBuf, sizeof(betaBuf), "%.4f", beta);
                snprintf(gammaBuf, sizeof(gammaBuf), "%.4f", gamma);
                snprintf(maxProcBuf, sizeof(maxProcBuf), "%.1f", maxProcessed);

                std::string entryStr =
                  std::to_string(inputRate) + " / " +
                  std::to_string(currentCount) + " / " +
                  std::to_string(workersNum) + " / " +
                  std::to_string(totalQueueSize) + " / " +
                  std::to_string(avgWaitTime) + " / " +
                  std::to_string(avgExecTime) + " / " +
                  std::to_string(totalExecutors) + " / " +
                  std::to_string(avgExecutors) + " / " +
                  std::string(cpuBuffer) + " / " +
                  std::to_string(secMedianLat) + " / " +
                  std::to_string(secP95Lat) + " / " +
                  std::to_string(secP99Lat) + " / " +
                  (plannerSaturated ? "saturated" : "no") + " / " +
                  std::string(coeffCBuf) + " / " + std::string(alphaBuf) +
                  " / " + std::string(betaBuf) + " / " + std::string(gammaBuf) +
                  " / " + std::string(maxProcBuf);

                historyArray.PushBack(
                  rapidjson::Value(entryStr.c_str(), alloc).Move(), alloc);
            }
        }

        doc.AddMember("runtimeCountHistory: input_rate / count / worker_num / "
                      "queue_size / avg_wait / avg_exec / executors / "
                      "average_executors / avg_cpu / p50latency (ms) / "
                      "p95latency (ms) / p99latency (ms) / planner_saturated"
                      " / coeff_C / alpha / beta / gamma / max_processed",
                      historyArray,
                      alloc);

        // Arrival latency per second, in its own history so the one above
        // keeps its layout: the second (epoch) the inputs ended in, how many,
        // and their percentiles (ms).
        rapidjson::Value arrivalHistoryArray(rapidjson::kArrayType);
        for (const auto& [secondKey, record] : runtimeMetricsHistory) {
            if (record.arrivalCount <= 0) {
                continue;
            }
            long target50 = record.arrivalCount * 0.50;
            long target95 = record.arrivalCount * 0.95;
            long target99 = record.arrivalCount * 0.99;
            int secMedian = 0;
            int secP95 = 0;
            int secP99 = 0;
            long cumCount = 0;
            for (int i = 0; i < histConfig.totalBins; ++i) {
                if (record.arrivalLatencyBins[i] == 0) {
                    continue;
                }
                cumCount += record.arrivalLatencyBins[i];
                int currentLatMs = histConfig.getLatencyMs(i);
                if (secMedian == 0 && cumCount >= target50) {
                    secMedian = currentLatMs;
                }
                if (secP95 == 0 && cumCount >= target95) {
                    secP95 = currentLatMs;
                }
                if (secP99 == 0 && cumCount >= target99) {
                    secP99 = currentLatMs;
                    break;
                }
            }
            std::string entryStr = std::to_string(secondKey) + " / " +
                                   std::to_string(record.arrivalCount) + " / " +
                                   std::to_string(secMedian) + " / " +
                                   std::to_string(secP95) + " / " +
                                   std::to_string(secP99);
            arrivalHistoryArray.PushBack(
              rapidjson::Value(entryStr.c_str(), alloc).Move(), alloc);
        }
        doc.AddMember("arrivalLatencyHistory: second / count / p50latency (ms) "
                      "/ p95latency (ms) / p99latency (ms)",
                      arrivalHistoryArray,
                      alloc);

        // Optionally, if you want to include per-instance metrics, add them
        // here.
        rapidjson::Value instancesObj(rapidjson::kObjectType);
        for (const auto& [instName, instancePtr] : instances) {
            std::string instanceJson = instancePtr->getMetrics();
            SPDLOG_INFO("we got instanceJson: {}", instanceJson);
            rapidjson::Document instDoc;
            instDoc.Parse(instanceJson.c_str());
            if (instDoc.HasParseError()) {
                // Handle the parse error as needed.
                continue;
            }
            rapidjson::Value instValue;
            instValue.CopyFrom(instDoc, alloc);
            instancesObj.AddMember(
              rapidjson::Value(instName.c_str(), alloc).Move(),
              instValue,
              alloc);
        }
        doc.AddMember("instances", instancesObj, alloc);

        // Per-worker time series: throughput / num_dest_workers /
        // total_chained_count
        rapidjson::Value workerMetricDetailsObj(rapidjson::kObjectType);

        if (!runtimeMetricsHistory.empty()) {
            int64_t startSec = runtimeMetricsHistory.begin()->first;
            int64_t endSec = runtimeMetricsHistory.rbegin()->first;

            for (const auto& [ip, workerNode] : clusterWorkerMetrics) {
                rapidjson::Value workerArr(rapidjson::kArrayType);

                for (int64_t s = startSec; s <= endSec; ++s) {
                    int totalInputCount = 0;
                    int totalThroughput = 0;
                    long long queueNumWeightedSum = 0;
                    long long queueNumTotalCount = 0;
                    long long queueTimeWeightedSum = 0;
                    long long queueTimeTotalCount = 0;
                    std::map<std::string, int> aggregatedChained;

                    for (const auto& [instanceName, timeSeries] :
                         workerNode.instances) {
                        auto it = timeSeries.history.find(s);
                        if (it != timeSeries.history.end()) {
                            const auto& rec = it->second;
                            totalInputCount += rec.inputCount;
                            totalThroughput += rec.throughput;
                            if (rec.workerQueueNum.count > 0) {
                                queueNumWeightedSum +=
                                  static_cast<long long>(
                                    rec.workerQueueNum.average) *
                                  rec.workerQueueNum.count;
                                queueNumTotalCount += rec.workerQueueNum.count;
                            }
                            if (rec.workerQueueTime.count > 0) {
                                queueTimeWeightedSum +=
                                  static_cast<long long>(
                                    rec.workerQueueTime.average) *
                                  rec.workerQueueTime.count;
                                queueTimeTotalCount +=
                                  rec.workerQueueTime.count;
                            }
                            for (const auto& [dest, cnt] :
                                 rec.chainedCallHistory) {
                                aggregatedChained[dest] += cnt;
                            }
                        }
                    }
                    auto satIt = workerNode.saturatedHistory.find(s);
                    bool workerSaturated =
                      satIt != workerNode.saturatedHistory.end() &&
                      satIt->second;

                    int numDestWorkers =
                      static_cast<int>(aggregatedChained.size());
                    int totalChainedCount = 0;
                    for (const auto& [dest, cnt] : aggregatedChained) {
                        totalChainedCount += cnt;
                    }
                    double avgQueueNum =
                      queueNumTotalCount > 0
                        ? static_cast<double>(queueNumWeightedSum) /
                            queueNumTotalCount
                        : 0.0;
                    double avgQueueTime =
                      queueTimeTotalCount > 0
                        ? static_cast<double>(queueTimeWeightedSum) /
                            queueTimeTotalCount
                        : 0.0;

                    char queueNumBuf[16], queueTimeBuf[16];
                    snprintf(
                      queueNumBuf, sizeof(queueNumBuf), "%.1f", avgQueueNum);
                    snprintf(
                      queueTimeBuf, sizeof(queueTimeBuf), "%.1f", avgQueueTime);
                    std::string entry =
                      std::to_string(totalInputCount) + " / " +
                      std::to_string(totalThroughput) + " / " + queueNumBuf +
                      " / " + queueTimeBuf + " / " +
                      std::to_string(numDestWorkers) + " / " +
                      std::to_string(totalChainedCount) + " / " +
                      (workerSaturated ? "saturated" : "no");
                    workerArr.PushBack(
                      rapidjson::Value(entry.c_str(), alloc).Move(), alloc);
                }

                workerMetricDetailsObj.AddMember(
                  rapidjson::Value(ip.c_str(), alloc).Move(), workerArr, alloc);
            }
        }

        doc.AddMember(
          "workerMetricDetails: input / processed / "
          "queueNum / queueTime / num_dest_workers / total_chained_count",
          workerMetricDetailsObj,
          alloc);

        addFluxMigrationMetrics(doc);
        addFluxOverheadMetrics(doc);

        // Per-second RLS update inputs: one entry per saturated host per
        // second. Format: throughput / execTime(us) / localChained /
        // remoteChained
        rapidjson::Value rlsHistObj(rapidjson::kObjectType);
        if (!runtimeMetricsHistory.empty()) {
            int64_t startSec = runtimeMetricsHistory.begin()->first;
            int64_t endSec = runtimeMetricsHistory.rbegin()->first;
            for (int64_t s = startSec; s <= endSec; ++s) {
                auto it = runtimeMetricsHistory.find(s);
                if (it == runtimeMetricsHistory.end() ||
                    it->second.rlsUpdates.empty())
                    continue;
                rapidjson::Value arr(rapidjson::kArrayType);
                for (const auto& u : it->second.rlsUpdates) {
                    char buf[128];
                    snprintf(buf,
                             sizeof(buf),
                             "%.1f / %.1f / %.1f / %.1f",
                             u.throughput,
                             u.execTime,
                             u.localChained,
                             u.remoteChained);
                    arr.PushBack(rapidjson::Value(buf, alloc).Move(), alloc);
                }
                std::string secKey = std::to_string(s);
                rlsHistObj.AddMember(
                  rapidjson::Value(secKey.c_str(), alloc).Move(), arr, alloc);
            }
        }
        doc.AddMember("rlsUpdateHistory: throughput / execTime(us) / "
                      "localChained / remoteChained",
                      rlsHistObj,
                      alloc);

        return doc;
    }

    void setVersion(int version)
    {
        faabric::util::FullLock lock(opMx);

        // Record the version change at the current second
        int64_t secondKey =
          faabric::util::getGlobalClock().epochMicros() / 1000000;
        versionHistory[secondKey] = version;
    }

    // Thread-safe: called by multiple threads to accumulate incoming request
    // count for the current second. Uses a shared lock when the per-second
    // entry already exists (common case) and falls back to a full lock only
    // when a new entry must be created.
    void recordInputRate(int n)
    {
        int64_t secondKey = faabric::util::getGlobalClock().epochSeconds();

        {
            faabric::util::SharedLock sharedLock(opMx);
            auto it = runtimeMetricsHistory.find(secondKey);
            if (it != runtimeMetricsHistory.end()) {
                it->second.inputRate.fetch_add(n, std::memory_order_relaxed);
                return;
            }
        }

        // Entry not found — release shared lock, acquire exclusive lock to
        // insert, then increment.
        faabric::util::FullLock fullLock(opMx);
        runtimeMetricsHistory[secondKey].inputRate.fetch_add(
          n, std::memory_order_relaxed);
    }

    // Per-second snapshot for one instance on one worker.
    struct InstanceRecord
    {
        faabric::scheduler::AverageAndCount workerQueueTime;
        faabric::scheduler::AverageAndCount workerQueueNum;
        faabric::scheduler::AverageAndCount workerExecTime;
        int inputCount = 0;
        int throughput = 0;
        std::map<std::string, int> chainedCallHistory;
    };
    struct InstanceTimeSeries
    {
        std::map<time_t, InstanceRecord> history;
    };
    struct WorkerNodeMetrics
    {
        std::map<std::string, InstanceTimeSeries> instances;
        std::map<time_t, double> cpuLoadHistory;
        std::map<time_t, double> executorsHistory;
        std::map<time_t, bool> saturatedHistory;
        // ModeFlux: the worker's migration counters as reported each second
        // (cumulative since the last flush).
        std::map<time_t, faabric::FluxMigrationStats> fluxMigrationHistory;
        // ModeFlux: the worker's overhead counters as reported each second
        // (cumulative since the last flush).
        std::map<time_t, faabric::FluxOverheadStats> fluxOverheadHistory;
    };

    // The cumulative counters of FluxOverheadStats, in output order. `nanos`
    // marks CPU times, printed per second in microseconds.
    struct FluxOverheadField
    {
        const char* name;
        int64_t (faabric::FluxOverheadStats::*get)() const;
        void (faabric::FluxOverheadStats::*set)(int64_t);
        bool nanos;
    };
    static const std::vector<FluxOverheadField>& fluxOverheadFields()
    {
        using S = faabric::FluxOverheadStats;
        static const std::vector<FluxOverheadField> fields = {
            { "routing_cpu_ns", &S::routingcpuns, &S::set_routingcpuns, true },
            { "routed", &S::routedrequests, &S::set_routedrequests, false },
            { "rebalance_cpu_ns",
              &S::rebalancecpuns,
              &S::set_rebalancecpuns,
              true },
            { "rebalance_rounds",
              &S::rebalancerounds,
              &S::set_rebalancerounds,
              false },
            { "stats_cpu_ns", &S::statscpuns, &S::set_statscpuns, true },
            { "stats_rounds", &S::statsrounds, &S::set_statsrounds, false },
            { "handovers", &S::handovers, &S::set_handovers, false },
            { "prepare_us", &S::prepareus, &S::set_prepareus, false },
            { "wait_us", &S::waitus, &S::set_waitus, false },
            { "pack_us", &S::packus, &S::set_packus, false },
            { "send_us", &S::sendus, &S::set_sendus, false },
            { "flip_us", &S::flipus, &S::set_flipus, false },
            { "forward_us", &S::forwardus, &S::set_forwardus, false },
            { "parked", &S::parkedrequests, &S::set_parkedrequests, false },
        };
        return fields;
    }

    // ModeFlux, planner: one stats round's cost, and the planner's routing of
    // inputs per second -- CPU time (ns) and inputs routed.
    std::vector<std::pair<int64_t, FluxControlPlaneRound>>
      fluxControlPlaneRounds;
    struct PlannerRoutingSecond
    {
        int64_t cpuNs = 0;
        int64_t inputs = 0;
    };
    std::map<int64_t, PlannerRoutingSecond> plannerRouting;

    // The cumulative counters of FluxMigrationStats, in output order.
    struct FluxCounterField
    {
        const char* name;
        int64_t (faabric::FluxMigrationStats::*get)() const;
        void (faabric::FluxMigrationStats::*set)(int64_t);
    };

    static const std::vector<FluxCounterField>& fluxCounterFields()
    {
        using S = faabric::FluxMigrationStats;
        static const std::vector<FluxCounterField> fields = {
            { "handovers", &S::handovers, &S::set_handovers },
            { "failed", &S::failed, &S::set_failed },
            { "out_units", &S::outunits, &S::set_outunits },
            { "out_state_reqs", &S::outstatereqs, &S::set_outstatereqs },
            { "out_stateless", &S::outstateless, &S::set_outstateless },
            { "out_state_bytes", &S::outstatebytes, &S::set_outstatebytes },
            { "migrate_us", &S::migrateus, &S::set_migrateus },
            { "in_units", &S::inunits, &S::set_inunits },
            { "in_state_reqs", &S::instatereqs, &S::set_instatereqs },
            { "in_stateless", &S::instateless, &S::set_instateless },
            { "in_state_bytes", &S::instatebytes, &S::set_instatebytes },
            { "fwd_tombstone", &S::fwdtombstone, &S::set_fwdtombstone },
            { "fwd_owner", &S::fwdowner, &S::set_fwdowner },
            { "rerouted", &S::rerouted, &S::set_rerouted },
        };
        return fields;
    }

    // What the counters grew by between two reports. A counter that went
    // down was reset by a flush in between, so all of it is new.
    // Counters added later, kept out of fluxCounterFields so the per-second
    // "fluxMigrations" entries keep their layout: their own per-second
    // history ("fluxMigrationsExtra") and summary fields.
    static const std::vector<FluxCounterField>& fluxExtraCounterFields()
    {
        using S = faabric::FluxMigrationStats;
        static const std::vector<FluxCounterField> fields = {
            { "split_units", &S::splitunits, &S::set_splitunits },
            { "merge_units", &S::mergeunits, &S::set_mergeunits },
            { "remigrations", &S::remigrations, &S::set_remigrations },
            { "spilled", &S::spilled, &S::set_spilled },
        };
        return fields;
    }

    static faabric::FluxMigrationStats fluxMigrationDelta(
      const faabric::FluxMigrationStats& current,
      const faabric::FluxMigrationStats& previous)
    {
        faabric::FluxMigrationStats delta;
        for (const auto* fields :
             { &fluxCounterFields(), &fluxExtraCounterFields() }) {
            for (const auto& field : *fields) {
                int64_t now = (current.*field.get)();
                int64_t before = (previous.*field.get)();
                (delta.*field.set)(now >= before ? now - before : now);
            }
        }
        delta.set_heldunits(current.heldunits());
        delta.set_costus(current.costus());
        return delta;
    }

    static void addFluxMigration(faabric::FluxMigrationStats& total,
                                 const faabric::FluxMigrationStats& delta)
    {
        for (const auto* fields :
             { &fluxCounterFields(), &fluxExtraCounterFields() }) {
            for (const auto& field : *fields) {
                (total.*field.set)((total.*field.get)() + (delta.*field.get)());
            }
        }
        total.set_heldunits(delta.heldunits());
        total.set_costus(delta.costus());
    }

    // Adds or updates the worker stats fetched from a specific host.
    // Deserialise one InstanceMetricsResultProto into an InstanceRecord.
    // The proto carries only the latest second's data (isRuntime=true path),
    // so we read the first (and only) entry of each map.
    static void deserialiseInstanceMetrics(
      const faabric::InstanceMetricsResultProto& proto,
      InstanceRecord& rec)
    {
        if (!proto.workerqueuetimestats().empty()) {
            const auto& qt = proto.workerqueuetimestats().begin()->second;
            rec.workerQueueTime.average = qt.average();
            rec.workerQueueTime.count = qt.count();
        }
        if (!proto.workerqueuenumstats().empty()) {
            const auto& qn = proto.workerqueuenumstats().begin()->second;
            rec.workerQueueNum.average = qn.average();
            rec.workerQueueNum.count = qn.count();
        }
        if (!proto.workerexectimestats().empty()) {
            const auto& et = proto.workerexectimestats().begin()->second;
            rec.workerExecTime.average = et.average();
            rec.workerExecTime.count = et.count();
        }
        if (!proto.inputcountstats().empty()) {
            rec.inputCount = proto.inputcountstats().begin()->second;
        }
        if (!proto.throughputstats().empty()) {
            rec.throughput = proto.throughputstats().begin()->second;
        }
        if (!proto.chainedcallhistory().empty()) {
            const auto& chainedSecond =
              proto.chainedcallhistory().begin()->second;
            for (const auto& [destHost, count] : chainedSecond.hostcount()) {
                rec.chainedCallHistory[destHost] += count;
            }
        }
    }

    // ModeFlux, planner: one stats round's control-plane cost.
    void recordFluxControlPlane(const FluxControlPlaneRound& round)
    {
        faabric::util::FullLock lock(opMx);
        fluxControlPlaneRounds.emplace_back(
          faabric::util::getGlobalClock().epochSeconds(), round);
    }

    // ModeFlux, planner: routing `inputs` inputs took `cpuNs` of CPU time.
    void recordPlannerRouting(int64_t cpuNs, int64_t inputs)
    {
        faabric::util::FullLock lock(opMx);
        auto& second =
          plannerRouting[faabric::util::getGlobalClock().epochSeconds()];
        second.cpuNs += cpuNs;
        second.inputs += inputs;
    }

    void recordWorkerMetrics(
      const std::map<std::string, std::unique_ptr<faabric::WorkerStats>>&
        results)
    {
        faabric::util::FullLock lock(opMx);
        auto currentTime = faabric::util::getGlobalClock().epochSeconds();

        // Iterate hosts, update the per-worker records and coeff estimator.
        for (const auto& [ip, statsPtr] : results) {
            if (!statsPtr) {
                continue;
            }

            clusterWorkerMetrics[ip].cpuLoadHistory[currentTime] =
              statsPtr->cpuload();
            clusterWorkerMetrics[ip].executorsHistory[currentTime] =
              statsPtr->executorsnum();
            if (statsPtr->has_fluxoverhead()) {
                clusterWorkerMetrics[ip].fluxOverheadHistory[currentTime] =
                  statsPtr->fluxoverhead();
            }
            if (statsPtr->has_fluxmigration()) {
                clusterWorkerMetrics[ip].fluxMigrationHistory[currentTime] =
                  statsPtr->fluxmigration();
            }

            // Aggregate across all instances on this host before RLS update.
            double hostThroughput = 0.0;
            long long hostExecTimeSum = 0;
            long long hostExecTimeCount = 0;
            double hostLocalChainedCalls = 0.0;
            double hostRemoteChainedCalls = 0.0;
            long long hostQueueNumSum = 0;
            long long hostQueueNumCount = 0;
            long long hostQueueTimeSum = 0;
            long long hostQueueTimeCount = 0;

            for (const auto& [instanceName, metricsProto] :
                 statsPtr->workermetrics()) {
                auto& rec = clusterWorkerMetrics[ip]
                              .instances[instanceName]
                              .history[currentTime];
                deserialiseInstanceMetrics(metricsProto, rec);

                hostThroughput += rec.throughput;
                if (rec.workerExecTime.count > 0) {
                    hostExecTimeSum +=
                      static_cast<long long>(rec.workerExecTime.average) *
                      rec.workerExecTime.count;
                    hostExecTimeCount += rec.workerExecTime.count;
                }
                if (rec.workerQueueNum.count > 0) {
                    hostQueueNumSum +=
                      static_cast<long long>(rec.workerQueueNum.average) *
                      rec.workerQueueNum.count;
                    hostQueueNumCount += rec.workerQueueNum.count;
                }
                if (rec.workerQueueTime.count > 0) {
                    hostQueueTimeSum +=
                      static_cast<long long>(rec.workerQueueTime.average) *
                      rec.workerQueueTime.count;
                    hostQueueTimeCount += rec.workerQueueTime.count;
                }
            }

            // Workers no longer report chained-call counts, so the RLS
            // chained-call inputs (local, remote, remote host count) stay 0.
            double hostRemoteHostCount = 0.0;

            // Host is saturated when the host-wide weighted average queue depth
            // and wait time both exceed their thresholds.
            double hostAvgQueueNum =
              hostQueueNumCount > 0
                ? static_cast<double>(hostQueueNumSum) / hostQueueNumCount
                : 0.0;
            double hostAvgQueueTime =
              hostQueueTimeCount > 0
                ? static_cast<double>(hostQueueTimeSum) / hostQueueTimeCount
                : 0.0;
            bool hostSaturated =
              (hostAvgQueueNum > workerQueueNumThreshold) &&
              (hostAvgQueueTime > workerQueueTimeThresholdUs);

            clusterWorkerMetrics[ip].saturatedHistory[currentTime] =
              hostSaturated;

            // One RLS update per host per second, only when saturated.
            if (coeffEstimatorEnabled && hostSaturated &&
                hostThroughput > 0.0 && hostExecTimeCount > 0) {
                double avgExecTime =
                  static_cast<double>(hostExecTimeSum) / hostExecTimeCount;
                coeffEstimator.update(hostThroughput,
                                      avgExecTime,
                                      hostLocalChainedCalls,
                                      hostRemoteChainedCalls,
                                      hostRemoteHostCount);
                runtimeMetricsHistory[currentTime].rlsUpdates.push_back(
                  { hostThroughput,
                    avgExecTime,
                    hostLocalChainedCalls,
                    hostRemoteChainedCalls,
                    hostRemoteHostCount });
                SPDLOG_DEBUG(
                  "CoeffEstimator updated for host {}: alpha={:.4f}, "
                  "beta={:.4f}, gamma={:.4f} (throughput={:.0f}, "
                  "execTime={:.1f}us, localChained={:.0f}, "
                  "remoteChained={:.0f}, remoteHosts={:.0f})",
                  ip,
                  coeffEstimator.alpha(),
                  coeffEstimator.beta(),
                  coeffEstimator.gamma(),
                  hostThroughput,
                  avgExecTime,
                  hostLocalChainedCalls,
                  hostRemoteChainedCalls,
                  hostRemoteHostCount);
            }

            int totalQueueSize = 0;
            for (const auto& [instName, qSize] : statsPtr->instancequeuenum()) {
                totalQueueSize += qSize;
            }
            runtimeMetricsHistory[currentTime].hostQueueSize[ip] =
              totalQueueSize;
        }

        int workersNum = results.size();
        runtimeMetricsHistory[currentTime].workersNum = workersNum;

        // Cluster-wide executor count for this second, used by the Binpack
        // executor budget as the "old parallelism" calibration point.
        int clusterExecutors = 0;
        for (const auto& [ip, statsPtr] : results) {
            if (statsPtr) {
                clusterExecutors += statsPtr->executorsnum();
            }
        }
        runtimeMetricsHistory[currentTime].totalExecutors = clusterExecutors;

        // Snapshot CoeffEstimator state + derived scaling fields.
        {
            long long execTimeSum = 0;
            long execTimeCount = 0;
            // Chained-call totals stay 0: workers no longer report them.
            long long totalChained = 0;
            long long totalLocalChained = 0;
            long long totalRemoteChained = 0;
            long long totalThroughput = 0;
            double totalDestHosts = 0.0;
            int workerCount = 0;

            for (const auto& [workerIp, workerNode] : clusterWorkerMetrics) {
                bool workerHasData = false;
                for (const auto& [instName, timeSeries] :
                     workerNode.instances) {
                    auto it = timeSeries.history.find(currentTime);
                    if (it == timeSeries.history.end())
                        continue;
                    const auto& r = it->second;
                    if (r.workerExecTime.count > 0) {
                        execTimeSum +=
                          static_cast<long long>(r.workerExecTime.average) *
                          r.workerExecTime.count;
                        execTimeCount += r.workerExecTime.count;
                    }
                    totalThroughput += r.throughput;
                    workerHasData = true;
                }

                if (workerHasData) {
                    workerCount++;
                }
            }

            double avgExecTime =
              execTimeCount > 0
                ? static_cast<double>(execTimeSum) / execTimeCount
                : 0.0;
            double avgChainedRatio =
              totalThroughput > 0
                ? static_cast<double>(totalChained) / totalThroughput
                : 0.0;
            double avgLocalChainedRatio =
              totalThroughput > 0
                ? static_cast<double>(totalLocalChained) / totalThroughput
                : 0.0;
            double avgRemoteChainedRatio =
              totalThroughput > 0
                ? static_cast<double>(totalRemoteChained) / totalThroughput
                : 0.0;
            double avgNumDestHosts =
              workerCount > 0 ? totalDestHosts / workerCount : 0.0;

            auto& snap = runtimeMetricsHistory[currentTime];
            if (coeffEstimatorEnabled) {
                snap.coeffC = coeffEstimator.C();
                snap.alpha = coeffEstimator.alpha();
                snap.beta = coeffEstimator.beta();
                snap.gamma = coeffEstimator.gamma();
                snap.maxProcessed =
                  coeffEstimator.maxProcessed(avgExecTime, 0.0, 0.0, 0.0);
            }
            snap.avgExecTime = avgExecTime;
            snap.avgChainedRatio = avgChainedRatio;
            snap.avgLocalChainedRatio = avgLocalChainedRatio;
            snap.avgRemoteChainedRatio = avgRemoteChainedRatio;
            snap.avgNumDestHosts = avgNumDestHosts;
        }

        // Clean old metrics beyond a certain time window (e.g., 1000 seconds)
        time_t cutoffTime = currentTime - 1000;
        for (auto& [ip, workerNode] : clusterWorkerMetrics) {
            for (auto& [instanceName, timeSeries] : workerNode.instances) {
                auto& historyMap = timeSeries.history;
                while (!historyMap.empty() &&
                       historyMap.begin()->first < cutoffTime) {
                    historyMap.erase(historyMap.begin());
                }
            }

            while (!workerNode.cpuLoadHistory.empty() &&
                   workerNode.cpuLoadHistory.begin()->first < cutoffTime) {
                workerNode.cpuLoadHistory.erase(
                  workerNode.cpuLoadHistory.begin());
            }

            while (!workerNode.executorsHistory.empty() &&
                   workerNode.executorsHistory.begin()->first < cutoffTime) {
                workerNode.executorsHistory.erase(
                  workerNode.executorsHistory.begin());
            }

            while (!workerNode.saturatedHistory.empty() &&
                   workerNode.saturatedHistory.begin()->first < cutoffTime) {
                workerNode.saturatedHistory.erase(
                  workerNode.saturatedHistory.begin());
            }
        }

        while (!runtimeMetricsHistory.empty() &&
               runtimeMetricsHistory.begin()->first < cutoffTime) {
            runtimeMetricsHistory.erase(runtimeMetricsHistory.begin());
        }
    }

    struct ScalingSignals
    {
        double avgInputRate = 0.0;
        // Completed applications per second over the same window. Paired with
        // avgTotalExecutors it calibrates how much input one executor sustains.
        double avgThroughput = 0.0;
        // Cluster-wide executors running over the window.
        double avgTotalExecutors = 0.0;
        double avgQueueSize = 0.0;
        int plannerWaitingQueueSize = 0;
        int64_t plannerWaitingQueueAgeMicros = 0;
        bool plannerQueueSaturated = false;
        // CoeffEstimator-derived fields for capacity estimation
        double coeffC = 0.0;      // physical CPU budget per worker (us/s)
        double alpha = 0.0;       // local chained-call overhead (us/call)
        double beta = 0.0;        // remote chained-call overhead (us/call)
        double gamma = 0.0;       // per-dest-host fixed overhead (us/host)
        double avgExecTime = 0.0; // avg exec time per request (us)
        double avgChainedRatio =
          0.0; // total chained calls per processed request
        double avgLocalChainedRatio =
          0.0; // local chained calls per processed request
        double avgRemoteChainedRatio =
          0.0; // remote chained calls per processed request
        double avgNumDestHosts = 0.0; // avg distinct dest hosts per worker
        // totalLoad = inputRate + inputRate × chainedMultiplier
        // chainedMultiplier = chainedOperatorCount / inputOperatorCount
        double chainedMultiplier = 0.0;
    };

    // Numeric per-instance lifecycle snapshot (instanceName = User_Func_Par),
    // fed by record() from each request's payload timestamps. Consumed by the
    // mode-4 non-linear scaling model, which turns snapshot deltas into
    // scheduling-overhead-ratio samples.
    std::map<std::string, InstanceMetrics::Snapshot>
    getInstanceLifecycleSnapshots() const
    {
        faabric::util::SharedLock lock(opMx);
        std::map<std::string, InstanceMetrics::Snapshot> out;
        for (const auto& [name, inst] : instances) {
            out[name] = inst->snapshot();
        }
        return out;
    }

    ScalingSignals getScalingSignals(int windowSec) const
    {
        faabric::util::SharedLock lock(opMx);
        int64_t nowSec = faabric::util::getGlobalClock().epochSeconds();
        int totalInput = 0, totalQueue = 0, validSecs = 0;
        long totalCompleted = 0;
        long totalExecutors = 0;
        int execSecs = 0;
        double totalExecTime = 0.0, totalChainedRatio = 0.0,
               totalLocalChainedRatio = 0.0, totalRemoteChainedRatio = 0.0,
               totalDestHosts = 0.0;

        for (int64_t s = nowSec - windowSec; s < nowSec; s++) {
            auto it = runtimeMetricsHistory.find(s);
            if (it == runtimeMetricsHistory.end())
                continue;
            totalInput += it->second.inputRate.load(std::memory_order_relaxed);
            totalCompleted += it->second.count;
            // Only average over seconds that actually carry a worker-stats
            // snapshot, so a missed poll does not read as "zero executors".
            if (it->second.totalExecutors > 0) {
                totalExecutors += it->second.totalExecutors;
                execSecs++;
            }
            for (const auto& [ip, q] : it->second.hostQueueSize)
                totalQueue += q;
            totalExecTime += it->second.avgExecTime;
            totalChainedRatio += it->second.avgChainedRatio;
            totalLocalChainedRatio += it->second.avgLocalChainedRatio;
            totalRemoteChainedRatio += it->second.avgRemoteChainedRatio;
            totalDestHosts += it->second.avgNumDestHosts;
            validSecs++;
        }

        ScalingSignals sig;
        if (validSecs > 0) {
            sig.avgInputRate = (double)totalInput / validSecs;
            sig.avgThroughput = (double)totalCompleted / validSecs;
            sig.avgQueueSize = (double)totalQueue / validSecs;
            sig.avgExecTime = totalExecTime / validSecs;
            sig.avgChainedRatio = totalChainedRatio / validSecs;
            sig.avgLocalChainedRatio = totalLocalChainedRatio / validSecs;
            sig.avgRemoteChainedRatio = totalRemoteChainedRatio / validSecs;
            sig.avgNumDestHosts = totalDestHosts / validSecs;
        }
        if (execSecs > 0) {
            sig.avgTotalExecutors = (double)totalExecutors / execSecs;
        }

        // CoeffEstimator physical coefficients (current estimates, not
        // windowed). Left at 0 when the estimator is off: a coeffC of 0 is
        // what the capacity model already treats as "not warmed up yet".
        if (coeffEstimatorEnabled) {
            sig.coeffC = coeffEstimator.C();
            sig.alpha = coeffEstimator.alpha();
            sig.beta = coeffEstimator.beta();
            sig.gamma = coeffEstimator.gamma();
        }

        // Latest planner waiting queue snapshot
        if (!runtimeMetricsHistory.empty()) {
            const auto& latest = runtimeMetricsHistory.rbegin()->second;
            sig.plannerWaitingQueueSize = latest.waitingQueueSize;
            sig.plannerWaitingQueueAgeMicros = latest.waitingQueueAgeMicros;
            sig.plannerQueueSaturated = latest.plannerQueueSaturated;
        }
        return sig;
    }

    struct RuntimeRecordSummary
    {
        int count = 0;
        int workersNum = 0;
    };

    // Returns up to the last `n` records where plannerQueueSaturated == true,
    // newest first.
    std::vector<RuntimeRecordSummary> getLastNSaturatedRecords(int n) const
    {
        faabric::util::SharedLock lock(opMx);
        std::vector<RuntimeRecordSummary> result;
        result.reserve(n);
        for (auto it = runtimeMetricsHistory.rbegin();
             it != runtimeMetricsHistory.rend() && (int)result.size() < n;
             ++it) {
            if (!it->second.plannerQueueSaturated)
                continue;
            result.push_back({ it->second.count, it->second.workersNum });
        }
        return result;
    }

    // Computes the adaptive target host count using the FaaSFlow Adaptive
    // algorithm: derives avgCountPerWorker from the last N saturated records
    // and divides avgInputRate by it, clamped to [1, numHosts].
    int computeAdaptiveHostCount(double avgInputRate, int numHosts) const
    {
        double avgCountPerWorker = 0.0;
        auto saturated = getLastNSaturatedRecords(10);
        if (!saturated.empty()) {
            double total = 0.0;
            int valid = 0;
            for (const auto& rec : saturated) {
                if (rec.workersNum > 0) {
                    total += static_cast<double>(rec.count) / rec.workersNum;
                    ++valid;
                }
            }
            if (valid > 0)
                avgCountPerWorker = total / valid;
        }

        if (avgCountPerWorker <= 0.0 || avgInputRate <= 0.0)
            return numHosts;

        double totalRequired =
          std::max(1.0,
                   std::min(static_cast<double>(numHosts),
                            avgInputRate / avgCountPerWorker));
        return std::min(numHosts, static_cast<int>(std::ceil(totalRequired)));
    }

    // Called from updateRuntimeStats to snapshot the planner's waiting queue.
    // Saturation is computed here so getScalingSignals just reads the result.
    void recordQueueSnapshot(int queueSize, int64_t queueAgeMicros)
    {
        faabric::util::FullLock lock(opMx);
        int64_t currentTime = faabric::util::getGlobalClock().epochSeconds();
        auto& rec = runtimeMetricsHistory[currentTime];
        rec.waitingQueueSize = queueSize;
        rec.waitingQueueAgeMicros = queueAgeMicros;
        rec.plannerQueueSaturated =
          (queueSize > plannerWaitingQueueSizeThreshold) &&
          (queueAgeMicros > plannerWaitingQueueAgeThresholdUs);
    }

    // Dynamically update saturation thresholds (called from resetParameter).
    bool setThreshold(const std::string& key, int64_t value)
    {
        faabric::util::FullLock lock(opMx);
        if (key == "worker_queue_num_threshold") {
            workerQueueNumThreshold = static_cast<int>(value);
        } else if (key == "worker_queue_time_threshold") {
            workerQueueTimeThresholdUs = value;
        } else if (key == "planner_queue_size_threshold") {
            plannerWaitingQueueSizeThreshold = static_cast<int>(value);
        } else if (key == "planner_queue_age_threshold") {
            plannerWaitingQueueAgeThresholdUs = value;
        } else {
            return false;
        }
        SPDLOG_INFO("ApplicationMetrics: set {} = {}", key, value);
        return true;
    }

    // The RLS coefficient estimator only feeds the Binpack (mode 0) capacity
    // model -- ScalingSignals' coeffC/alpha/beta/gamma have no other reader.
    // Modes that do not use that model, ModeFlux among them, switch it off so
    // the estimator is not fitted every stats round on data nothing consumes.
    // The snapshot fields stay in place and read 0, which keeps the metrics
    // output's column layout unchanged.
    void setCoeffEstimatorEnabled(bool value)
    {
        faabric::util::FullLock lock(opMx);
        if (coeffEstimatorEnabled == value) {
            return;
        }
        coeffEstimatorEnabled = value;
        SPDLOG_INFO("ApplicationMetrics: coefficient estimator {}",
                    value ? "ON" : "OFF");
    }

    bool setCoeff(const std::string& key, double value)
    {
        faabric::util::FullLock lock(opMx);
        if (!coeffEstimator.set(key, value))
            return false;
        SPDLOG_INFO("ApplicationMetrics: set coeff {} = {:.4f}", key, value);
        return true;
    }

    bool setRlsParam(const std::string& key, double value)
    {
        faabric::util::FullLock lock(opMx);
        if (!coeffEstimator.setRlsParam(key, value))
            return false;
        SPDLOG_INFO(
          "ApplicationMetrics: set RLS param {} = {:.6f}", key, value);
        return true;
    }

    // Experiments: the *Stable arrival latencies leave out the inputs that
    // reached the planner within this many seconds of the first one.
    void setStableWarmupSec(int seconds)
    {
        faabric::util::FullLock lock(opMx);
        stableWarmupSec = std::max(0, seconds);
        SPDLOG_INFO("ApplicationMetrics: stable latencies leave out the "
                    "first {}s",
                    stableWarmupSec);
    }

    void reset()
    {
        faabric::util::FullLock lock(opMx);

        count = 0;
        startTime = std::numeric_limits<int64_t>::max();
        endTime = std::numeric_limits<int64_t>::min();
        latenciesUnderTen.clear();
        latenciesOverTen.clear();
        totalLatenciesUnderTen.clear();
        totalLatenciesOverTen.clear();
        arrivalLatenciesUnderTen.clear();
        arrivalLatenciesOverTen.clear();
        arrivalForwardedLatencies = {};
        arrivalDirectLatencies = {};
        // stableWarmupSec is a setting, and stays.
        firstArrivalTime = std::numeric_limits<int64_t>::max();
        arrivalStableLatencies = {};
        arrivalForwardedStableLatencies = {};
        arrivalDirectStableLatencies = {};
        latencyBySecond.clear();
        fluxControlPlaneRounds.clear();
        plannerRouting.clear();

        for (auto& [instName, instancePtr] : instances) {
            instancePtr->reset();
        }

        edgeWeightMap.clear();
        runtimeMetricsHistory.clear();
        versionHistory.clear();

        avgExecutionOperators = 0.0;
        avgRunningReqs = 0.0;
        coeffEstimator = CoeffEstimator{};
        clusterWorkerMetrics.clear();
    }

  private:
    mutable std::shared_mutex opMx;
    const std::string appName;
    int period;
    std::map<std::string, std::unique_ptr<InstanceMetrics>> instances;
    std::map<int, int> latenciesOverTen;
    std::map<int, int> latenciesUnderTen;
    std::map<int, int> totalLatenciesOverTen;
    std::map<int, int> totalLatenciesUnderTen;
    // Arrival latency, as latencies*/totalLatencies*: microseconds under
    // 10 ms, milliseconds above.
    std::map<int, int> arrivalLatenciesOverTen;
    std::map<int, int> arrivalLatenciesUnderTen;
    // The same split by whether any request of the input was migrated or
    // forwarded: {under 10 ms in us, above in ms}.
    std::pair<std::map<int, int>, std::map<int, int>> arrivalForwardedLatencies;
    std::pair<std::map<int, int>, std::map<int, int>> arrivalDirectLatencies;
    // All three again, of the inputs that reached the planner
    // stableWarmupSec or more after the first one (firstArrivalTime, us)
    // did.
    int stableWarmupSec = 0;
    int64_t firstArrivalTime = std::numeric_limits<int64_t>::max();
    std::pair<std::map<int, int>, std::map<int, int>> arrivalStableLatencies;
    std::pair<std::map<int, int>, std::map<int, int>>
      arrivalForwardedStableLatencies;
    std::pair<std::map<int, int>, std::map<int, int>>
      arrivalDirectStableLatencies;

    // Adds one latency to a histogram kept as {microseconds under 10 ms,
    // milliseconds above}.
    static void addLatency(
      std::pair<std::map<int, int>, std::map<int, int>>& histogram,
      int64_t latencyMicro)
    {
        if (latencyMicro < 10000) {
            histogram.first[static_cast<int>(latencyMicro)]++;
        } else {
            histogram.second[static_cast<int>(latencyMicro / 1000)]++;
        }
    }

    // {median, p95, p99, count} (us) of a latency histogram kept as
    // microseconds under 10 ms and milliseconds above.
    static std::tuple<int, int, int, long> percentilesOf(
      const std::map<int, int>& underTen,
      const std::map<int, int>& overTen)
    {
        std::map<int, long> combined;
        for (const auto& [latencyMicro, freq] : underTen) {
            combined[latencyMicro] += freq;
        }
        for (const auto& [latencyMilli, freq] : overTen) {
            combined[latencyMilli * 1000] += freq;
        }
        long total = 0;
        for (const auto& [latency, freq] : combined) {
            total += freq;
        }
        int median = 0;
        int p95 = 0;
        int p99 = 0;
        long cumCount = 0;
        for (const auto& [latencyMicro, freq] : combined) {
            cumCount += freq;
            if (median == 0 && cumCount >= total * 0.50) {
                median = latencyMicro;
            }
            if (p95 == 0 && cumCount >= total * 0.95) {
                p95 = latencyMicro;
            }
            if (p99 == 0 && cumCount >= total * 0.99) {
                p99 = latencyMicro;
                break;
            }
        }
        return { median, p95, p99, total };
    }

    // Per-second latency aggregate, one entry per wall-clock second. Feeds the
    // "latencyBySecond" percentiles, which treat each second (not each
    // request) as one sample.
    struct LatencySecondAccum
    {
        int64_t sumMicros = 0;
        long count = 0;
    };
    std::map<int64_t, LatencySecondAccum> latencyBySecond;

    // Nearest-rank percentile over an ascending-sorted vector: with 600
    // samples p50 is the 300th, p95 the 570th and p99 the 594th. Matches the
    // "first bucket whose cumulative count reaches size * p" semantics used by
    // the histogram-based percentiles.
    static int64_t nearestRank(const std::vector<int64_t>& sorted, double p)
    {
        if (sorted.empty()) {
            return 0;
        }

        long size = static_cast<long>(sorted.size());
        long idx =
          static_cast<long>(std::ceil(static_cast<double>(size) * p)) - 1;
        return sorted[std::clamp(idx, 0L, size - 1)];
    }

    long count = 0;
    int64_t startTime = std::numeric_limits<int64_t>::max();
    int64_t endTime = std::numeric_limits<int64_t>::min();
    double avgExecutionOperators = 0.0;
    double avgRunningReqs = 0.0;

    // Online RLS coefficient estimator (alpha, beta in CPU-budget model).
    CoeffEstimator coeffEstimator;
    // Whether to fit and publish it at all; see setCoeffEstimatorEnabled.
    bool coeffEstimatorEnabled = true;

    // thresholds for worker-side saturation detection
    int workerQueueNumThreshold = 500;
    int64_t workerQueueTimeThresholdUs = 100000; // microseconds

    // thresholds for planner-level waiting queue saturation
    int plannerWaitingQueueSizeThreshold = 5000;
    int64_t plannerWaitingQueueAgeThresholdUs = 500000;

    LatencyHistogramConfig histConfig;

    std::map<int64_t, RuntimeMetricsRecord> runtimeMetricsHistory;

    std::map<std::string, std::map<std::string, int>> edgeWeightMap;

    std::map<int64_t, int> versionHistory;

    // MAP<host_ip, WorkerNodeMetrics>
    std::map<std::string, WorkerNodeMetrics> clusterWorkerMetrics;

    std::string getInstanceName(const std::shared_ptr<faabric::Message> msg)
    {
        return msg->user() + "_" + msg->function() + "_" +
               std::to_string(msg->parallelismid());
    }

    std::string getOperatorName(const std::shared_ptr<faabric::Message> msg)
    {
        return msg->user() + "_" + msg->function();
    }
};

}
