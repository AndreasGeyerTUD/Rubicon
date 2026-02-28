#include "TransferAnalysis.hpp"

#include <algorithm>
#include <cmath>
#include <functional>
#include <iomanip>
#include <iostream>
#include <limits>
#include <numeric>
#include <unordered_set>

#include "Logger.hpp"

namespace transfer_analysis {

// ---------------------------------------------------------------------------
// Helper: convert bytes + bandwidth (GB/s) to nanoseconds
// ---------------------------------------------------------------------------
static inline double bytes_to_ns(double bytes, double bandwidth_GBps) {
    // bandwidth_GBps is in GB/s = 1e9 bytes/s
    // time_s = bytes / (bw * 1e9)
    // time_ns = bytes / bw  (since 1e9 cancels)
    return bytes / bandwidth_GBps;
}

// ---------------------------------------------------------------------------
// Depth computation
// ---------------------------------------------------------------------------

DepthResult computeDepths(const QueryPlan& plan) {
    DepthResult result;
    result.max_depth = 0;

    std::unordered_map<uint32_t, std::vector<uint32_t>> deps;
    for (const auto& item : plan.planitems()) {
        deps[item.itemid()] = {item.dependson().begin(), item.dependson().end()};
    }

    std::function<uint32_t(uint32_t)> getDepth = [&](uint32_t id) -> uint32_t {
        auto it = result.item_depths.find(id);
        if (it != result.item_depths.end()) return it->second;

        uint32_t d = 0;
        for (uint32_t dep : deps[id]) {
            d = std::max(d, getDepth(dep) + 1);
        }
        result.item_depths[id] = d;
        result.max_depth = std::max(result.max_depth, d);
        return d;
    };

    for (const auto& item : plan.planitems()) {
        getDepth(item.itemid());
    }

    return result;
}

// ---------------------------------------------------------------------------
// System load profile
// ---------------------------------------------------------------------------

void SystemLoadProfile::recomputePeak() {
    peak_depth_demand = 0;
    peak_depth = 0;
    for (const auto& [d, bytes] : bytes_at_depth) {
        if (bytes > peak_depth_demand) {
            peak_depth_demand = bytes;
            peak_depth = d;
        }
    }
}

SystemLoadProfile buildSystemLoadProfile(
    const std::vector<std::vector<size_t>>& groups,
    const std::vector<plan::PlanDAG>&       dags,
    const std::unordered_map<std::string, size_t>& schema)
{
    SystemLoadProfile profile;

    for (const auto& group : groups) {
        for (size_t dagIdx : group) {
            const QueryPlan& plan = dags[dagIdx].getPlan();
            auto depths = computeDepths(plan);
            const auto& base_cols = dags[dagIdx].getBaseColumns();

            for (const auto& [col_name, item_ids] : base_cols) {
                auto schema_it = schema.find(col_name);
                if (schema_it == schema.end()) continue;
                const size_t col_size = schema_it->second;

                for (uint32_t item_id : item_ids) {
                    auto depth_it = depths.item_depths.find(item_id);
                    if (depth_it == depths.item_depths.end()) continue;
                    profile.bytes_at_depth[depth_it->second] += col_size;
                }
            }
        }
    }

    profile.recomputePeak();
    return profile;
}

// ---------------------------------------------------------------------------
// Per-group context
// ---------------------------------------------------------------------------

std::vector<GroupTransferContext> buildGroupContexts(
    const std::vector<std::vector<size_t>>&            groups,
    const std::vector<plan::PlanDAG>&                  dags,
    const std::vector<const plan::BasColTypeMap*>&     typeMaps,
    const std::unordered_map<std::string, size_t>&     schema,
    const std::vector<std::pair<std::string, uint64_t>>& cuUuids)
{
    std::vector<GroupTransferContext> contexts(groups.size());

    for (size_t groupIdx = 0; groupIdx < groups.size(); ++groupIdx) {
        auto& ctx     = contexts[groupIdx];
        ctx.group_idx = groupIdx;
        ctx.target_cu_uuid = cuUuids[groupIdx % cuUuids.size()].second;

        for (size_t dagIdx : groups[groupIdx]) {
            const QueryPlan& plan = dags[dagIdx].getPlan();
            auto depths = computeDepths(plan);

            const auto& base_cols = dags[dagIdx].getBaseColumns();
            const auto& typeMap   = typeMaps[dagIdx];

            for (const auto& [col_name, item_ids] : base_cols) {
                auto& info = ctx.column_accesses[col_name];

                if (info.col_name.empty()) {
                    info.col_name = col_name;
                    info.col_type = typeMap->at(col_name);
                    info.earliest_depth = std::numeric_limits<size_t>::max();

                    auto schema_it = schema.find(col_name);
                    info.column_size_bytes = (schema_it != schema.end())
                                                ? schema_it->second : 0;
                }

                info.access_count += item_ids.size();

                if (depths.max_depth > info.max_depth) {
                    info.max_depth = depths.max_depth;
                }

                for (uint32_t item_id : item_ids) {
                    auto depth_it = depths.item_depths.find(item_id);
                    if (depth_it == depths.item_depths.end()) continue;

                    size_t d = depth_it->second;
                    info.access_depths.push_back(d);

                    if (d < info.earliest_depth) {
                        info.earliest_depth = d;
                    }
                }
            }
        }
    }

    return contexts;
}

// ---------------------------------------------------------------------------
// Cost estimation
// ---------------------------------------------------------------------------

TransferCostEstimate estimateTransferCost(
    const GroupColumnAccess& info,
    const SystemLoadProfile& system_load,
    uint64_t                 chunk_size,
    const CXLSystemConfig&   hw)
{
    TransferCostEstimate est{};
    est.cost_without_ns = 0.0;
    est.cost_with_ns    = 0.0;
    est.saving_ns       = 0.0;

    const double S = static_cast<double>(info.column_size_bytes);
    if (!(S > 0.0) || info.access_count == 0) return est;

    const double C = static_cast<double>(std::min<uint64_t>(chunk_size, info.column_size_bytes));

    // Aggregate accesses by depth (stall on first chunk paid once per depth).
    std::unordered_map<size_t, size_t> accesses_per_depth;
    accesses_per_depth.reserve(info.access_depths.size());
    if (!info.access_depths.empty()) {
        for (size_t d : info.access_depths) ++accesses_per_depth[d];
    } else {
        const size_t fallback_depth =
            (info.earliest_depth != std::numeric_limits<size_t>::max()) ? info.earliest_depth : 0;
        accesses_per_depth[fallback_depth] = info.access_count;
    }

    // ------------------------------------------------------------------
    // Per-depth contended CXL bandwidth.
    //
    // fair-share: bw_share(d) = cxl_bw × (S / total_demand(d))
    //
    // Under heavy load this drops dramatically, e.g. 50 columns of 100MB
    // competing => bw_share ≈ 1 GB/s instead of 52.5 GB/s.
    // ------------------------------------------------------------------
    auto contended_bw = [&](size_t depth) -> double {
        double total_demand = S;
        if (auto it = system_load.bytes_at_depth.find(depth); it != system_load.bytes_at_depth.end()) {
            total_demand = std::max(total_demand, static_cast<double>(it->second));
        }
        double bw = hw.cxl_bandwidth_GBps * (S / total_demand);
        return std::clamp(bw, 0.001, hw.cxl_bandwidth_GBps);
    };

    // Compute aggressiveness (uncapped — scales with log of contention).
    // This drives how much of the background copy we consider "free".
    //
    //   ratio ~1    => aggressiveness ~0   (no contention)
    //   ratio ~4    => aggressiveness ~0.5
    //   ratio ~10   => aggressiveness ~1.0
    //   ratio ~50   => aggressiveness ~1.7
    //   ratio ~200  => aggressiveness ~2.3
    //
    // Using log2(avg_ratio) so it keeps growing but with diminishing returns.
    double sum_ratio = 0.0;
    size_t n_depths  = 0;
    for (const auto& [d, _k] : accesses_per_depth) {
        double total_demand = S;
        if (auto it = system_load.bytes_at_depth.find(d); it != system_load.bytes_at_depth.end()) {
            total_demand = std::max(total_demand, static_cast<double>(it->second));
        }
        sum_ratio += std::max(1.0, total_demand / S);
        ++n_depths;
    }
    const double avg_ratio = (n_depths > 0) ? (sum_ratio / static_cast<double>(n_depths)) : 1.0;
    const double aggressiveness = (avg_ratio > 1.0) ? std::log2(avg_ratio) : 0.0;

    // ------------------------------------------------------------------
    // WITHOUT transfer: first-chunk stall from CXL at each unique depth,
    // at the contention-degraded bandwidth.
    //
    // Example: 4MiB chunk, bw_share = 1.05 GB/s => stall ≈ 3.8ms
    // ------------------------------------------------------------------
    double total_cxl_wait_ns = 0.0;
    for (const auto& [d, _k] : accesses_per_depth) {
        double bw = contended_bw(d);
        total_cxl_wait_ns += bytes_to_ns(C, bw) + hw.cxl_latency_ns;
    }

    // ------------------------------------------------------------------
    // WITH transfer:
    //
    // 1) Transfer first-chunk stall: the copy reads over the SAME
    //    contended CXL link. We model the transfer as competing at
    //    depth 0 (where it runs alongside depth-0 operators).
    //
    // 2) Background copy interference: after the first chunk, the
    //    rest of the copy streams in background. Under heavy contention
    //    this overlaps well with other work (many operators running).
    //    Under low contention there's less to overlap with.
    //
    //    interference_fraction:
    //      aggressiveness 0   => 30% of remaining copy on critical path
    //      aggressiveness 1   => 10%
    //      aggressiveness 2+  =>  3%
    //
    // 3) Operator DRAM reads: first-chunk from DRAM per unique depth.
    //    DRAM is ~3.5× faster and uncontended.
    // ------------------------------------------------------------------

    // (1) Transfer first-chunk under contention at depth 0
    const double transfer_bw = contended_bw(0);
    const double transfer_first_chunk_ns =
        hw.transfer_setup_ns + bytes_to_ns(C, transfer_bw) + hw.cxl_latency_ns;

    // (2) Background copy interference
    const double remaining_bytes = std::max(0.0, S - C);
    const double remaining_copy_ns = bytes_to_ns(remaining_bytes, transfer_bw);

    // interference_fraction decreases as contention grows:
    //   at aggr=0: 0.30 (30% on critical path — little overlap opportunity)
    //   at aggr=1: 0.10
    //   at aggr=2: 0.03
    //   at aggr=3: 0.01
    const double interference_fraction =
        0.30 * std::exp(-1.2 * aggressiveness);

    const double interference_ns = interference_fraction * remaining_copy_ns;

    // (3) DRAM first-chunk stalls (one per unique depth)
    double total_dram_wait_ns = 0.0;
    for (const auto& [_d, _k] : accesses_per_depth) {
        total_dram_wait_ns += bytes_to_ns(C, hw.dram_bandwidth_GBps) + hw.dram_latency_ns;
    }

    est.cost_without_ns = total_cxl_wait_ns;
    est.cost_with_ns    = transfer_first_chunk_ns + interference_ns + total_dram_wait_ns;
    est.saving_ns       = est.cost_without_ns - est.cost_with_ns;

    if (!std::isfinite(est.cost_without_ns)) est.cost_without_ns = std::numeric_limits<double>::infinity();
    if (!std::isfinite(est.cost_with_ns))    est.cost_with_ns    = std::numeric_limits<double>::infinity();
    if (!std::isfinite(est.saving_ns))       est.saving_ns = 0.0;

    return est;
}

// ---------------------------------------------------------------------------
// Selection for a single group
// ---------------------------------------------------------------------------

std::unordered_set<std::string> selectTransfersForGroup(
    GroupTransferContext&       ctx,
    const SystemLoadProfile&   system_load,
    uint64_t                   chunk_size,
    const CXLSystemConfig&     hw)
{
    std::vector<std::pair<std::string, double>> candidates;

    for (auto& [col_name, info] : ctx.column_accesses) {
        if (info.column_size_bytes < chunk_size) continue;
        if (info.access_count < 2) continue;

        const double S = static_cast<double>(info.column_size_bytes);
        if (!(S > 0.0)) continue;

        // Compute aggressiveness (same log2 scale as estimator).
        std::unordered_set<size_t> uniq_depths;
        uniq_depths.reserve(info.access_depths.size());
        for (size_t d : info.access_depths) uniq_depths.insert(d);
        if (uniq_depths.empty()) uniq_depths.insert(
            (info.earliest_depth != std::numeric_limits<size_t>::max()) ? info.earliest_depth : 0);

        double sum_ratio = 0.0;
        for (size_t d : uniq_depths) {
            double total_demand = S;
            if (auto it = system_load.bytes_at_depth.find(d); it != system_load.bytes_at_depth.end()) {
                total_demand = std::max(total_demand, static_cast<double>(it->second));
            }
            sum_ratio += std::max(1.0, total_demand / S);
        }
        const double avg_ratio = sum_ratio / static_cast<double>(uniq_depths.size());
        const double aggressiveness = (avg_ratio > 1.0) ? std::log2(avg_ratio) : 0.0;

        // Adaptive thresholds using exponential decay:
        //
        //   aggr=0  => min_saving = 5ms,   min_improve = 20%
        //   aggr=1  => min_saving = 2.7ms, min_improve = 11%
        //   aggr=2  => min_saving = 1.5ms, min_improve = 6%
        //   aggr=3  => min_saving = 0.8ms, min_improve = 3.3%
        //   aggr=5+ => min_saving ≈ 0.25ms,min_improve ≈ 1%
        //
        // This ensures thresholds keep dropping as contention grows,
        // rather than flooring at fixed values.
        const double min_saving_ns     = 5e6 * std::exp(-0.6 * aggressiveness);
        const double min_improve_ratio = 0.20 * std::exp(-0.6 * aggressiveness);

        auto estimate = estimateTransferCost(info, system_load, chunk_size, hw);

        const double improve_ratio =
            (estimate.cost_without_ns > 0.0)
                ? (estimate.saving_ns / estimate.cost_without_ns)
                : 0.0;

        if (estimate.saving_ns >= min_saving_ns && improve_ratio >= min_improve_ratio) {
            VLOG("  [Transfer candidate] " << col_name
                      << " | size=" << (info.column_size_bytes / (1024*1024)) << "MiB"
                      << " | accesses=" << info.access_count
                      << " | contention=" << std::fixed << std::setprecision(1) << avg_ratio << "x"
                      << " | saving=" << std::setprecision(2) << (estimate.saving_ns / 1e6) << "ms"
                      << " | without=" << (estimate.cost_without_ns / 1e6) << "ms"
                      << " | with=" << (estimate.cost_with_ns / 1e6) << "ms"
                      << std::defaultfloat);
            candidates.emplace_back(col_name, estimate.saving_ns);
        }
    }

    std::sort(candidates.begin(), candidates.end(),
        [](const auto& a, const auto& b) { return a.second > b.second; });

    std::unordered_set<std::string> selected;
    for (const auto& [name, _] : candidates) selected.insert(name);
    return selected;
}

// ---------------------------------------------------------------------------
// Top-level: iterative decisions across all groups
// ---------------------------------------------------------------------------

std::vector<std::unordered_set<std::string>> decideTransfers(
    const std::vector<std::vector<size_t>>&            groups,
    const std::vector<plan::PlanDAG>&                  dags,
    const std::vector<const plan::BasColTypeMap*>&     typeMaps,
    const std::unordered_map<std::string, size_t>&     schema,
    const std::vector<std::pair<std::string, uint64_t>>& cuUuids,
    uint64_t                                           chunk_size,
    const CXLSystemConfig&                             hw)
{
    auto system_load = buildSystemLoadProfile(groups, dags, schema);
    auto contexts    = buildGroupContexts(groups, dags, typeMaps, schema, cuUuids);

    VLOG("=== Transfer Analysis ===");
    VLOG("System CXL peak: " << (system_load.peak_depth_demand / (1024*1024))
              << " MiB at depth " << system_load.peak_depth);
    VLOG("Hardware: CXL " << hw.cxl_bandwidth_GBps << " GB/s @ "
              << hw.cxl_latency_ns << " ns"
              << " | DRAM " << hw.dram_bandwidth_GBps << " GB/s @ "
              << hw.dram_latency_ns << " ns"
              << " | Transfer " << hw.transfer_bandwidth_GBps << " GB/s");

    // Process groups heaviest-first so the most impactful decisions
    // come first. Later groups see an updated (reduced) system profile.
    std::vector<size_t> group_order(groups.size());
    std::iota(group_order.begin(), group_order.end(), 0);
    std::sort(group_order.begin(), group_order.end(),
        [&contexts](size_t a, size_t b) {
            size_t load_a = 0, load_b = 0;
            for (const auto& [_, info] : contexts[a].column_accesses)
                load_a += info.access_count * info.column_size_bytes;
            for (const auto& [_, info] : contexts[b].column_accesses)
                load_b += info.access_count * info.column_size_bytes;
            return load_a > load_b;
        });

    std::vector<std::unordered_set<std::string>> decisions(groups.size());

    for (size_t gi : group_order) {
        VLOG("\n--- Group " << gi << " (" << contexts[gi].column_accesses.size() << " columns) ---");

        decisions[gi] = selectTransfersForGroup(contexts[gi], system_load, chunk_size, hw);

        // Update system load: remove this group's operator-level CXL
        // accesses for transferred columns (they now read from DRAM),
        // but add the one-time transfer read at depth 0 since the memcpy
        // itself consumes CXL bandwidth concurrently with depth-0 operators.
        for (const auto& col_name : decisions[gi]) {
            const auto& info = contexts[gi].column_accesses.at(col_name);
            for (size_t d : info.access_depths) {
                auto it = system_load.bytes_at_depth.find(d);
                if (it != system_load.bytes_at_depth.end()) {
                    it->second = (it->second > info.column_size_bytes) ? it->second - info.column_size_bytes : 0;
                }
            }
            // The transfer itself reads the full column over CXL once.
            // Model this at depth 0 where it competes with leaf operators.
            system_load.bytes_at_depth[0] += info.column_size_bytes;
        }
        system_load.recomputePeak();

        VLOG("  Selected " << decisions[gi].size() << " transfer(s)"
                  << " | Updated peak: " << (system_load.peak_depth_demand / (1024*1024))
                  << " MiB at depth " << system_load.peak_depth);
    }

    VLOG("\n=== Transfer Analysis Complete ===");

    return decisions;
}

}  // namespace transfer_analysis