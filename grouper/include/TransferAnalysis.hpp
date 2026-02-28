#ifndef TRANSFER_ANALYSIS_HPP
#define TRANSFER_ANALYSIS_HPP

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <numeric>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "PlanDAG.hpp"
#include "QueryPlan.pb.h"
#include "WorkItem.pb.h"

namespace transfer_analysis {

// ---------------------------------------------------------------------------
// Hardware Configuration  (defaults from Intel MLC measurements)
//
//   MLC bandwidth matrix (MB/s):         MLC latency matrix (ns):
//     Node    0         1         2         Node    0       1       2
//   0  186139.5  182733.4   52517.5       0  115.8   135.9   311.5
//   1  181170.0  186135.9   52515.2       1  140.1   111.7   325.3
//
//   Node 2 = CXL device
// ---------------------------------------------------------------------------

struct CXLSystemConfig {
    // CXL link (Node 0/1 -> Node 2)
    double cxl_bandwidth_GBps   = 52.5;    // ~52,5 GB/s
    double cxl_latency_ns       = 320;   // Node 0 -> Node 2

    // Local DRAM (Node 0 -> Node 0)
    double dram_bandwidth_GBps  = 186.0;   // ~186 GB/s
    double dram_latency_ns      = 120;

    // CXL -> DRAM memcpy (streaming copy with NT stores)
    // Bottlenecked by CXL read; ~20% loss from write side + TLB faults.
    double transfer_bandwidth_GBps = 42.0;
    double transfer_setup_ns       = 500.0; // TLB warmup, prefetcher ramp-up
};

// ---------------------------------------------------------------------------
// Depth computation
// ---------------------------------------------------------------------------

struct DepthResult {
    std::unordered_map<uint32_t, uint32_t> item_depths;
    uint32_t max_depth = 0;
};

DepthResult computeDepths(const QueryPlan& plan);

// ---------------------------------------------------------------------------
// System-wide CXL load profile
// ---------------------------------------------------------------------------

struct SystemLoadProfile {
    std::unordered_map<size_t, size_t> bytes_at_depth;
    size_t peak_depth_demand = 0;
    size_t peak_depth        = 0;

    void recomputePeak();
};

SystemLoadProfile buildSystemLoadProfile(
    const std::vector<std::vector<size_t>>& groups,
    const std::vector<plan::PlanDAG>&       dags,
    const std::unordered_map<std::string, size_t>& schema);

// ---------------------------------------------------------------------------
// Per-column, per-GROUP access information
// ---------------------------------------------------------------------------

struct GroupColumnAccess {
    std::string  col_name;
    ColumnType   col_type;
    size_t       column_size_bytes = 0;
    size_t       access_count      = 0;
    size_t       earliest_depth    = 0;
    size_t       max_depth         = 0;
    std::vector<size_t> access_depths;
};

struct GroupTransferContext {
    size_t   group_idx        = 0;
    uint64_t target_cu_uuid   = 0;
    std::unordered_map<std::string, GroupColumnAccess> column_accesses;
};

// ---------------------------------------------------------------------------
// Cost estimation
// ---------------------------------------------------------------------------

struct TransferCostEstimate {
    double cost_without_ns;   // total time if column stays in CXL
    double cost_with_ns;      // total time if transferred to DRAM first
    double saving_ns;         // positive = transfer is beneficial
};

TransferCostEstimate estimateTransferCost(
    const GroupColumnAccess& info,
    const SystemLoadProfile& system_load,
    uint64_t                 chunk_size,
    const CXLSystemConfig&   hw);

// ---------------------------------------------------------------------------
// Context building
// ---------------------------------------------------------------------------

std::vector<GroupTransferContext> buildGroupContexts(
    const std::vector<std::vector<size_t>>&            groups,
    const std::vector<plan::PlanDAG>&                  dags,
    const std::vector<plan::BasColTypeMap>&             typeMaps,
    const std::unordered_map<std::string, size_t>&     schema,
    const std::vector<std::pair<std::string, uint64_t>>& cuUuids);

// ---------------------------------------------------------------------------
// Selection
// ---------------------------------------------------------------------------

std::unordered_set<std::string> selectTransfersForGroup(
    GroupTransferContext&       ctx,
    const SystemLoadProfile&   system_load,
    uint64_t                   chunk_size,
    const CXLSystemConfig&     hw);

std::vector<std::unordered_set<std::string>> decideTransfers(
    const std::vector<std::vector<size_t>>&            groups,
    const std::vector<plan::PlanDAG>&                  dags,
    const std::vector<const plan::BasColTypeMap*>&     typeMaps,
    const std::unordered_map<std::string, size_t>&     schema,
    const std::vector<std::pair<std::string, uint64_t>>& cuUuids,
    uint64_t                                           chunk_size,
    const CXLSystemConfig&                             hw = CXLSystemConfig{});

}  // namespace transfer_analysis

#endif  // TRANSFER_ANALYSIS_HPP