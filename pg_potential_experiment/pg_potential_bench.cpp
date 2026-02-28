// ============================================================================
// CXL vs DRAM Shared-Access Microbenchmark (Pipelined Chunked Transfer)
// ============================================================================
// Demonstrates the tradeoff between reading directly from CXL (remote NUMA)
// vs. copying in chunks to local DRAM with overlapped compute.
//
// Design:
//   - 5 GiB column of uint64_t on a remote NUMA node (CXL proxy)
//   - Each of the 48 CXL reader threads gets its own private copy on CXL
//     to prevent L3 cache sharing from masking the CXL latency/bandwidth cost
//   - 48 concurrent query threads, each computing sum over the column
//   - Sweep K (0..48, step 5): K threads read from a shared local DRAM copy,
//     (48-K) threads read directly from their private CXL copy
//   - When K > 0, a dedicated copy thread transfers data in chunks.
//     DRAM readers process each chunk as soon as it arrives (pipelined).
//   - The copy cost is fully overlapped with compute where possible.
//   - NOTE: Requires 48 x 5 GiB = 480 GiB CXL + 5 GiB DRAM
//
// Build:
//   cmake -B build -DCMAKE_BUILD_TYPE=Release && cmake --build build
//
// Run:
//   ./pg_potential_bench <cxl_node> <dram_node> [reps] [chunk_mib] [csv_file] [--scalar]
//
// Example:
//   ./pg_potential_bench 1 0 5 64 results.csv            # AVX-512 (default)
//   ./pg_potential_bench 1 0 5 64 results.csv --scalar   # scalar (no SIMD)
// ============================================================================

#include <immintrin.h>
#include <numa.h>
#include <numaif.h>

#include <algorithm>
#include <atomic>
#include <barrier>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <string>
#include <thread>
#include <vector>

// ============================================================================
// Configuration
// ============================================================================

static constexpr size_t DATA_SIZE       = 5ULL * 1024 * 1024 * 1024; // 5 GiB
static constexpr size_t NUM_ELEMENTS    = DATA_SIZE / sizeof(uint64_t);
static constexpr int    NUM_QUERIES     = 48;
static constexpr int    K_STEP          = 2;
static constexpr int    DEFAULT_REPS    = 10;
static constexpr size_t DEFAULT_CHUNK   = 64ULL * 1024 * 1024; // 64 MiB

// ============================================================================
// AVX-512 sum over uint64_t array (64-byte aligned assumed)
// ============================================================================

__attribute__((noinline))
uint64_t avx512_sum(const uint64_t* __restrict__ data, size_t count) {
    __m512i acc0 = _mm512_setzero_si512();
    __m512i acc1 = _mm512_setzero_si512();
    __m512i acc2 = _mm512_setzero_si512();
    __m512i acc3 = _mm512_setzero_si512();

    size_t i = 0;
    const size_t unrolled = count & ~(size_t)31; // 4 x 8 = 32 elements

    for (; i < unrolled; i += 32) {
        acc0 = _mm512_add_epi64(acc0, _mm512_load_si512(data + i +  0));
        acc1 = _mm512_add_epi64(acc1, _mm512_load_si512(data + i +  8));
        acc2 = _mm512_add_epi64(acc2, _mm512_load_si512(data + i + 16));
        acc3 = _mm512_add_epi64(acc3, _mm512_load_si512(data + i + 24));
    }

    acc0 = _mm512_add_epi64(_mm512_add_epi64(acc0, acc1), _mm512_add_epi64(acc2, acc3));
    uint64_t sum = _mm512_reduce_add_epi64(acc0);

    for (; i < count; i++) sum += data[i];
    return sum;
}

// ============================================================================
// Scalar sum over uint64_t array (no explicit vectorization)
// ============================================================================

__attribute__((noinline, optimize("no-tree-vectorize")))
uint64_t scalar_sum(const uint64_t* __restrict__ data, size_t count) {
    uint64_t s0 = 0, s1 = 0, s2 = 0, s3 = 0;

    size_t i = 0;
    const size_t unrolled = count & ~(size_t)3;

    for (; i < unrolled; i += 4) {
        s0 += data[i + 0];
        s1 += data[i + 1];
        s2 += data[i + 2];
        s3 += data[i + 3];
    }
    for (; i < count; i++) s0 += data[i];

    return s0 + s1 + s2 + s3;
}

// ============================================================================
// Sum function pointer type
// ============================================================================

using SumFn = uint64_t(*)(const uint64_t* __restrict__, size_t);

// ============================================================================
// Core pinning — threads 0..47 pinned to cores 0..47
// ============================================================================

static void pin_to_core(int core) {
    cpu_set_t cpuset;
    CPU_ZERO(&cpuset);
    CPU_SET(core, &cpuset);
    if (sched_setaffinity(0, sizeof(cpuset), &cpuset) != 0) {
        std::cerr << "WARNING: failed to pin thread to core " << core << "\n";
    }
}

// ============================================================================
// NUMA helpers
// ============================================================================

static uint64_t* numa_alloc_column(int node, size_t bytes) {
    void* ptr = numa_alloc_onnode(bytes, node);
    if (!ptr) {
        std::cerr << "FATAL: numa_alloc_onnode failed for " << (bytes >> 20)
                  << " MiB on node " << node << "\n";
        std::exit(1);
    }
    return reinterpret_cast<uint64_t*>(ptr);
}

static void fault_pages(uint64_t* data, size_t count, int nthreads = 16) {
    std::vector<std::thread> workers;
    const size_t chunk = count / nthreads;
    for (int t = 0; t < nthreads; t++) {
        size_t s = t * chunk, e = (t == nthreads - 1) ? count : s + chunk;
        workers.emplace_back([data, s, e]() {
            for (size_t i = s; i < e; i += 512) data[i] = 0;
        });
    }
    for (auto& th : workers) th.join();
}

static void generate_data(uint64_t* data, size_t count, int nthreads = 16) {
    std::vector<std::thread> workers;
    const size_t chunk = count / nthreads;
    for (int t = 0; t < nthreads; t++) {
        size_t s = t * chunk, e = (t == nthreads - 1) ? count : s + chunk;
        workers.emplace_back([data, s, e, t]() {
            uint64_t state = 0xBEEF'CAFE'DEAD'0000ULL ^ ((uint64_t)t << 48);
            for (size_t i = s; i < e; i++) {
                state ^= state << 13;
                state ^= state >> 7;
                state ^= state << 17;
                data[i] = state;
            }
        });
    }
    for (auto& th : workers) th.join();
}

static void verify_numa_placement(const void* ptr, int expected) {
    int actual = -1;
    if (get_mempolicy(&actual, nullptr, 0, const_cast<void*>(ptr),
                      MPOL_F_NODE | MPOL_F_ADDR) == 0) {
        if (actual != expected)
            std::cerr << "WARNING: expected node " << expected
                      << ", got node " << actual << "\n";
        else
            std::cout << "  Verified: data on NUMA node " << actual << "\n";
    }
}

// ============================================================================
// Benchmark core — pipelined chunked transfer
// ============================================================================
//
// Pipeline:
//   Copy thread:   [chunk 0] [chunk 1] [chunk 2] ...
//   DRAM reader i:       [sum 0] [sum 1] [sum 2] ...  (waits for chunk ready)
//   CXL reader j:  [------------- full column sum from CXL -------------]
//
// The copy thread signals completion of each chunk via an atomic counter.
// DRAM readers spin-wait on the counter with _mm_pause().
// ============================================================================

struct BenchResult {
    int    K;
    double total_ms;
    double copy_ms;     // wall-clock of the copy thread (overlapped with compute)
};

// ============================================================================
// Baseline: data fully resident in DRAM, no copy cost
// ============================================================================

static double run_dram_baseline(const uint64_t* dram_buf, SumFn sum_fn) {
    using Clock = std::chrono::steady_clock;

    volatile uint64_t sink = 0;
    std::vector<uint64_t> results(NUM_QUERIES, 0);
    std::barrier sync_barrier(NUM_QUERIES);

    auto t0 = Clock::now();

    std::vector<std::thread> threads;
    threads.reserve(NUM_QUERIES);

    for (int i = 0; i < NUM_QUERIES; i++) {
        threads.emplace_back([&, i]() {
            pin_to_core(i);
            sync_barrier.arrive_and_wait();
            results[i] = sum_fn(dram_buf, NUM_ELEMENTS);
        });
    }

    for (auto& th : threads) th.join();
    auto t1 = Clock::now();

    for (int i = 0; i < NUM_QUERIES; i++) sink += results[i];
    (void)sink;

    return std::chrono::duration<double, std::milli>(t1 - t0).count();
}

// ============================================================================
// Pipelined benchmark run
// ============================================================================

static BenchResult run_single(
    uint64_t* const* cxl_copies,   // per-thread private CXL copies
    const uint64_t*  cxl_master,   // canonical copy (used for CXL→DRAM transfer)
    uint64_t*        dram_buf,
    int              K,
    size_t           chunk_bytes,
    SumFn            sum_fn)
{
    using Clock = std::chrono::steady_clock;

    const size_t chunk_elems = chunk_bytes / sizeof(uint64_t);
    const size_t num_chunks  = (NUM_ELEMENTS + chunk_elems - 1) / chunk_elems;

    // Atomic counter: how many chunks have been fully copied to DRAM.
    std::atomic<size_t> chunks_ready{0};

    volatile uint64_t sink = 0;
    std::vector<uint64_t> results(NUM_QUERIES, 0);

    // Barrier: synchronize all query threads + copy thread (if K > 0)
    const int barrier_count = NUM_QUERIES + (K > 0 ? 1 : 0);
    std::barrier sync_barrier(barrier_count);

    std::atomic<double> copy_ms_out{0.0};

    auto t_start = Clock::now();

    // ---- Copy thread ----
    std::thread copy_thread;
    if (K > 0) {
        copy_thread = std::thread([&]() {
            sync_barrier.arrive_and_wait();

            auto tc0 = Clock::now();
            for (size_t c = 0; c < num_chunks; c++) {
                const size_t offset = c * chunk_elems;
                const size_t elems  = std::min(chunk_elems, NUM_ELEMENTS - offset);
                const size_t bytes  = elems * sizeof(uint64_t);

                std::memcpy(dram_buf + offset, cxl_master + offset, bytes);

                // Signal: chunk c is now safe to read from DRAM
                chunks_ready.store(c + 1, std::memory_order_release);
            }
            auto tc1 = Clock::now();

            copy_ms_out.store(
                std::chrono::duration<double, std::milli>(tc1 - tc0).count(),
                std::memory_order_relaxed);
        });
    }

    // ---- Query threads ----
    std::vector<std::thread> threads;
    threads.reserve(NUM_QUERIES);

    for (int i = 0; i < NUM_QUERIES; i++) {
        const bool use_dram = (i < K);

        threads.emplace_back([&, i, use_dram, chunk_elems, num_chunks, sum_fn]() {
            pin_to_core(i);
            sync_barrier.arrive_and_wait();

            if (!use_dram) {
                // CXL direct: each thread reads from its own private copy
                results[i] = sum_fn(cxl_copies[i], NUM_ELEMENTS);
            } else {
                // DRAM pipelined: process each chunk as it becomes available
                uint64_t partial = 0;
                for (size_t c = 0; c < num_chunks; c++) {
                    // Spin until chunk c has been copied
                    while (chunks_ready.load(std::memory_order_acquire) <= c) {
                        _mm_pause();
                    }
                    const size_t offset = c * chunk_elems;
                    const size_t elems  = std::min(chunk_elems, NUM_ELEMENTS - offset);
                    partial += sum_fn(dram_buf + offset, elems);
                }
                results[i] = partial;
            }
        });
    }

    for (auto& th : threads) th.join();
    if (copy_thread.joinable()) copy_thread.join();

    auto t_end = Clock::now();

    // Sink all results to prevent dead-code elimination
    for (int i = 0; i < NUM_QUERIES; i++) sink += results[i];
    (void)sink;

    BenchResult br;
    br.K        = K;
    br.total_ms = std::chrono::duration<double, std::milli>(t_end - t_start).count();
    br.copy_ms  = copy_ms_out.load(std::memory_order_relaxed);
    return br;
}

// ============================================================================
// main
// ============================================================================

int main(int argc, char* argv[]) {
    // Check for --scalar flag and build a filtered argv for positional parsing
    bool use_scalar = false;
    std::vector<char*> pos_args;
    pos_args.push_back(argv[0]);
    for (int a = 1; a < argc; a++) {
        if (std::string(argv[a]) == "--scalar") {
            use_scalar = true;
        } else {
            pos_args.push_back(argv[a]);
        }
    }
    const int pos_argc = static_cast<int>(pos_args.size());
    const SumFn sum_fn = use_scalar ? scalar_sum : avx512_sum;
    const char* sum_name = use_scalar ? "scalar" : "AVX-512";

    if (pos_argc < 3) {
        std::cerr << "Usage: " << pos_args[0]
                  << " <cxl_node> <dram_node> [reps] [chunk_mib] [csv_file] [--scalar]\n"
                  << "\n"
                  << "  cxl_node    NUMA node simulating CXL (remote)\n"
                  << "  dram_node   NUMA node for local DRAM\n"
                  << "  reps        Repetitions per K (default: 10)\n"
                  << "  chunk_mib   Chunk size in MiB (default: 64)\n"
                  << "  csv_file    Output CSV (default: results.csv)\n"
                  << "  --scalar    Use scalar sum instead of AVX-512\n";
        return 1;
    }

    const int    cxl_node    = std::atoi(pos_args[1]);
    const int    dram_node   = std::atoi(pos_args[2]);
    const int    reps        = (pos_argc > 3) ? std::atoi(pos_args[3]) : DEFAULT_REPS;
    const size_t chunk_bytes = (pos_argc > 4)
        ? (size_t)std::atoi(pos_args[4]) * 1024ULL * 1024ULL
        : DEFAULT_CHUNK;
    const char*  csv_path    = (pos_argc > 5) ? pos_args[5] : "results.csv";

    if (chunk_bytes == 0) {
        std::cerr << "FATAL: chunk size must be > 0.\n"; return 1;
    }
    if (reps <= 0) {
        std::cerr << "FATAL: repetitions must be > 0.\n"; return 1;
    }

    const size_t chunk_elems = chunk_bytes / sizeof(uint64_t);
    const size_t num_chunks  = (NUM_ELEMENTS + chunk_elems - 1) / chunk_elems;

    // ---- Sanity checks ----
    if (numa_available() < 0) {
        std::cerr << "FATAL: NUMA not available.\n"; return 1;
    }
    const int max_node = numa_max_node();
    if (cxl_node > max_node || dram_node > max_node) {
        std::cerr << "FATAL: invalid NUMA node (max: " << max_node << ")\n";
        return 1;
    }
    if (cxl_node == dram_node) {
        std::cerr << "WARNING: CXL and DRAM on same node — no meaningful diff.\n";
    }

    // ---- Print config ----
    std::cout << "=== CXL vs DRAM — Pipelined Chunked Transfer ===\n"
              << "  Data:       " << (DATA_SIZE >> 30) << " GiB  ("
              << NUM_ELEMENTS << " x uint64_t)\n"
              << "  Chunk:      " << (chunk_bytes >> 20) << " MiB  ("
              << num_chunks << " chunks)\n"
              << "  Queries:    " << NUM_QUERIES << " concurrent threads\n"
              << "  K sweep:    0.." << NUM_QUERIES << " (step " << K_STEP << ")\n"
              << "  Reps:       " << reps << "\n"
              << "  CXL node:   " << cxl_node << "\n"
              << "  DRAM node:  " << dram_node << "\n"
              << "  CSV:        " << csv_path << "\n"
              << "  Sum mode:   " << sum_name << "\n\n";

    // ---- Allocate & initialize CXL data (per-thread private copies) ----
    const size_t total_cxl_gib = (size_t)NUM_QUERIES * (DATA_SIZE >> 30);
    std::cout << "[1/5] Allocating " << NUM_QUERIES << " x "
              << (DATA_SIZE >> 30) << " GiB = " << total_cxl_gib
              << " GiB on node " << cxl_node << " (CXL)...\n";

    // Allocate master copy and generate data into it
    uint64_t* cxl_master = numa_alloc_column(cxl_node, DATA_SIZE);
    fault_pages(cxl_master, NUM_ELEMENTS);

    std::cout << "[2/5] Generating data into master CXL copy...\n";
    generate_data(cxl_master, NUM_ELEMENTS);
    verify_numa_placement(cxl_master, cxl_node);

    // Allocate per-thread copies and replicate from master
    std::cout << "[3/5] Replicating into " << NUM_QUERIES
              << " private CXL copies...\n";
    std::vector<uint64_t*> cxl_copies(NUM_QUERIES);
    cxl_copies[0] = cxl_master; // thread 0 uses master directly

    for (int t = 1; t < NUM_QUERIES; t++) {
        cxl_copies[t] = numa_alloc_column(cxl_node, DATA_SIZE);
        fault_pages(cxl_copies[t], NUM_ELEMENTS);
        std::memcpy(cxl_copies[t], cxl_master, DATA_SIZE);
    }

    // ---- Allocate DRAM buffer ----
    std::cout << "[4/5] Allocating DRAM buffer on node " << dram_node << "...\n";
    uint64_t* dram_buf = numa_alloc_column(dram_node, DATA_SIZE);
    fault_pages(dram_buf, NUM_ELEMENTS);
    verify_numa_placement(dram_buf, dram_node);

    // ---- Reference sum ----
    std::cout << "[5/5] Reference sum...\n";
    uint64_t ref = sum_fn(cxl_master, NUM_ELEMENTS);
    std::cout << "  ref_sum = " << ref << "\n\n";

    // ---- DRAM-resident baseline ----
    std::cout << "Running DRAM-resident baseline (" << reps << " reps)...\n";
    std::memcpy(dram_buf, cxl_master, DATA_SIZE); // pre-populate DRAM
    std::vector<double> dram_baseline_runs;
    dram_baseline_runs.reserve(reps);
    for (int r = 0; r < reps; r++) {
        dram_baseline_runs.push_back(run_dram_baseline(dram_buf, sum_fn));
    }
    std::sort(dram_baseline_runs.begin(), dram_baseline_runs.end());
    const double dram_baseline_ms = dram_baseline_runs[reps / 2];
    std::cout << "  DRAM-resident baseline (median): "
              << std::fixed << std::setprecision(2)
              << dram_baseline_ms << " ms\n\n";

    // ---- CSV ----
    std::ofstream csv(csv_path);
    if (!csv) { std::cerr << "Cannot open " << csv_path << "\n"; return 1; }
    csv << "K,dram_readers,cxl_readers,rep,total_ms,copy_ms\n";

    // Write DRAM baseline rows (K = -1 sentinel)
    for (int r = 0; r < reps; r++) {
        csv << "-1," << NUM_QUERIES << ",0,"
            << r << ","
            << std::fixed << std::setprecision(2)
            << dram_baseline_runs[r] << ",0.00\n";
    }

    // ---- Stdout header ----
    std::cout << std::setw(5)  << "K"
              << std::setw(8)  << "DRAM"
              << std::setw(8)  << "CXL"
              << std::setw(14) << "total_ms"
              << std::setw(14) << "copy_ms"
              << "  (median)\n"
              << std::string(60, '-') << "\n";

    // Print DRAM baseline in table
    std::cout << std::setw(5)  << "base"
              << std::setw(8)  << NUM_QUERIES
              << std::setw(8)  << 0
              << std::setw(14) << std::fixed << std::setprecision(2) << dram_baseline_ms
              << std::setw(14) << "0.00"
              << "  * (DRAM-resident)\n";

    // ---- Sweep K ----
    std::vector<int> k_values;
    for (int K = 0; K <= NUM_QUERIES; K += K_STEP) {
        k_values.push_back(K);
    }
    if (k_values.back() != NUM_QUERIES) {
        k_values.push_back(NUM_QUERIES);
    }

    for (int K : k_values) {
        std::vector<BenchResult> runs;
        runs.reserve(reps);

        for (int r = 0; r < reps; r++) {
            // Clear DRAM buffer to avoid stale cache effects
            std::memset(dram_buf, 0, DATA_SIZE);

            auto br = run_single(cxl_copies.data(), cxl_master, dram_buf,
                                 K, chunk_bytes, sum_fn);
            runs.push_back(br);

            csv << K << "," << K << "," << (NUM_QUERIES - K) << ","
                << r << ","
                << std::fixed << std::setprecision(2)
                << br.total_ms << "," << br.copy_ms << "\n";
        }

        // Report median
        std::sort(runs.begin(), runs.end(),
                  [](auto& a, auto& b) { return a.total_ms < b.total_ms; });
        const auto& med = runs[reps / 2];

        std::cout << std::setw(5)  << K
                  << std::setw(8)  << K
                  << std::setw(8)  << (NUM_QUERIES - K)
                  << std::setw(14) << std::fixed << std::setprecision(2) << med.total_ms
                  << std::setw(14) << med.copy_ms
                  << "  *\n";
    }

    csv.close();
    std::cout << "\nResults written to " << csv_path << "\n";

    numa_free(dram_buf, DATA_SIZE);
    for (int t = 0; t < NUM_QUERIES; t++) {
        numa_free(cxl_copies[t], DATA_SIZE);
    }
    return 0;
}