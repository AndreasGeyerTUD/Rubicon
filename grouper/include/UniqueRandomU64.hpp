#pragma once

#include <atomic>
#include <cstdint>
#include <random>
#include <chrono>

class UniqueRandomU64 {
public:
    // Access the singleton instance (Meyers' singleton: thread-safe since C++11)
    static UniqueRandomU64& instance() {
        static UniqueRandomU64 inst;
        return inst;
    }

    // Non-copyable / non-movable
    UniqueRandomU64(const UniqueRandomU64&) = delete;
    UniqueRandomU64& operator=(const UniqueRandomU64&) = delete;
    UniqueRandomU64(UniqueRandomU64&&) = delete;
    UniqueRandomU64& operator=(UniqueRandomU64&&) = delete;

    // Get a unique, "random-looking" uint64_t (no collisions)
    uint64_t next() {
        uint64_t x = counter_.fetch_add(1, std::memory_order_relaxed);
        return permute64(x ^ key_);
    }

private:
    UniqueRandomU64() {
        // Seed counter and key with some entropy.
        std::random_device rd;

        auto rd64 = [&]() -> uint64_t {
            // Combine multiple rd() calls (rd() is often 32-bit)
            uint64_t hi = uint64_t(rd());
            uint64_t lo = uint64_t(rd());
            return (hi << 32) ^ lo;
        };

        uint64_t t = uint64_t(std::chrono::high_resolution_clock::now()
                                  .time_since_epoch().count());

        uint64_t seed_counter = rd64() ^ (t * 0x9E3779B97F4A7C15ULL);
        uint64_t seed_key     = rd64() ^ (t * 0xD2B74407B1CE6E93ULL);

        counter_.store(seed_counter, std::memory_order_relaxed);
        key_ = seed_key | 1ULL; // ensure non-zero-ish / oddness isn't required here, but fine
    }

    static uint64_t permute64(uint64_t z) {
        // SplitMix64 finalizer (bijective over 64-bit words)
        z ^= z >> 30;
        z *= 0xBF58476D1CE4E5B9ULL;
        z ^= z >> 27;
        z *= 0x94D049BB133111EBULL;
        z ^= z >> 31;
        return z;
    }

    std::atomic<uint64_t> counter_{0};
    uint64_t key_{0};
};
