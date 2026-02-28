#ifndef ANALYSIS_HPP
#define ANALYSIS_HPP

#include <algorithm>
#include <cstdint>
#include <iostream>
#include <numeric>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <set>
#include <vector>

#include "PlanDAG.hpp"
#include "Logger.hpp"

namespace analysis {

using ColumnSet = std::unordered_set<std::string>;

// Helper: extract column names as a set
inline ColumnSet getColumnSet(const plan::BasColMap* map) {
    ColumnSet result;
    for (const auto& [key, _] : *map) {
        result.insert(key);
    }
    return result;
}

// Helper: compute union of column sets
inline ColumnSet unionColumns(const ColumnSet& a, const ColumnSet& b) {
    ColumnSet result = a;
    for (const auto& k : b) {
        result.insert(k);
    }
    return result;
}

// Helper: check if a is subset of b
inline bool isSubset(const ColumnSet& a, const ColumnSet& b) {
    for (const auto& k : a) {
        if (!b.contains(k)) return false;
    }
    return true;
}

struct GroupInfo {
    std::vector<size_t> indices;
    ColumnSet columns;
    size_t maxMemberSize;  // largest individual DAG's column count in this group
};

inline float computeOverheadRatio(const GroupInfo& group) {
    if (group.maxMemberSize == 0) return 1.0f;
    return static_cast<float>(group.columns.size()) / static_cast<float>(group.maxMemberSize);
}

inline float computeMergeOverheadRatio(
    const GroupInfo& a,
    const GroupInfo& b) {
    auto merged = unionColumns(a.columns, b.columns);
    size_t maxMember = std::max(a.maxMemberSize, b.maxMemberSize);
    if (maxMember == 0) return 1.0f;
    return static_cast<float>(merged.size()) / static_cast<float>(maxMember);
}

struct SupersetAbsorptionConfig {
    float maxMergeOverhead;  // For leftover merging: e.g., 1.5 means union can be 50% larger than largest
};

inline std::vector<std::vector<size_t>> groupBySupersetAbsorption(
    const std::vector<const plan::BasColMap*>& maps,
    const SupersetAbsorptionConfig& config) {
    const size_t n = maps.size();
    if (n == 0) return {};

    // Precompute column sets and sizes
    std::vector<std::unordered_set<std::string>> columnSets(n);
    std::vector<size_t> sizes(n);
    for (size_t i = 0; i < n; ++i) {
        columnSets[i] = getColumnSet(maps[i]);
        sizes[i] = columnSets[i].size();
    }

    // Sort indices by column count (descending)
    std::vector<size_t> sortedIndices(n);
    std::iota(sortedIndices.begin(), sortedIndices.end(), 0);
    std::sort(sortedIndices.begin(), sortedIndices.end(), [&](size_t a, size_t b) {
        return sizes[a] > sizes[b];
    });

    std::vector<bool> assigned(n, false);
    std::vector<GroupInfo> groups;

    // Phase 1: Large DAGs absorb smaller subsets
    for (size_t i : sortedIndices) {
        if (assigned[i]) continue;

        GroupInfo group;
        group.indices.push_back(i);
        group.columns = columnSets[i];
        group.maxMemberSize = sizes[i];
        assigned[i] = true;

        // Try to absorb unassigned DAGs that are subsets
        for (size_t j : sortedIndices) {
            if (assigned[j]) continue;
            if (isSubset(columnSets[j], group.columns)) {
                group.indices.push_back(j);
                assigned[j] = true;
                // columns and maxMemberSize don't change since j is a subset
            }
        }

        groups.push_back(std::move(group));
    }

    // Phase 2: Merge remaining groups if overhead is acceptable
    bool merged = true;
    while (merged) {
        merged = false;
        float bestRatio = config.maxMergeOverhead + 1.0f;
        size_t bestI = 0, bestJ = 0;

        for (size_t i = 0; i < groups.size(); ++i) {
            for (size_t j = i + 1; j < groups.size(); ++j) {
                float ratio = computeMergeOverheadRatio(groups[i], groups[j]);
                if (ratio <= config.maxMergeOverhead && ratio < bestRatio) {
                    bestRatio = ratio;
                    bestI = i;
                    bestJ = j;
                }
            }
        }

        if (bestRatio <= config.maxMergeOverhead) {
            // Merge bestJ into bestI
            for (size_t idx : groups[bestJ].indices) {
                groups[bestI].indices.push_back(idx);
            }
            groups[bestI].columns = unionColumns(groups[bestI].columns, groups[bestJ].columns);
            groups[bestI].maxMemberSize = std::max(groups[bestI].maxMemberSize, groups[bestJ].maxMemberSize);

            groups.erase(groups.begin() + bestJ);
            merged = true;
        }
    }

    // Extract result
    std::vector<std::vector<size_t>> result;
    result.reserve(groups.size());
    for (const auto& g : groups) {
        result.push_back(g.indices);
    }
    return result;
}

// ============================================================================
// Print helper
// ============================================================================

inline void printGroups(
    const std::vector<std::vector<size_t>>& groups,
    const std::vector<const plan::BasColMap*>& maps,
    const std::vector<std::string>& names = {}) {
    auto getName = [&](size_t i) -> std::string {
        return names.empty() ? "DAG " + std::to_string(i) : names[i];
    };

    VLOG("Found " << groups.size() << " groups:\n");

    for (size_t g = 0; g < groups.size(); ++g) {
        VLOG("Group " << g << ":");

        std::unordered_set<std::string> unionCols;
        size_t maxSize = 0;

        for (size_t idx : groups[g]) {
            std::stringstream strstr;
            auto cols = getColumnSet(maps[idx]);
            strstr << "  " << getName(idx) << " (" << cols.size() << " cols): {";
            bool first = true;
            for (const auto& c : cols) {
                if (!first) strstr << ", ";
                strstr << c;
                first = false;
                unionCols.insert(c);
            }
            strstr << "}";
            maxSize = std::max(maxSize, cols.size());
            VLOG(strstr.str());
        }

        float overhead = maxSize == 0 ? 1.0f : static_cast<float>(unionCols.size()) / static_cast<float>(maxSize);
        VLOG("  -> Union: " << unionCols.size() << " cols, " << "Overhead ratio: " << std::fixed << std::setprecision(2) << overhead);
    }
}

}  // end namespace analysis

#endif // ANALYSIS_HPP