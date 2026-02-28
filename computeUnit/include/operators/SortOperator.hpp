#pragma once

#include <algorithms/dbops/sort/sort.hpp>
#include <algorithms/dbops/sort/sort_by_clusters.hpp>  // SIMDOps
#include <algorithms/utils/hinting.hpp>                // SIMDOps
#include <datastructures/column.hpp>                   // SIMDOps
#include <infrastructure/DataCatalog.hpp>
#include <operators/AbstractOperator.hpp>

/**
 * @brief Iterate over a column and check the values against one or two filter predicates.
 *
 */
class SortOperator : public AbstractOperator {
   public:
    SortOperator(WorkItem p_witem) : AbstractOperator(p_witem) {};
    virtual WorkResponse run() override;

   private:
    template <tuddbs::TSL_SORT_ORDER SortOrderT, typename base_t>
    std::deque<tuddbs::Cluster> do_group_first(std::shared_ptr<Column> column, tuddbs::InMemoryColumn<size_t>& res_col) {
        using SimdStyle = tsl::simd<base_t, typename tsl::runtime::cpu::max_width_extension_t>;
        using IndexStyle = tsl::simd<size_t, typename tsl::runtime::cpu::max_width_extension_t>;
        using HS_INLEAF =
            tuddbs::OperatorHintSet<tuddbs::hints::sort::indirect_gather, tuddbs::hints::sort::leaf_clustering>;
        using SorterT = tuddbs::ClusteringSingleColumnSort<SimdStyle, SortOrderT, HS_INLEAF, IndexStyle>::sorter_t;

        // to ensure data availability, we use the View interface
        View col = View<base_t>(static_cast<base_t*>(column->data), column->elements, column);

        // Sort first column
        SorterT clusterer(col.begin(), res_col.begin());
        clusterer(0, col.getNumberOfElements());
        return std::move(clusterer.getClusters());
    }

    template <tuddbs::TSL_SORT_ORDER SortOrderT>
    std::deque<tuddbs::Cluster> do_group_first_string_enc(std::shared_ptr<Column> column, tuddbs::InMemoryColumn<size_t>& res_col) {
        // to ensure data availability, we use the View interface
        View col = View<uint64_t>(static_cast<uint64_t*>(column->data), column->elements, column);

        std::deque<tuddbs::Cluster> clusters;

        auto sorter = [&column](const size_t lhs, const size_t rhs) -> bool {
            const auto& vlhs = column->dictionaryEncoding->getDictionaryValue(static_cast<uint64_t*>(column->data)[lhs]);
            const auto& vrhs = column->dictionaryEncoding->getDictionaryValue(static_cast<uint64_t*>(column->data)[rhs]);
            return vlhs < vrhs;
        };

        auto encoded_string_data = static_cast<uint64_t*>(col.begin());
        auto sortable_index_data = static_cast<size_t*>(res_col.begin());

        std::sort(sortable_index_data, sortable_index_data + res_col.count(), sorter);

        uint64_t run_value = encoded_string_data[sortable_index_data[0]];
        size_t run_length = 1;
        for (size_t i = 1; i < col.getNumberOfElements(); ++i) {
            const uint64_t current_value = encoded_string_data[sortable_index_data[i]];
            if (current_value != run_value) {
                if (run_length > 1) {
                    clusters.emplace_back(i - run_length, run_length);
                }
                run_length = 1;
                run_value = current_value;
            } else {
                ++run_length;
            }
        }
        if (run_length > 1) {
            clusters.emplace_back(col.getNumberOfElements() - run_length, run_length);
        }

        return clusters;
    }

    template <tuddbs::TSL_SORT_ORDER SortOrderT, typename base_t>
    void do_group_consecutive(std::shared_ptr<Column> column, tuddbs::InMemoryColumn<size_t>& res_col, std::deque<tuddbs::Cluster>& clusters) {
        using SimdStyle = tsl::simd<base_t, typename tsl::runtime::cpu::max_width_extension_t>;
        using IndexStyle = tsl::simd<uint64_t, typename tsl::runtime::cpu::max_width_extension_t>;
        using HS_GATH = tuddbs::OperatorHintSet<tuddbs::hints::sort::indirect_gather>;
        using HS_INLEAF =
            tuddbs::OperatorHintSet<tuddbs::hints::sort::indirect_gather, tuddbs::hints::sort::leaf_clustering>;
        tuddbs::ClusterSortIndirect<SimdStyle, IndexStyle, HS_GATH> mcol_sorter(res_col.begin(), &clusters);

        // to ensure data availability, we use the View interface
        View col = View<base_t>(static_cast<base_t*>(column->data), column->elements, column);

        mcol_sorter(col.begin(), SortOrderT);
    }

    template <tuddbs::TSL_SORT_ORDER SortOrderT>
    void do_group_consecutive_string_enc(std::shared_ptr<Column> column, tuddbs::InMemoryColumn<size_t>& res_col, std::deque<tuddbs::Cluster>& clusters) {
        auto sorter = [&column](const size_t lhs, const size_t rhs) -> bool {
            const auto& vlhs = column->dictionaryEncoding->getDictionaryValue(static_cast<uint64_t*>(column->data)[lhs]);
            const auto& vrhs = column->dictionaryEncoding->getDictionaryValue(static_cast<uint64_t*>(column->data)[rhs]);
            return vlhs < vrhs;
        };

        const size_t cluster_count = clusters.size();
        for (size_t i = 0; i < cluster_count; ++i) {
            tuddbs::Cluster& c = clusters.front();
            clusters.pop_front();
            if (c.len == 1) {
                continue;
            }
            const size_t start_pos = c.start;
            const size_t end_pos = start_pos + c.len;

            // to ensure data availability, we use the View interface
            View col = View<uint64_t>(static_cast<uint64_t*>(column->data), column->elements, column);

            auto p_data = static_cast<uint64_t*>(col.begin());
            auto m_idx = static_cast<size_t*>(res_col.begin());

            std::sort(m_idx + start_pos, m_idx + end_pos, sorter);

            uint64_t curr_value = p_data[m_idx[start_pos]];
            std::deque<tuddbs::Cluster> new_clusters;
            size_t curr_start = start_pos;
            for (size_t j = start_pos + 1; j != end_pos; ++j) {
                uint64_t run_value = p_data[m_idx[j]];
                if (run_value != curr_value) {
                    const size_t curr_run_len = (j - curr_start);
                    if (curr_run_len > 1) {
                        new_clusters.push_back(tuddbs::Cluster(curr_start, curr_run_len));
                    }
                    curr_start = j;
                    curr_value = run_value;
                }
            }
            const size_t curr_run_len = end_pos - curr_start;
            if (curr_run_len > 1) {
                new_clusters.emplace_back(tuddbs::Cluster(curr_start, end_pos - curr_start));
            }

            while (!new_clusters.empty()) {
                clusters.push_back(new_clusters.front());
                new_clusters.pop_front();
            }
        }
    }

    template <tuddbs::TSL_SORT_ORDER SortOrderT>
    auto select_datatype_first(std::shared_ptr<Column> column, tuddbs::InMemoryColumn<size_t>& res_column, WorkResponse& response) {
        switch (column->datatype) {
            case DataType::uint8: {
                return do_group_first<SortOrderT, uint8_t>(column, res_column);
            } break;
            case DataType::int8: {
                return do_group_first<SortOrderT, int8_t>(column, res_column);
            } break;
            case DataType::uint16: {
                return do_group_first<SortOrderT, uint16_t>(column, res_column);
            } break;
            case DataType::int16: {
                return do_group_first<SortOrderT, int16_t>(column, res_column);
            } break;
            case DataType::uint32: {
                return do_group_first<SortOrderT, uint32_t>(column, res_column);
            } break;
            case DataType::int32: {
                return do_group_first<SortOrderT, int32_t>(column, res_column);
            } break;
            case DataType::timestamp:  // Fallthrough intended; Let's see if this works
            case DataType::string_enc: // Fallthrough intended; Works because the dictionary is sorted
            case DataType::uint64: {
                return do_group_first<SortOrderT, uint64_t>(column, res_column);
            } break;
            case DataType::int64: {
                return do_group_first<SortOrderT, int64_t>(column, res_column);
            } break;
            case DataType::f32: {
                return do_group_first<SortOrderT, float>(column, res_column);
            } break;
            case DataType::f64: {
                return do_group_first<SortOrderT, double>(column, res_column);
            } break;
            case DataType::bitmask: {
                // Lets see if this is a valid approach
                return do_group_first<SortOrderT, size_t>(column, res_column);
            } break;
            // case DataType::string_enc: {
            //     return do_group_first_string_enc<SortOrderT>(column, res_column);
            // } break;
            case DataType::position_list:
            case DataType::unknown: {
                response.set_info("[GroupOperator] ERROR - unsupported datatype.");
                response.set_success(false);
                LOG_ERROR("[GroupOperator] Unsupported data type: " << static_cast<int>(column->datatype) << "." << std::endl;)
            }
        }
        return std::deque<tuddbs::Cluster>();
    }

    template <tuddbs::TSL_SORT_ORDER SortOrderT>
    void select_datatype_consecutive(std::shared_ptr<Column> column, tuddbs::InMemoryColumn<size_t>& res_column, std::deque<tuddbs::Cluster>& clusters, WorkResponse& response) {
        switch (column->datatype) {
            case DataType::uint8: {
                do_group_consecutive<SortOrderT, uint8_t>(column, res_column, clusters);
            } break;
            case DataType::int8: {
                do_group_consecutive<SortOrderT, int8_t>(column, res_column, clusters);
            } break;
            case DataType::uint16: {
                do_group_consecutive<SortOrderT, uint16_t>(column, res_column, clusters);
            } break;
            case DataType::int16: {
                do_group_consecutive<SortOrderT, int16_t>(column, res_column, clusters);
            } break;
            case DataType::uint32: {
                do_group_consecutive<SortOrderT, uint32_t>(column, res_column, clusters);
            } break;
            case DataType::int32: {
                do_group_consecutive<SortOrderT, int32_t>(column, res_column, clusters);
            } break;
            case DataType::timestamp:  // Fallthrough intended; Let's see if this works
            case DataType::string_enc: // Fallthrough intended; Works because the dictionary is sorted
            case DataType::uint64: {
                do_group_consecutive<SortOrderT, uint64_t>(column, res_column, clusters);
            } break;
            case DataType::int64: {
                do_group_consecutive<SortOrderT, int64_t>(column, res_column, clusters);
            } break;
            case DataType::f32: {
                do_group_consecutive<SortOrderT, float>(column, res_column, clusters);
            } break;
            case DataType::f64: {
                do_group_consecutive<SortOrderT, double>(column, res_column, clusters);
            } break;
            case DataType::bitmask: {
                // Lets see if this is a valid approach
                do_group_consecutive<SortOrderT, size_t>(column, res_column, clusters);
            } break;
            // case DataType::string_enc: {
            //     do_group_consecutive_string_enc<SortOrderT>(column, res_column, clusters);
            // } break;
            case DataType::position_list:
            case DataType::unknown: {
                response.set_info("[GroupOperator] ERROR - unsupported datatype.");
                response.set_success(false);
                LOG_ERROR("[GroupOperator] Unsupported data type: " << static_cast<int>(column->datatype) << "." << std::endl;)
            }
        }
    }
};