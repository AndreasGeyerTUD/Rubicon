#pragma once

#include <filesystem>
#include <infrastructure/DataCatalog.hpp>
#include <operators/AbstractOperator.hpp>
#include <sstream>

/**
 * @brief The operator converts all elements of a column into a string representation.
 *
 */
class ResultOperator : public AbstractOperator {
   public:
    ResultOperator(WorkItem p_witem) : AbstractOperator(p_witem) {};
    virtual WorkResponse run() override;
    static const std::string result_directory;
    static bool createResultDir() {
        if (!std::filesystem::is_directory(result_directory)) {
            try {
                if (std::filesystem::create_directory(result_directory)) {
                    LOG_INFO("[FetchDataOperator] Created result directory." << std::endl;)
                    return true;
                } else {
                    LOG_INFO("[FetchDataOperator] Could not create result directory, nothing persisted!" << std::endl;)
                    return false;
                }
            } catch (const std::exception& e) {
                LOG_ERROR("" << e.what() << std::endl;)
                return false;
            }
        } else {
            return true;
        }
    }

   private:
    template <typename T>
    void retrieveValue(std::stringstream& ss, std::shared_ptr<Column> col, const size_t idx) {
        if constexpr (std::is_floating_point_v<T>) {
            ss << static_cast<double>(static_cast<T*>(col->data)[idx]);
        } else {
            if constexpr (std::is_signed_v<T>) {
                ss << static_cast<int64_t>(static_cast<T*>(col->data)[idx]);
            } else {
                ss << static_cast<uint64_t>(static_cast<T*>(col->data)[idx]);
            }
        }
    }

    void retrieveStringValue(std::stringstream& ss, std::shared_ptr<Column> col, const size_t idx) {
        const uint64_t encoded = static_cast<uint64_t*>(col->data)[idx];
        ss << col->dictionaryEncoding->getDictionaryValue(encoded);
    }

    void dispatchRetrieveValue(std::stringstream& ss, std::shared_ptr<Column> col, const size_t idx) {
        switch (col->datatype) {
            case DataType::uint8: {
                retrieveValue<uint8_t>(ss, col, idx);
            } break;
            case DataType::int8: {
                retrieveValue<int8_t>(ss, col, idx);
            } break;
            case DataType::uint16: {
                retrieveValue<uint16_t>(ss, col, idx);
            } break;
            case DataType::int16: {
                retrieveValue<int16_t>(ss, col, idx);
            } break;
            case DataType::uint32: {
                retrieveValue<uint32_t>(ss, col, idx);
            } break;
            case DataType::int32: {
                retrieveValue<int32_t>(ss, col, idx);
            } break;
            case DataType::timestamp:
            case DataType::uint64: {
                retrieveValue<uint64_t>(ss, col, idx);
            } break;
            case DataType::int64: {
                retrieveValue<int64_t>(ss, col, idx);
            } break;
            case DataType::f32: {
                retrieveValue<float>(ss, col, idx);
            } break;
            case DataType::f64: {
                retrieveValue<double>(ss, col, idx);
            } break;
            case DataType::string_enc: {
                retrieveStringValue(ss, col, idx);
            } break;
            case DataType::position_list:
            case DataType::bitmask:
            case DataType::unknown: {
                LOG_ERROR("[ResultOperator] Datatype not suitable for result: " << static_cast<int>(col->datatype) << "." << std::endl;)
            }
        }
    }
};