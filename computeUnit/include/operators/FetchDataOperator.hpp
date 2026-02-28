#pragma once

#include <filesystem>
#include <infrastructure/DataCatalog.hpp>
#include <operators/AbstractOperator.hpp>
#include <operators/ResultOperator.hpp>
#include <sstream>

/**
 * @brief The operator converts all elements of a column into a string representation.
 *
 */
class FetchDataOperator : public AbstractOperator {
   public:
    FetchDataOperator(WorkItem p_witem) : AbstractOperator(p_witem) {};
    virtual WorkResponse run() override;

    template <typename T, bool isBitmask = false>
    std::string col_to_string(std::shared_ptr<Column> col, const uint64_t chunkElements = 1024 * 1024 * 4) {
        std::stringstream ss;

        col->wait_data_allocated();
        View chunk = View<T>(static_cast<T*>(col->data), chunkElements, col);
        const size_t max_print_elements = 5;

        auto append = [](std::string part, std::stringstream& _ss, size_t print_count, size_t offset, T* data) -> void {
            _ss << part << ": ";
            for (size_t i = offset; i < offset + print_count; ++i) {
                if (i == offset) {
                    _ss << data[i];
                    continue;
                }
                _ss << " " << data[i];
            }
        };

        // Get first and last 5 elements
        T* data = chunk.begin();
        if (chunk.isLastChunk()) {
            if (chunk.getNumberOfElements() >= (2 * max_print_elements)) {
                append("First", ss, max_print_elements, 0, data);
                append(" -- Last", ss, max_print_elements, chunk.getNumberOfElements() - max_print_elements, data);
            } else {
                append("First and Last", ss, chunk.getNumberOfElements(), 0, data);
            }
        } else {
            if (chunk.getNumberOfElements() >= max_print_elements) {
                append("First", ss, max_print_elements, 0, data);
            } else {
                const size_t print_count = max_print_elements - chunk.getNumberOfElements();
                append("First", ss, print_count, 0, data);
            }
            // Advance until last chunk
            while (true) {
                if (chunk.isLastChunk()) {
                    break;
                }
                chunk++;
            }
            data = chunk.begin();
            if (chunk.getNumberOfElements() >= max_print_elements) {
                append(" -- Last", ss, max_print_elements, chunk.getNumberOfElements() - max_print_elements, data);
            } else {
                const size_t print_count = max_print_elements - chunk.getNumberOfElements();
                append(" -- Last", ss, print_count, 0, data);
            }
        }

        if (m_work_item.fetchdata().printtofile() && ResultOperator::createResultDir()) {
            const std::string tabident = m_work_item.fetchdata().inputcolumn().tabname();
            const std::string colident = m_work_item.fetchdata().inputcolumn().colname();
            LOG_DEBUG1("[FetchDataOperator] Persisting column " << tabident << "." << colident << " to a file." << std::endl;)
            std::ofstream file(ResultOperator::result_directory + "/" + tabident + "-" + colident + ".txt");
            const T* const printdata = static_cast<T*>(col->data);
            for (size_t i = 0; i < col->elements; ++i) {
                file << printdata[i] << std::endl;
            }
            file.close();
        }

        return ss.str();
    }

    std::string stringEncColToString(std::shared_ptr<Column> col, const uint64_t chunkElements = 1024 * 1024 * 4) {
        std::stringstream ss;

        col->wait_data_allocated();
        View chunk = View<uint64_t>(static_cast<uint64_t*>(col->data), chunkElements, col);
        const size_t max_print_elements = 5;

        auto append = [](std::string part, std::stringstream& _ss, size_t print_count, size_t offset, uint64_t* data, std::shared_ptr<Column> _col) -> void {
            _ss << part << ": ";
            for (size_t i = offset; i < offset + print_count; ++i) {
                if (i == offset) {
                    _ss << _col->dictionaryEncoding->getDictionaryValue(data[i]);
                    continue;
                }
                _ss << " " << _col->dictionaryEncoding->getDictionaryValue(data[i]);
            }
        };

        // Get first and last 5 elements
        uint64_t* data = chunk.begin();
        if (chunk.isLastChunk()) {
            if (chunk.getNumberOfElements() >= (2 * max_print_elements)) {
                append("First", ss, max_print_elements, 0, data, col);
                append(" -- Last", ss, max_print_elements, chunk.getNumberOfElements() - max_print_elements, data, col);
            } else {
                const size_t print_count = (2 * max_print_elements) - chunk.getNumberOfElements();
                append("First and Last", ss, print_count, 0, data, col);
            }
        } else {
            if (chunk.getNumberOfElements() >= max_print_elements) {
                append("First", ss, max_print_elements, 0, data, col);
            } else {
                const size_t print_count = max_print_elements - chunk.getNumberOfElements();
                append("First", ss, print_count, 0, data, col);
            }
            // Advance until last chunk
            while (true) {
                if (chunk.isLastChunk()) {
                    break;
                }
                chunk++;
            }
            data = chunk.begin();
            if (chunk.getNumberOfElements() >= max_print_elements) {
                append(" -- Last", ss, max_print_elements, chunk.getNumberOfElements() - max_print_elements, data, col);
            } else {
                const size_t print_count = max_print_elements - chunk.getNumberOfElements();
                append(" -- Last", ss, print_count, 0, data, col);
            }
        }

        if (m_work_item.fetchdata().printtofile() && ResultOperator::createResultDir()) {
            const std::string tabident = m_work_item.fetchdata().inputcolumn().tabname();
            const std::string colident = m_work_item.fetchdata().inputcolumn().colname();
            LOG_DEBUG1("[FetchDataOperator] Persisting column " << tabident << "." << colident << " to a file." << std::endl;)
            std::ofstream file(ResultOperator::result_directory + "/" + tabident + "-" + colident + ".txt");
            const uint64_t* const printdata = static_cast<uint64_t*>(col->data);
            for (size_t i = 0; i < col->elements; ++i) {
                file << col->dictionaryEncoding->getDictionaryValue(printdata[i]) << std::endl;
            }
            file.close();
        }

        return ss.str();
    }
};