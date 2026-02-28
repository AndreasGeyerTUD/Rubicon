#include <chrono>
#include <operators/ResultOperator.hpp>
#include <sstream>

const std::string ResultOperator::result_directory{"./results"};

WorkResponse ResultOperator::run() {
    WorkResponse response;
    response.set_planid(m_work_item.planid());
    response.set_itemid(m_work_item.itemid());
    response.set_success(true);

    const auto res_timing_start = std::chrono::high_resolution_clock::now();

    if (m_work_item.returnextendedresult()) {
        response.mutable_extendedresult()->set_itemid(m_work_item.itemid());
        using namespace std::chrono;
        const uint64_t startTime = duration_cast<nanoseconds>(high_resolution_clock::now().time_since_epoch()).count();
        response.mutable_extendedresult()->set_start(startTime);
    }

    auto resultData = m_work_item.resultdata();
    std::vector<std::shared_ptr<Column>> cols;
    for (auto col : resultData.resultcolumns()) {
        LOG_DEBUG1("[ResultOperator] Result Column: " << col.tabname() << "." << col.colname() << std::endl;)
        cols.emplace_back(DataCatalog::getInstance().getColumnByName(col.tabname(), col.colname()));
        if (!cols.back()) {
            LOG_ERROR("[ResultOperator] Column " << col.tabname() << "." << col.colname() << " is not present, aborting.";)
            response.set_success(false);
            response.set_info("[ResultOperator] Could not fetch column " + col.tabname() + "." + col.colname());
            return response;
        }
    }

    std::stringstream resstream;

    const size_t elements = cols[0]->elements;
    // Check all columns are of equal length
    for (size_t i = 1; i < cols.size(); ++i) {
        if (cols[i]->elements != elements) {
            auto col0 = resultData.resultcolumns().at(0);
            auto col1 = resultData.resultcolumns().at(1);
            response.set_info("[ResultOperator] Element mismatch between " + col0.tabname() + "." + col0.colname() + "( " + std::to_string(elements) + ") and " + col1.tabname() + "." + col1.colname() + " (" + std::to_string(cols[i]->elements) + ")");
            response.set_success(false);
            return response;
        }
    }
    auto appendSep = [](std::stringstream& ss, const size_t idx, const size_t end) {
        if (idx < end - 1) {
            ss << "\t";
        } else {
            ss << std::endl;
        }
    };
    if (resultData.resultheader_size() > 0) {
        for (int colidx = 0; colidx != resultData.resultheader_size(); ++colidx) {
            resstream << resultData.resultheader().at(colidx);
            appendSep(resstream, colidx, resultData.resultheader_size());
        }
    } else {
        for (int colidx = 0; colidx != resultData.resultcolumns_size(); ++colidx) {
            resstream << resultData.resultcolumns().at(colidx).colname();
            appendSep(resstream, colidx, resultData.resultcolumns_size());
        }
    }
    if (resultData.has_resultindex()) {
        auto res_idx_col = DataCatalog::getInstance().getColumnByName(resultData.resultindex().tabname(), resultData.resultindex().colname());
        for (size_t idx = 0; idx < elements; ++idx) {
            const size_t res_pos = static_cast<size_t*>(res_idx_col->data)[idx];
            for (size_t colidx = 0; colidx != cols.size(); ++colidx) {
                dispatchRetrieveValue(resstream, cols[colidx], res_pos);
                appendSep(resstream, colidx, cols.size());
            }
        }
    } else {
        for (size_t idx = 0; idx < elements; ++idx) {
            for (size_t colidx = 0; colidx != cols.size(); ++colidx) {
                dispatchRetrieveValue(resstream, cols[colidx], idx);
                appendSep(resstream, colidx, cols.size());
            }
        }
    }

    if (createResultDir()) {
        LOG_DEBUG1("[ResultOperator] Persisted result file " << resultData.filename() << std::endl;)
        std::ofstream file(ResultOperator::result_directory + "/" + resultData.filename() + ".tsv");
        file << resstream.str();
        file.close();
    }

    const auto res_timing_end = std::chrono::high_resolution_clock::now();
    const size_t dur = std::chrono::duration_cast<std::chrono::microseconds>(res_timing_end - res_timing_start).count();
    response.set_info("[ResultOperator] Time taken to print: " + std::to_string(dur) + " us");

    if (m_work_item.returnextendedresult()) {
        response.mutable_extendedresult()->set_rowcount(elements);
        using namespace std::chrono;
        const uint64_t endTime = duration_cast<nanoseconds>(high_resolution_clock::now().time_since_epoch()).count();
        response.mutable_extendedresult()->set_end(endTime);
    }

    return response;
}