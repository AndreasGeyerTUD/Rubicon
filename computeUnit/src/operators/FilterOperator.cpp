#include <operators/FilterOperator.hpp>

WorkResponse FilterOperator::run() {
    WorkResponse response;
    response.set_planid(m_work_item.planid());
    response.set_itemid(m_work_item.itemid());
    response.set_success(true);

    if (m_work_item.returnextendedresult()) {
        response.mutable_extendedresult()->set_itemid(m_work_item.itemid());
        using namespace std::chrono;
        const uint64_t startTime = duration_cast<nanoseconds>(high_resolution_clock::now().time_since_epoch()).count();
        response.mutable_extendedresult()->set_start(startTime);
    }

    auto filterData = m_work_item.filterdata();
    LOG_DEBUG1("[FilterOperator] Column: " << filterData.inputcolumn().tabname() << "." << filterData.inputcolumn().colname() << std::endl;)
    auto column = DataCatalog::getInstance().getColumnByName(filterData.inputcolumn().tabname(), filterData.inputcolumn().colname());

    if (!column) {
        response.set_info("[FilterOperator] ERROR - Could not find the column. Aborting execution.");
        response.set_success(false);
        LOG_ERROR("[FilterOperator] Could not find the column (" << filterData.inputcolumn().tabname() << "." << filterData.inputcolumn().colname() << "). Aborting execution." << std::endl;)
        return response;
    }

    uint64_t rowCount = 0;

    switch (column->datatype) {
        case DataType::uint8: {
            rowCount = selectFilter<uint8_t>(column);
        } break;
        case DataType::int8: {
            rowCount = selectFilter<int8_t>(column);
        } break;
        case DataType::uint16: {
            rowCount = selectFilter<uint16_t>(column);
        } break;
        case DataType::int16: {
            rowCount = selectFilter<int16_t>(column);
        } break;
        case DataType::uint32: {
            rowCount = selectFilter<uint32_t>(column);
        } break;
        case DataType::int32: {
            rowCount = selectFilter<int32_t>(column);
        } break;
        case DataType::timestamp:  // Fallthrough intended; Let's see if this works
        case DataType::uint64: {
            rowCount = selectFilter<uint64_t>(column);
        } break;
        case DataType::int64: {
            rowCount = selectFilter<int64_t>(column);
        } break;
        case DataType::f32: {
            rowCount = selectFilter<float>(column);
        } break;
        case DataType::f64: {
            rowCount = selectFilter<double>(column);
        } break;
        case DataType::string_enc: {
            rowCount = selectEncFilter(column);
        } break;
        case DataType::position_list:
        case DataType::bitmask: {
            // Lets see if this is a valid approach
            rowCount = selectFilter<size_t>(column);
        } break;
        case DataType::unknown: {
            response.set_info("[FilterOperator] ERROR - unsupporter datatype.");
            response.set_success(false);
            LOG_ERROR("[FilterOperator] Unsupported data type: " << static_cast<int>(column->datatype) << "." << std::endl;)
        }
    }

    if (m_work_item.returnextendedresult()) {
        response.mutable_extendedresult()->set_rowcount(rowCount);
        using namespace std::chrono;
        const uint64_t endTime = duration_cast<nanoseconds>(high_resolution_clock::now().time_since_epoch()).count();
        response.mutable_extendedresult()->set_end(endTime);
    }

    return response;
}

uint64_t FilterOperator::doBetweenFilterOnEnc(std::shared_ptr<Column> column, const uint64_t chunkElements) {
    auto filterData = m_work_item.filterdata();

    column->dictionaryInitialized.wait(false);  // wait for the pointer to be set
    column->dictionaryEncoding->waitReady();    // wait for actual dictionary content ready
    const std::unordered_set<size_t> keysInRange = column->dictionaryEncoding->getKeysInRange(filterData.filtervalue().Get(0).stringval().value(), filterData.filtervalue().Get(1).stringval().value());

    return doActualInFilter<uint64_t>(column, chunkElements, keysInRange);
}

uint64_t FilterOperator::doInFilterOnEnc(std::shared_ptr<Column> column, const uint64_t chunkElements) {
    auto filterData = m_work_item.filterdata();

    column->dictionaryInitialized.wait(false);  // wait for the pointer to be set
    column->dictionaryEncoding->waitReady();    // wait for actual dictionary content ready

    std::unordered_set<size_t> keysInSet;
    for (const auto& val : filterData.filtervalue()) {
        keysInSet.insert(column->dictionaryEncoding->getDictionaryValue(val.stringval().value()));
    }

    return doActualInFilter<uint64_t>(column, chunkElements, keysInSet);
}

uint64_t FilterOperator::doLikeFilterOnEnc(std::shared_ptr<Column> column, const uint64_t chunkElements) {
    auto filterData = m_work_item.filterdata();

    column->dictionaryInitialized.wait(false);  // wait for the pointer to be set
    column->dictionaryEncoding->waitReady();    // wait for actual dictionary content ready

    std::unordered_set<size_t> matchingDictValues = column->dictionaryEncoding->findMatchingValues(filterData.filtervalue().Get(0).stringval().value());

    return doActualInFilter<uint64_t>(column, chunkElements, matchingDictValues);
}