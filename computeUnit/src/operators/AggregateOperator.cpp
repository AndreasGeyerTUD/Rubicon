#include <operators/AggregateOperator.hpp>

WorkResponse AggregateOperator::run() {
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

    auto aggregateData = m_work_item.aggdata();
    LOG_DEBUG1("[AggregateOperator] Column: " << aggregateData.inputcolumn().tabname() << "." << aggregateData.inputcolumn().colname() << std::endl;)
    auto column = DataCatalog::getInstance().getColumnByName(aggregateData.inputcolumn().tabname(), aggregateData.inputcolumn().colname());

    if (!column) {
        response.set_info("[AggregateOperator] ERROR - Could not find the column. Aborting execution.");
        response.set_success(false);
        LOG_ERROR("[AggregateOperator] Could not find the column (" << aggregateData.inputcolumn().tabname() << "." << aggregateData.inputcolumn().colname() << "). Aborting execution." << std::endl;)
        return response;
    }

    uint64_t rowCount = 0;

    switch (column->datatype) {
        case DataType::uint8: {
            rowCount = selectAggregate<uint8_t>(column);
        } break;
        case DataType::int8: {
            rowCount = selectAggregate<int8_t>(column);
        } break;
        case DataType::uint16: {
            rowCount = selectAggregate<uint16_t>(column);
        } break;
        case DataType::int16: {
            rowCount = selectAggregate<int16_t>(column);
        } break;
        case DataType::uint32: {
            rowCount = selectAggregate<uint32_t>(column);
        } break;
        case DataType::int32: {
            rowCount = selectAggregate<int32_t>(column);
        } break;
        case DataType::uint64: {
            rowCount = selectAggregate<uint64_t>(column);
        } break;
        case DataType::int64: {
            rowCount = selectAggregate<int64_t>(column);
        } break;
        case DataType::f32: {
            rowCount = selectAggregate<float>(column);
        } break;
        case DataType::f64: {
            rowCount = selectAggregate<double>(column);
        } break;
        case DataType::timestamp:
        case DataType::string_enc:
        case DataType::position_list:
        case DataType::bitmask:
        case DataType::unknown: {
            response.set_info("[AggregateOperator] ERROR - unsupported datatype.");
            response.set_success(false);
            LOG_ERROR("[AggregateOperator] Unsupported data type: " << static_cast<int>(column->datatype) << "." << std::endl;)
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