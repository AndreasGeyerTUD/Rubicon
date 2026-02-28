#include <operators/HashJoinOperator.hpp>

WorkResponse HashJoinOperator::run() {
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

    auto joinData = m_work_item.joindata();
    LOG_DEBUG1("[HashJoinOperator] Columns: " << joinData.innercolumn().tabname() << "." << joinData.innercolumn().colname() << ", " << joinData.outercolumn().tabname() << "." << joinData.outercolumn().colname() << std::endl;)
    auto innerColumn = DataCatalog::getInstance().getColumnByName(joinData.innercolumn().tabname(), joinData.innercolumn().colname());
    auto outerColumn = DataCatalog::getInstance().getColumnByName(joinData.outercolumn().tabname(), joinData.outercolumn().colname());

    if (!innerColumn) {
        response.set_info("[HashJoinOperator] ERROR - Could not find the column (" + joinData.innercolumn().tabname() + "." + joinData.innercolumn().colname() + "). Aborting execution.");
        response.set_success(false);
        LOG_ERROR("[HashJoinOperator] Could not find the column (" << joinData.innercolumn().tabname() << "." << joinData.innercolumn().colname() << "). Aborting execution." << std::endl;)
        return response;
    }

    if (!outerColumn) {
        response.set_info("[HashJoinOperator] ERROR - Could not find the column (" + joinData.outercolumn().tabname() + "." + joinData.outercolumn().colname() + "). Aborting execution.");
        response.set_success(false);
        LOG_ERROR("[HashJoinOperator] Could not find the column (" << joinData.outercolumn().tabname() << "." << joinData.outercolumn().colname() << "). Aborting execution." << std::endl;)
        return response;
    }

    if (innerColumn->datatype != outerColumn->datatype) {
        response.set_info("[HashJoinOperator] ERROR - Data types do not match. Aborting execution.");
        response.set_success(false);
        LOG_ERROR("[HashJoinOperator] Data types do not match (" << static_cast<int>(innerColumn->datatype) << " != " << static_cast<int>(outerColumn->datatype) << "). Aborting execution." << std::endl;)
        return response;
    }

    uint64_t rowCount = 0;

    switch (innerColumn->datatype) { // as tested before, innerColumn->datatype == outerColumn->datatype
        case DataType::uint8: {
            rowCount = doHashJoin<uint8_t>(innerColumn, outerColumn);
        } break;
        case DataType::int8: {
            rowCount = doHashJoin<int8_t>(innerColumn, outerColumn);
        } break;
        case DataType::uint16: {
            rowCount = doHashJoin<uint16_t>(innerColumn, outerColumn);
        } break;
        case DataType::int16: {
            rowCount = doHashJoin<int16_t>(innerColumn, outerColumn);
        } break;
        case DataType::uint32: {
            rowCount = doHashJoin<uint32_t>(innerColumn, outerColumn);
        } break;
        case DataType::int32: {
            rowCount = doHashJoin<int32_t>(innerColumn, outerColumn);
        } break;
        case DataType::timestamp:
        case DataType::string_enc:
        case DataType::uint64: {
            rowCount = doHashJoin<uint64_t>(innerColumn, outerColumn);
        } break;
        case DataType::int64: {
            rowCount = doHashJoin<int64_t>(innerColumn, outerColumn);
        } break;
        /**
         * TODO: Current limitation of available TSL Primitives
         */
        // case DataType::f32: {
        //     doHashJoin<float>(innerColumn, outerColumn);
        // } break;
        // case DataType::f64: {
        //     doHashJoin<double>(innerColumn, outerColumn);
        // } break;
        case DataType::f32:
        case DataType::f64:
        case DataType::position_list:
        case DataType::bitmask:
        case DataType::unknown: {
            response.set_info("[HashJoinOperator] ERROR - unsupported datatype.");
            response.set_success(false);
            LOG_ERROR("[HashJoinOperator] Unsupported data type: " << static_cast<int>(innerColumn->datatype) << "." << std::endl;)
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