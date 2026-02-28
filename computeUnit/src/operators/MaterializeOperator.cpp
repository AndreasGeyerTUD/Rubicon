#include <operators/MaterializeOperator.hpp>

WorkResponse MaterializeOperator::run() {
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

    auto materializeData = m_work_item.materializedata();
    LOG_DEBUG1("[MaterializeOperator] Filter-Col: " << materializeData.filtercolumn().tabname() << "." << materializeData.filtercolumn().colname() << std::endl;)
    LOG_DEBUG1("[MaterializeOperator] Index-Col: " << materializeData.indexcolumn().tabname() << "." << materializeData.indexcolumn().colname() << std::endl;)
    auto column = DataCatalog::getInstance().getColumnByName(materializeData.filtercolumn().tabname(), materializeData.filtercolumn().colname());
    auto idxCol = DataCatalog::getInstance().getColumnByName(materializeData.indexcolumn().tabname(), materializeData.indexcolumn().colname());

    if (!column) {
        response.set_info("[MaterializeOperator] ERROR - Could not find the column (" + materializeData.filtercolumn().tabname() + "." + materializeData.filtercolumn().colname() + "). Aborting execution.");
        response.set_success(false);
        LOG_ERROR("[MaterializeOperator] Could not find the column (" << materializeData.filtercolumn().tabname() << "." << materializeData.filtercolumn().colname() << "). Aborting execution." << std::endl;)
        return response;
    }

    if (!idxCol) {
        response.set_info("[MaterializeOperator] ERROR - Could not find the column (" + materializeData.indexcolumn().tabname() + "." + materializeData.indexcolumn().colname() + "). Aborting execution.");
        response.set_success(false);
        LOG_ERROR("[MaterializeOperator] Could not find the column (" << materializeData.indexcolumn().tabname() << "." << materializeData.indexcolumn().colname() << "). Aborting execution." << std::endl;)
        return response;
    }

    uint64_t rowCount = 0;

    switch (column->datatype) {
        case DataType::uint8: {
            rowCount = select_index_type<uint8_t>(column, idxCol);
        } break;
        case DataType::int8: {
            rowCount = select_index_type<int8_t>(column, idxCol);
        } break;
        case DataType::uint16: {
            rowCount = select_index_type<uint16_t>(column, idxCol);
        } break;
        case DataType::int16: {
            rowCount = select_index_type<int16_t>(column, idxCol);
        } break;
        case DataType::uint32: {
            rowCount = select_index_type<uint32_t>(column, idxCol);
        } break;
        case DataType::int32: {
            rowCount = select_index_type<int32_t>(column, idxCol);
        } break;
        case DataType::timestamp:  // Fallthrough intended; Let's see if this works
        case DataType::position_list:
        case DataType::string_enc: 
        case DataType::uint64: {
            rowCount = select_index_type<uint64_t>(column, idxCol);
        } break;
        case DataType::int64: {
            rowCount = select_index_type<int64_t>(column, idxCol);
        } break;
        case DataType::f32: {
            rowCount = select_index_type<float>(column, idxCol);
        } break;
        case DataType::f64: {
            rowCount = select_index_type<double>(column, idxCol);
        } break;
        case DataType::bitmask: {
            response.set_info("[MaterializeOperator] Error - Bitmask operator not implemented so far.");
            response.set_success(false);
            LOG_ERROR("[MaterializeOperator] Bitmask operator not implemented so far." << std::endl;)
        } break;
        case DataType::unknown: {
            response.set_info("[MaterializeOperator] Error - unknown data type.");
            response.set_success(false);
            LOG_ERROR("[MaterializeOperator] Error - unknown data type." << std::endl;)
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