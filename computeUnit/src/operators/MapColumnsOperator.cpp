#include <operators/MapColumnsOperator.hpp>

WorkResponse MapColumnsOperator::run() {
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

    auto mapData = m_work_item.mapdata();
    LOG_DEBUG1("[MapColumnsOperator] Columns: " << mapData.inputcolumn().tabname() << "." << mapData.inputcolumn().colname() << ", " << mapData.partnercolumn().tabname() << "." << mapData.partnercolumn().colname() << std::endl;)
    auto inputColumn = DataCatalog::getInstance().getColumnByName(mapData.inputcolumn().tabname(), mapData.inputcolumn().colname());
    auto partnerColumn = DataCatalog::getInstance().getColumnByName(mapData.partnercolumn().tabname(), mapData.partnercolumn().colname());

    if (!inputColumn) {
        response.set_info("[MapColumnsOperator] ERROR - Could not find the column (" + mapData.inputcolumn().tabname() + "." + mapData.inputcolumn().colname() + "). Aborting execution.");
        response.set_success(false);
        LOG_ERROR("[MapColumnsOperator] Could not find the column (" << mapData.inputcolumn().tabname() << "." << mapData.inputcolumn().colname() << "). Aborting execution." << std::endl;)
        return response;
    }

    if (!partnerColumn) {
        response.set_info("[MapColumnsOperator] ERROR - Could not find the column (" + mapData.partnercolumn().tabname() + "." + mapData.partnercolumn().colname() + "). Aborting execution.");
        response.set_success(false);
        LOG_ERROR("[MapColumnsOperator] Could not find the column (" << mapData.partnercolumn().tabname() << "." << mapData.partnercolumn().colname() << "). Aborting execution." << std::endl;)
        return response;
    }

    if (inputColumn->datatype != partnerColumn->datatype) {
        response.set_info("[MapColumnsOperator] ERROR - Data types do not match. Aborting execution.");
        response.set_success(false);
        LOG_ERROR("[MapColumnsOperator] Data types do not match (" << static_cast<int>(inputColumn->datatype) << " != " << static_cast<int>(partnerColumn->datatype) << "). Aborting execution." << std::endl;)
        return response;
    }

    if (inputColumn->elements != partnerColumn->elements) {
        response.set_info("[MapColumnsOperator] ERROR - Columnsizes do not match. Aborting execution.");
        response.set_success(false);
        LOG_ERROR("[MapColumnsOperator] Columnsizes do not match (" << inputColumn->elements << " != " << partnerColumn->elements << "). Aborting execution." << std::endl;)
        return response;
    }

    uint64_t rowCount = 0;

    switch (inputColumn->datatype) { // as tested before, inputColumn->datatype == partnerColumn->datatype
        case DataType::uint8: {
            rowCount = selectMap<uint8_t>(inputColumn, partnerColumn);
        } break;
        case DataType::int8: {
            rowCount = selectMap<int8_t>(inputColumn, partnerColumn);
        } break;
        case DataType::uint16: {
            rowCount = selectMap<uint16_t>(inputColumn, partnerColumn);
        } break;
        case DataType::int16: {
            rowCount = selectMap<int16_t>(inputColumn, partnerColumn);
        } break;
        case DataType::uint32: {
            rowCount = selectMap<uint32_t>(inputColumn, partnerColumn);
        } break;
        case DataType::int32: {
            rowCount = selectMap<int32_t>(inputColumn, partnerColumn);
        } break;
        case DataType::uint64: {
            rowCount = selectMap<uint64_t>(inputColumn, partnerColumn);
        } break;
        case DataType::int64: {
            rowCount = selectMap<int64_t>(inputColumn, partnerColumn);
        } break;
        case DataType::f32: {
            rowCount = selectMap<float>(inputColumn, partnerColumn);
        } break;
        case DataType::f64: {
            rowCount = selectMap<double>(inputColumn, partnerColumn);
        } break;
        case DataType::position_list:
        case DataType::bitmask:
        case DataType::timestamp:
        case DataType::string_enc:
        case DataType::unknown: {
            response.set_info("[MapColumnsOperator] ERROR - unsupported datatype.");
            response.set_success(false);
            LOG_ERROR("[MapColumnsOperator] Unsupported data type: " << static_cast<int>(inputColumn->datatype) << "." << std::endl;)
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