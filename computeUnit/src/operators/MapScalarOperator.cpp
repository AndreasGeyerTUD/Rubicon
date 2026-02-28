#include <operators/MapScalarOperator.hpp>

WorkResponse MapScalarOperator::run() {
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
    LOG_DEBUG1("[MapOperator] Column: " << mapData.inputcolumn().tabname() << "." << mapData.inputcolumn().colname() << std::endl;)
    auto inputColumn = DataCatalog::getInstance().getColumnByName(mapData.inputcolumn().tabname(), mapData.inputcolumn().colname());

    if (!inputColumn) {
        response.set_info("[MapOperator] ERROR - Could not find the column (" + mapData.inputcolumn().tabname() + "." + mapData.inputcolumn().colname() + "). Aborting execution.");
        response.set_success(false);
        LOG_ERROR("[MapOperator] Could not find the column (" << mapData.inputcolumn().tabname() << "." << mapData.inputcolumn().colname() << "). Aborting execution." << std::endl;)
        return response;
    }

    uint64_t rowCount = 0;

    switch (inputColumn->datatype) {
        case DataType::uint8: {
            rowCount = selectMap<uint8_t>(inputColumn, mapData.staticval().intval().value());
        } break;
        case DataType::int8: {
            rowCount = selectMap<int8_t>(inputColumn, mapData.staticval().intval().value());
        } break;
        case DataType::uint16: {
            rowCount = selectMap<uint16_t>(inputColumn, mapData.staticval().intval().value());
        } break;
        case DataType::int16: {
            rowCount = selectMap<int16_t>(inputColumn, mapData.staticval().intval().value());
        } break;
        case DataType::uint32: {
            rowCount = selectMap<uint32_t>(inputColumn, mapData.staticval().intval().value());
        } break;
        case DataType::int32: {
            rowCount = selectMap<int32_t>(inputColumn, mapData.staticval().intval().value());
        } break;
        case DataType::uint64: {
            rowCount = selectMap<uint64_t>(inputColumn, mapData.staticval().intval().value());
        } break;
        case DataType::int64: {
            rowCount = selectMap<int64_t>(inputColumn, mapData.staticval().intval().value());
        } break;
        case DataType::f32: {
            rowCount = selectMap<float>(inputColumn, mapData.staticval().floatval().value());
        } break;
        case DataType::f64: {
            rowCount = selectMap<double>(inputColumn, mapData.staticval().floatval().value());
        } break;
        case DataType::position_list:
        case DataType::bitmask:
        case DataType::timestamp:
        case DataType::string_enc:
        case DataType::unknown: {
            response.set_info("[MapOperator] ERROR - unsupported datatype.");
            response.set_success(false);
            LOG_ERROR("[MapOperator] Unsupported data type: " << static_cast<int>(inputColumn->datatype) << "." << std::endl;)
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