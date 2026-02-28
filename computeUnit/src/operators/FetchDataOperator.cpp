#include <bitset>
#include <operators/FetchDataOperator.hpp>

template <>
std::string FetchDataOperator::col_to_string<size_t, true>(std::shared_ptr<Column> col, const uint64_t chunkElements) {
    std::stringstream ss;

    LOG_DEBUG1("[FetchDataOperator] Fetching a bitmask with " << col->elements << " Bytes." << std::endl;)

    using print_t = uint8_t;

    col->wait_data_allocated();
    View chunk = View<print_t>(static_cast<print_t*>(col->data), chunkElements, col);

    while (true) {
        print_t* data = chunk.begin();

        if (!col->data) {
            ss << "Column data is corrupted.";
            return ss.str();
        }

        for (size_t i = 0; i < chunk.getNumberOfElements(); ++i) {
            std::bitset<sizeof(print_t) * CHAR_BIT> bs(*(data + i));
            for (size_t bit = 0; bit < bs.size(); ++bit) {
                ss << bs[bit];
            }
            ss << " ";
        }

        if (chunk.isLastChunk()) {
            break;
        }

        chunk++;
    }

    return ss.str();
}

WorkResponse FetchDataOperator::run() {
    WorkResponse response;
    response.set_planid(m_work_item.planid());
    response.set_itemid(m_work_item.itemid());
    response.set_success(true);

    auto fetchData = m_work_item.fetchdata();
    auto col = DataCatalog::getInstance().getColumnByName(fetchData.inputcolumn().tabname(), fetchData.inputcolumn().colname());

    if (!col) {
        response.set_info("[FetchDataOperator] Column not found.");
        response.set_success(false);
        LOG_ERROR("[FetchDataOperator] Column not found (" << fetchData.inputcolumn().tabname() << "." << fetchData.inputcolumn().colname() << ")." << std::endl;)
        return response;
    }

    switch (col->datatype) {
        case DataType::uint8: {
            response.set_info(col_to_string<uint8_t>(col));
        } break;
        case DataType::int8: {
            response.set_info(col_to_string<int8_t>(col));
        } break;
        case DataType::uint16: {
            response.set_info(col_to_string<uint16_t>(col));
        } break;
        case DataType::int16: {
            response.set_info(col_to_string<int16_t>(col));
        } break;
        case DataType::uint32: {
            response.set_info(col_to_string<uint32_t>(col));
        } break;
        case DataType::int32: {
            response.set_info(col_to_string<int32_t>(col));
        } break;
        case DataType::timestamp:  // Fallthrough intended; Let's see if this works
        case DataType::uint64: {
            response.set_info(col_to_string<uint64_t>(col));
        } break;
        case DataType::int64: {
            response.set_info(col_to_string<int64_t>(col));
        } break;
        case DataType::f32: {
            response.set_info(col_to_string<float>(col));
        } break;
        case DataType::f64: {
            response.set_info(col_to_string<double>(col));
        } break;
        case DataType::position_list: {
            response.set_info(col_to_string<size_t>(col));
        } break;
        case DataType::bitmask: {
            response.set_info(col_to_string<size_t, true>(col));
        } break;
        case DataType::string_enc: {
            response.set_info(stringEncColToString(col));
        } break;
        case DataType::unknown: {
            response.set_info("[FetchDataOperator] ERROR - unsupported datatype.");
            response.set_success(false);
            LOG_ERROR("[FetchDataOperator] Unsupported data type: " << static_cast<int>(col->datatype) << "." << std::endl;)
        }
    }

    return response;
}