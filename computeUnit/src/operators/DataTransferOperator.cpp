#include <operators/DataTransferOperator.hpp>

WorkResponse DataTransferOperator::run() {
    WorkResponse response;
    response.set_planid(0);
    response.set_itemid(0);
    response.set_success(true);

    DataTransferItem transfer = m_work_item.transferdata();

    LOG_DEBUG1("[FilterOperator] Column: " << transfer.source().tabname() << "." << transfer.source().colname() << std::endl;)
    auto column = DataCatalog::getInstance().getColumnByName(transfer.source().tabname(), transfer.source().colname());

    const size_t columnSize = column->sizeInBytes;

    // std::shared_ptr<Column> resColumn = std::make_shared<Column>(0, column->datatype, columnSize, column->elements, false, false, -1);
    // DataCatalog::getInstance().addColumn(transfer.destination().tabname(), transfer.destination().colname(), resColumn);
    auto resColumn = DataCatalog::getInstance().getColumnByName(transfer.destination().tabname(), transfer.destination().colname());

    if (column->datatype == DataType::string_enc) {
        resColumn->initDictionary(column->dictionaryEncoding);
    }

    resColumn->allocate();

    const size_t chunkSize = (1024 * 1024 * 4 <= columnSize) ? (1024 * 1024 * 4) : columnSize;  // 4 MiB or size of the column if smaller than 4 MiB
    size_t offset = 0;
    char* start = static_cast<char*>(column->data);
    while (true) {
        if (offset + chunkSize >= columnSize) {
            size_t remainder = columnSize - offset;
            resColumn->appendChunk(offset, remainder, start);
            resColumn->advance_end_pointer(remainder);
            break;
        }

        resColumn->appendChunk(offset, chunkSize, start);
        resColumn->advance_end_pointer(chunkSize);

        offset += chunkSize;
        start += chunkSize;
    }

    resColumn->isComplete = true;

    return response;
}