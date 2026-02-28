#include <operators/SetUnionOperator.hpp>

WorkResponse SetUnionOperator::run() {
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

    SetOperationItem setData = m_work_item.setdata();

    auto innerCol = DataCatalog::getInstance().getColumnByName(setData.innercolumn().tabname(), setData.innercolumn().colname());
    auto outerCol = DataCatalog::getInstance().getColumnByName(setData.outercolumn().tabname(), setData.outercolumn().colname());


    if (!innerCol) {
        response.set_info("[SetUnionOperator] ERROR - Could not find the column (" + setData.innercolumn().tabname() + "." + setData.innercolumn().colname() + "). Aborting execution.");
        response.set_success(false);
        LOG_ERROR("[SetUnionOperator] Could not find the column (" << setData.innercolumn().tabname() << "." << setData.innercolumn().colname() << "). Aborting execution." << std::endl;)
        return response;
    }

    if (!outerCol) {
        response.set_info("[SetUnionOperator] ERROR - Could not find the column (" + setData.outercolumn().tabname() + "." + setData.outercolumn().colname() + "). Aborting execution.");
        response.set_success(false);
        LOG_ERROR("[SetUnionOperator] Could not find the column (" << setData.outercolumn().tabname() << "." << setData.outercolumn().colname() << "). Aborting execution." << std::endl;)
        return response;
    }

    uint64_t rowCount = 0;

    switch (innerCol->datatype) {
        case DataType::bitmask: {
            rowCount = select_ise<tuddbs::hints::intermediate::bit_mask>(innerCol, outerCol);
        } break;
        case DataType::position_list: {
            rowCount = do_union_pl(innerCol, outerCol);
        } break;
        default: {
            response.set_info("[SetUnionOperator] ERROR - Unsupported column type. Union is currently only implemented with bitmasks.");
            response.set_success(false);
            LOG_ERROR("[SetUnionOperator] Unsupported column type. Union is currently only implemented with bitmasks." << std::endl;)
        }
    }

    if (m_work_item.returnextendedresult()) {
        response.mutable_extendedresult()->set_rowcount(rowCount);
        using namespace std::chrono;
        const uint64_t endTime = duration_cast<nanoseconds>(high_resolution_clock::now().time_since_epoch()).count();
        response.mutable_extendedresult()->set_end(endTime);
    }

    return response;
};