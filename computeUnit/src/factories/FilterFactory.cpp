#include <factories/FilterFactory.hpp>

#include <operators/IdxScanOperator.hpp>
#include <operators/FilterOperator.hpp>

FilterFactory::FilterFactory() : AbstractOperatorFactory() {
}

AbstractOperator* FilterFactory::create_operator(WorkItem& witem) {
    AbstractOperator* op = nullptr;
    switch( witem.operatorid() ) {
        case OperatorType::OP_IDXSCAN: {
            LOG_ERROR("[FilterFactory] Error. Index Scan is not yet implemented." << std::endl;)
            exit(-11);
            // op = new IdxScanOperator(item);
        } break;
        case OperatorType::OP_FILTER: {
            op = new FilterOperator(witem);
        }break;
        default: {
            LOG_ERROR("[FilterFactory] Error. Requesting unknown Operator type: " << OperatorType_Name(witem.operatorid()) << std::endl;)
            exit(-11);
        }
    }
    return op;
}
