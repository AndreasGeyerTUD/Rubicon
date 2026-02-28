#include <factories/SortFactory.hpp>

#include <operators/SortOperator.hpp>

SortFactory::SortFactory() : AbstractOperatorFactory() {
}

AbstractOperator* SortFactory::create_operator(WorkItem& witem) {
    AbstractOperator* op = nullptr;
    switch( witem.operatorid() ) {
        case OperatorType::OP_SORT: {
            op = new SortOperator(witem);
        }break;
        default: {
            LOG_ERROR("[SortFactory] Error. Requesting unknown Operator type: " << OperatorType_Name(witem.operatorid()) << std::endl;)
            exit(-11);
        }
    }
    return op;
}
