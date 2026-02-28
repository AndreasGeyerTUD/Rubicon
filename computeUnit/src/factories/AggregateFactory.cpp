#include <factories/AggregateFactory.hpp>
#include <operators/AggregateOperator.hpp>

AggregateFactory::AggregateFactory() : AbstractOperatorFactory() {
}

AbstractOperator* AggregateFactory::create_operator(WorkItem& witem) {
    AbstractOperator* op = nullptr;
    switch( witem.operatorid() ) {
        case OperatorType::OP_AGGREGATE: {
            op = new AggregateOperator(witem);
        }break;
        default: {
            LOG_ERROR("[AggregateFactory] Error. Requesting unknown Operator type: " << OperatorType_Name(witem.operatorid()) << std::endl;)
            exit(-11);
        }
    }
    return op;
}
