#include <factories/SetOperatorFactory.hpp>

#include <operators/SetUnionOperator.hpp>
#include <operators/SetIntersectOperator.hpp>

SetOperatorFactory::SetOperatorFactory() : AbstractOperatorFactory() {
}

AbstractOperator* SetOperatorFactory::create_operator(WorkItem& witem) {
    AbstractOperator* op = nullptr;
    const SetOperationItem& s_item = witem.setdata();
    switch( s_item.operation() ) {
        case RelOp::REL_UNION: {
            op = new SetUnionOperator(witem);
        } break;
        case RelOp::REL_INTERSECTION: {
            op = new SetIntersectOperator(witem);
        }break;
        default: {
            LOG_ERROR("[SetOperatorFactory] Error. Requesting unknown Operator type: " << OperatorType_Name(witem.operatorid()) << std::endl;)
            exit(-11);
        }
    }
    return op;
}
