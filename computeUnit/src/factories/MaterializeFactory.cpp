#include <factories/MaterializeFactory.hpp>
#include <operators/MaterializeOperator.hpp>

MaterializeFactory::MaterializeFactory() : AbstractOperatorFactory() {
}

AbstractOperator* MaterializeFactory::create_operator(WorkItem& witem) {
    AbstractOperator* op = nullptr;
    switch (witem.operatorid()) {
        case OperatorType::OP_MATERIALIZE: {
            op = new MaterializeOperator(witem);
        } break;
        default: {
            LOG_ERROR("[MaterializeFactory] Error. Requesting unknown Operator type: " << OperatorType_Name(witem.operatorid()) << std::endl;)
            exit(-11);
        }
    }
    return op;
}
