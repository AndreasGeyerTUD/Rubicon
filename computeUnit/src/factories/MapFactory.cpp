#include <factories/MapFactory.hpp>

#include <operators/MapScalarOperator.hpp>
#include <operators/MapColumnsOperator.hpp>

MapFactory::MapFactory() : AbstractOperatorFactory() {
}

AbstractOperator* MapFactory::create_operator(WorkItem& witem) {
    AbstractOperator* op = nullptr;
    switch( witem.operatorid() ) {
        case OperatorType::OP_MAP: {
            if (witem.mapdata().has_partnercolumn()) {
                op = new MapColumnsOperator(witem);
            } else if (witem.mapdata().has_staticval()) {
                op = new MapScalarOperator(witem);
            } else {
                LOG_ERROR("[MapFactory] Error. Requesting Map operator without a partner column or static value." << std::endl;)
                exit(-10);
            }
        }break;
        default: {
            LOG_ERROR("[MapFactory] Error. Requesting unknown Operator type: " << OperatorType_Name(witem.operatorid()) << std::endl;)
            exit(-11);
        }
    }
    return op;
}
