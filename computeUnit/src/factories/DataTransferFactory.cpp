#include <factories/DataTransferFactory.hpp>
#include <operators/DataTransferOperator.hpp>

DataTransferFactory::DataTransferFactory() : AbstractOperatorFactory() {
}

AbstractOperator* DataTransferFactory::create_operator(WorkItem& witem) {
    return new DataTransferOperator(witem);
}
