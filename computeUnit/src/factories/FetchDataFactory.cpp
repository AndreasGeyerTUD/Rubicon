#include <factories/FetchDataFactory.hpp>
#include <operators/FetchDataOperator.hpp>

FetchDataFactory::FetchDataFactory() : AbstractOperatorFactory() {
}

AbstractOperator* FetchDataFactory::create_operator(WorkItem& witem) {
    return new FetchDataOperator(witem);
}
