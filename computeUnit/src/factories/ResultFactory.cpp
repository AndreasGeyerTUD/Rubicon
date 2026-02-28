#include <factories/ResultFactory.hpp>
#include <operators/ResultOperator.hpp>

ResultFactory::ResultFactory() : AbstractOperatorFactory() {
}

AbstractOperator* ResultFactory::create_operator(WorkItem& witem) {
    return new ResultOperator(witem);
}
