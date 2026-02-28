#include <factories/AbstractOperatorFactory.hpp>

AbstractOperatorFactory::AbstractOperatorFactory() {
}

AbstractOperator* AbstractOperatorFactory::create_operator(WorkItem& witem) {
    return nullptr;
}

AbstractOperator* AbstractOperatorFactory::create_operator_request(WorkRequest& request) {
    return nullptr;
}