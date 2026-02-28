#include <factories/TestDataFactory.hpp>
#include <operators/TestDataOperator.hpp>

TestDataFactory::TestDataFactory() : AbstractOperatorFactory() {
}

AbstractOperator* TestDataFactory::create_operator_request(WorkRequest& request) {
    return new TestDataOperator(request);
}
