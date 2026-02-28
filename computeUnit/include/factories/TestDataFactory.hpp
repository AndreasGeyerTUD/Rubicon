#pragma once

#include <factories/AbstractOperatorFactory.hpp>

/**
 * @brief Creates operators that retrieve data from the DataCatalog.
 * 
 */
class TestDataFactory : public AbstractOperatorFactory {
   public:
    TestDataFactory();
    virtual AbstractOperator* create_operator_request(WorkRequest& request) override;
};