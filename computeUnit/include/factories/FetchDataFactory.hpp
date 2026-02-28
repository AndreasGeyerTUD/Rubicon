#pragma once

#include <factories/AbstractOperatorFactory.hpp>

/**
 * @brief Creates operators that retrieve data from the DataCatalog.
 * 
 */
class FetchDataFactory : public AbstractOperatorFactory {
   public:
    FetchDataFactory();
    virtual AbstractOperator* create_operator(WorkItem& witem) override;
};