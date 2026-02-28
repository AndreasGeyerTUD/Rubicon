#pragma once

#include <factories/AbstractOperatorFactory.hpp>

/**
 * @brief Creates operators that retrieve data from the DataCatalog.
 * 
 */
class ResultFactory : public AbstractOperatorFactory {
   public:
   ResultFactory();
    virtual AbstractOperator* create_operator(WorkItem& witem) override;
};