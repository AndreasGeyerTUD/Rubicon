#pragma once

#include <factories/AbstractOperatorFactory.hpp>

/**
 * @brief Creates operators that transfers columns.
 * 
 */
class DataTransferFactory : public AbstractOperatorFactory {
   public:
    DataTransferFactory();
    virtual AbstractOperator* create_operator(WorkItem& witem) override;
};