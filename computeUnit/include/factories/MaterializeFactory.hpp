#pragma once

#include <factories/AbstractOperatorFactory.hpp>

/**
 * @brief Creates operators that filter columns, i.e. usually produce a subset of the input column.
 * 
 */
class MaterializeFactory : public AbstractOperatorFactory {
   public:
    MaterializeFactory();
    virtual AbstractOperator* create_operator(WorkItem& witem) override;
};