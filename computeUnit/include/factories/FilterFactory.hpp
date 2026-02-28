#pragma once

#include <factories/AbstractOperatorFactory.hpp>

/**
 * @brief Creates operators that compare columns against one or multiple attributes.
 * 
 */
class FilterFactory : public AbstractOperatorFactory {
   public:
    FilterFactory();
    virtual AbstractOperator* create_operator(WorkItem& witem) override;
};