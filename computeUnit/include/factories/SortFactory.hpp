#pragma once

#include <factories/AbstractOperatorFactory.hpp>

/**
 * @brief Creates operators that compare columns against one or multiple attributes.
 * 
 */
class SortFactory : public AbstractOperatorFactory {
   public:
    SortFactory();
    virtual AbstractOperator* create_operator(WorkItem& witem) override;
};