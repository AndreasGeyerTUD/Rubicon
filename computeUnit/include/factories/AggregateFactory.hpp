#pragma once

#include <factories/AbstractOperatorFactory.hpp>

/**
 * @brief Creates operators that compare columns against one or multiple attributes.
 * 
 */
class AggregateFactory : public AbstractOperatorFactory {
   public:
    AggregateFactory();
    virtual AbstractOperator* create_operator(WorkItem& witem) override;
};