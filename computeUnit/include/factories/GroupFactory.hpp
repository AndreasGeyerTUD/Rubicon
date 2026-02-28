#pragma once

#include <factories/AbstractOperatorFactory.hpp>

/**
 * @brief Creates operators that compare columns against one or multiple attributes.
 * 
 */
class GroupFactory : public AbstractOperatorFactory {
   public:
    GroupFactory();
    virtual AbstractOperator* create_operator(WorkItem& witem) override;
};