#pragma once

#include <factories/AbstractOperatorFactory.hpp>

/**
 * @brief Creates operators that perform relational operators, such as intersection, union, etc.
 * 
 */
class SetOperatorFactory : public AbstractOperatorFactory {
   public:
    SetOperatorFactory();
    virtual AbstractOperator* create_operator(WorkItem& witem) override;
};