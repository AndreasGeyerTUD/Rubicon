#pragma once

#include <factories/AbstractOperatorFactory.hpp>

/**
 * @brief Creates operators that joins column against another one.
 * 
 */
class JoinFactory : public AbstractOperatorFactory {
   public:
    JoinFactory();
    virtual AbstractOperator* create_operator(WorkItem& witem) override;
};