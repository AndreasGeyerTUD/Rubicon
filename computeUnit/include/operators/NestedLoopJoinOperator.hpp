#pragma once

#include <operators/AbstractOperator.hpp>
#include <infrastructure/DataCatalog.hpp>

/**
 * @brief TODO.
 * 
 */
class NestedLoopJoinOperator : public AbstractOperator {
   public:
    NestedLoopJoinOperator(WorkItem p_witem) : AbstractOperator(p_witem){};
    virtual WorkResponse run() override;
};