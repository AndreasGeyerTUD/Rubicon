#pragma once

#include <operators/AbstractOperator.hpp>
#include <infrastructure/DataCatalog.hpp>

/**
 * @brief TODO.
 * 
 */
class MergeJoinOperator : public AbstractOperator {
   public:
    MergeJoinOperator(WorkItem p_witem) : AbstractOperator(p_witem){};
    virtual WorkResponse run() override;
};