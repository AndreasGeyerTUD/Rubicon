#pragma once

#include <operators/AbstractOperator.hpp>
#include <infrastructure/DataCatalog.hpp>

/**
 * @brief TODO.
 * 
 */
class IdxScanOperator : public AbstractOperator {
   public:
    IdxScanOperator(WorkItem p_witem) : AbstractOperator(p_witem){};
    virtual WorkResponse run() override;
};