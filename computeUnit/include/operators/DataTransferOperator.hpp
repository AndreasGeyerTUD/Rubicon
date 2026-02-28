#pragma once

#include <infrastructure/DataCatalog.hpp>
#include <operators/AbstractOperator.hpp>

/**
 * @brief The operator converts all elements of a column into a string representation.
 *
 */
class DataTransferOperator : public AbstractOperator {
   public:
    DataTransferOperator(WorkItem p_witem) : AbstractOperator(p_witem) {};
    virtual WorkResponse run() override;
};