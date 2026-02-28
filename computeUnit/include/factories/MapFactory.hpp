#pragma once

#include <factories/AbstractOperatorFactory.hpp>

/**
 * @brief Creates operators that maps an arithmetic function to one or two columns.
 * 
 */
class MapFactory : public AbstractOperatorFactory {
   public:
    MapFactory();
    virtual AbstractOperator* create_operator(WorkItem& witem) override;
};