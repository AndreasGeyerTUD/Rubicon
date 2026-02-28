#pragma once

#include <WorkRequest.pb.h>
#include <WorkItem.pb.h>
#include <operators/AbstractOperator.hpp>

/**
 * @brief An abstract operator factory skeleton. Servers as base class for concrete operator factories.
 * 
 */
class AbstractOperatorFactory {
   public:
    AbstractOperatorFactory();

    virtual AbstractOperator* create_operator_request(WorkRequest& request);
    virtual AbstractOperator* create_operator(WorkItem& witem);
};