#pragma once

#include <WorkItem.pb.h>
#include <WorkRequest.pb.h>
#include <WorkResponse.pb.h>
#include <tsl.hpp>

/**
 * @brief An abstract operator skeleton. Servers as base class/interface for concrete operator implementations.
 * 
 */
class AbstractOperator {
   public:
    AbstractOperator(WorkItem p_witem) : m_work_item(p_witem) {};
    AbstractOperator(WorkRequest p_request) : m_work_request(p_request) {};
    virtual ~AbstractOperator() = default;

    virtual WorkResponse run() = 0;

   protected:
    WorkItem m_work_item;
    WorkRequest m_work_request;
};