#include <operators/NestedLoopJoinOperator.hpp>

WorkResponse NestedLoopJoinOperator::run() {
    WorkResponse response;
    response.set_planid(m_work_item.planid());
    response.set_itemid(m_work_item.itemid());
    response.set_info("[NestedLoopJoinOperator] Please implement me. Nothing happened.");
    response.set_success(false);
    LOG_ERROR("[NestedLoopJoinOperator] Please implement me. Nothing happened." << std::endl;)
    return response;
}