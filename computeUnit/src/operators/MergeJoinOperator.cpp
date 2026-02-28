#include <operators/MergeJoinOperator.hpp>

WorkResponse MergeJoinOperator::run() {
    WorkResponse response;
    response.set_planid(m_work_item.planid());
    response.set_itemid(m_work_item.itemid());
    response.set_info("[MergeJoinOperator] Please implement me. Nothing happened.");
    response.set_success(false);
    LOG_ERROR("[MergeJoinOperator] Please implement me. Nothing happened." << std::endl;)
    return response;
}