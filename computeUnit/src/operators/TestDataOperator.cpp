#include <bitset>
#include <operators/TestDataOperator.hpp>

WorkResponse TestDataOperator::run() {
    WorkResponse response;
    response.set_planid(m_work_request.workitem().planid());
    response.set_itemid(m_work_request.testdatasetup().itemid());

    std::string info;
    bool success;

    switch (m_work_request.testdatasetup().action()) {
        case ddd::testing::BenchmarkAction::DATA_CREATE: {
            std::tie(info, success) = dispatch_create_test_column(m_work_request.testdatasetup());
        }; break;
        case ddd::testing::BenchmarkAction::DATA_DELETE: {
            std::tie(info, success) = dispatch_remove_column(m_work_request.testdatasetup());
        }; break;
        case ddd::testing::BenchmarkAction::IDLE_TASK: {
            LOG_INFO("I will now sleep for " << m_work_request.testdatasetup().elementcount() << " seconds." << std::endl;)
            std::this_thread::sleep_for(std::chrono::seconds(m_work_request.testdatasetup().elementcount()));
            LOG_INFO("Awake after " << m_work_request.testdatasetup().elementcount() << " seconds." << std::endl;)
            info = "[IdleTask] I slept for " + std::to_string(m_work_request.testdatasetup().elementcount()) + " seconds.";
            success = true;
        } break;
        default: {
            info = "[TestDataSetup] Unnown storage action. Nothing done.";
            success = false;
        };
    }

    response.set_info(info);
    response.set_success(success);
    return response;
}