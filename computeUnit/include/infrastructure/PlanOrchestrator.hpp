#pragma once

#include <atomic>
#include <condition_variable>
#include <cstdint>
#include <functional>
#include <memory>
#include <mutex>
#include <queue>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include <vector>
#include <variant>
#include <optional>

#include <infrastructure/OperatorDispatcher.hpp>

#include "WorkItem.pb.h"
#include "QueryPlan.pb.h"

namespace orchestrator {

enum class PlanStatus {
    Active,
    Completed,
    Cancelled,
    PendingCleanup
};

enum class ItemStatus {
    Pending,
    Ready,
    Dispatched,
    Completed,
    Failed,
    Cancelled
};

struct ItemState {
    uint32_t itemId;
    ItemStatus status;
    std::vector<uint32_t> dependencies;
    std::vector<uint32_t> dependents;
    WorkItem workItem;  // Store the actual work item for dispatch
};

struct PlanContext {
    uint32_t planId;
    PlanStatus status;
    uint64_t target_uuid;
    std::unordered_map<uint32_t, ItemState> items;
    std::unordered_set<uint32_t> pendingItems;
    std::unordered_set<uint32_t> completedItems;
};

class PlanOrchestrator {
public:
    struct Config {
        std::chrono::milliseconds gcInterval{5000};  // GC runs every 5 seconds by default
        size_t maxPendingCleanup{100};  // Force GC if cleanup queue exceeds this
    };

    explicit PlanOrchestrator(Config config);
    ~PlanOrchestrator();

    // Non-copyable, non-movable (owns threads)
    PlanOrchestrator(const PlanOrchestrator&) = delete;
    PlanOrchestrator& operator=(const PlanOrchestrator&) = delete;
    PlanOrchestrator(PlanOrchestrator&&) = delete;
    PlanOrchestrator& operator=(PlanOrchestrator&&) = delete;

    void setDispatcher(OperatorDispatcher* dispatcher);

    // Submit a new query plan for orchestration
    // Thread-safe: may be called from multiple threads
    bool submitPlan(const QueryPlan& plan, const uint64_t target_uuid);

    // Called externally when a WorkItem completes successfully
    // Thread-safe: may be called from multiple threads
    void onItemCompleted(uint32_t planId, uint32_t itemId);

    // Called externally when a WorkItem fails
    // Thread-safe: may be called from multiple threads
    void onItemFailed(uint32_t planId, uint32_t itemId);

    // Cancel all remaining items in a plan and mark for cleanup
    // Thread-safe: may be called from multiple threads
    void cancelPlan(uint32_t planId);

    // Finalize a completed plan (marks for cleanup)
    // Thread-safe: may be called from multiple threads
    void finalizePlan(uint32_t planId);

    // Query plan status
    // Thread-safe: may be called from multiple threads
    std::optional<PlanStatus> getPlanStatus(uint32_t planId) const;

    // Query item status
    // Thread-safe: may be called from multiple threads
    std::optional<ItemStatus> getItemStatus(uint32_t planId, uint32_t itemId) const;

    // Graceful shutdown
    void shutdown();

private:
    // Internal methods (must be called with appropriate locks held)
    void scheduleReadyItems(PlanContext& ctx);
    bool areAllDependenciesMet(const PlanContext& ctx, const ItemState& item) const;
    void markPlanForCleanup(uint32_t planId);
    void dispatchItem(PlanContext& ctx, ItemState& item);

    // Thread entry points
    void orchestratorThreadFunc();
    void gcThreadFunc();

    // Event types for the orchestrator queue
    struct SubmitPlanEvent {
        QueryPlan plan;
        uint64_t target_uuid;
    };

    struct ItemCompletedEvent {
        uint32_t planId;
        uint32_t itemId;
    };

    struct ItemFailedEvent {
        uint32_t planId;
        uint32_t itemId;
    };

    struct CancelPlanEvent {
        uint32_t planId;
    };

    struct FinalizePlanEvent {
        uint32_t planId;
    };

    struct ShutdownEvent {};

    using Event = std::variant<
        SubmitPlanEvent,
        ItemCompletedEvent,
        ItemFailedEvent,
        CancelPlanEvent,
        FinalizePlanEvent,
        ShutdownEvent
    >;

    // Process events
    void processEvent(const SubmitPlanEvent& event);
    void processEvent(const ItemCompletedEvent& event);
    void processEvent(const ItemFailedEvent& event);
    void processEvent(const CancelPlanEvent& event);
    void processEvent(const FinalizePlanEvent& event);
    void processEvent(const ShutdownEvent& event);

    // Configuration
    Config config_;

    // Dispatcher reference
    OperatorDispatcher* dispatcher_ = nullptr;

    // Plan state (accessed only by orchestrator thread, except for reads)
    mutable std::mutex plansMutex_;
    std::unordered_map<uint32_t, PlanContext> plans_;

    // Event queue for orchestrator thread
    std::mutex eventQueueMutex_;
    std::condition_variable eventQueueCV_;
    std::queue<Event> eventQueue_;

    // Cleanup queue for GC thread
    std::mutex cleanupMutex_;
    std::condition_variable cleanupCV_;
    std::queue<uint32_t> cleanupQueue_;

    // Thread management
    std::atomic<bool> running_{false};
    std::thread orchestratorThread_;
    std::thread gcThread_;
};

} // namespace orchestrator