"""
Workload executor: dispatches SSB queries according to configured patterns.

Integrates with the existing TCPClient, query functions, and synchronization
primitives (complete_plans, complete_condition, etc.) from ssb_workload.py.

Three execution modes:
    _execute_mix            - Fire total_queries according to weighted mix + arrival model
    _execute_phases         - Run through timed phases, each with its own arrival + optional mix
    _execute_virtual_users  - Simulate N analysts with sequential query-think-query loops

All modes populate the full QueryResult with instrumentation fields:
    workload_name, query_flight, phase_name, user_id, sequence_number,
    concurrent_in_flight, planned_delay_s, submit_timestamp
"""

import random
import time
import threading
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, List, Callable, Optional

from utils import msg
from utils import client as tcp_client
from utils import util
from utils.unique_id import new_uint32_id
from utils.queries import (
    q_1_1, q_1_2, q_1_3,
    q_2_1, q_2_2, q_2_3,
    q_3_1, q_3_2, q_3_3, q_3_4,
    q_4_1, q_4_2, q_4_3,
)
from proto import WorkRequest_pb2 as WorkRequest
from proto import WorkResponse_pb2 as WorkResponse
from proto import NetworkRequests_pb2 as NetworkRequests
from proto import ConfigurationRequests_pb2 as ConfigurationRequests
from proto.UnitDefinition_pb2 import TcpPackageType, UnitType

from utils.workload_config import WorkloadConfig
from utils.arrival import RampingArrival, create_arrival_model
from utils.query_result import QueryResult, extract_query_flight


name_list = []
uuid_list = []
complete_plans: Dict[int, bool] = {}
uuid_condition = threading.Condition()
complete_condition = threading.Condition()
# The `update_uuids` function is a callback function that is used to update the pairs for Unit
# name and their registered UUID. It takes a `TCPMessage` object as input, extracts the payload
# from the message, and then parses it into a `UuidForUnitResponse` object from the
# `NetworkRequests` protocol buffer.


def update_uuids(message: msg.TCPMessage):
    global name_list, uuid_list, uuid_condition
    response = NetworkRequests.UuidForUnitResponse()
    response.ParseFromString(message.payload)
    with uuid_condition:
        name_list.clear()
        uuid_list.clear()
        for name in response.names:
            name_list.append(name)
        for uuid in response.uuids:
            uuid_list.append(uuid)
        uuid_condition.notify_all()


# The `wait_next_completion` function is designed to wait until a specific plan ID (`planId`) is
# notified to be complete. It takes a `TCPClient` object and a plan ID as input parameters.
def wait_next_completion(client, plan_id, timeout=300.0):
    with complete_condition:
        deadline = time.time() + timeout
        while plan_id not in complete_plans and client.connection_up:
            remaining = deadline - time.time()
            if remaining <= 0:
                print(f"[Warn] Timeout waiting for plan_id={plan_id}")
                return False
            complete_condition.wait(timeout=remaining)

        return complete_plans.get(plan_id, False)


# The `wait_next_completions` function is designed to wait until all the specified plan IDs in
# the `planIds` list are notified to be complete. It takes a `TCPClient` object and a list of plan
# IDs as input parameters.
def wait_next_completions(client: tcp_client.TCPClient, planIds: list[int]):
    global complete_condition, complete_plans
    with complete_condition:
        while not all(planId in complete_plans for planId in planIds) and client.connection_up:
            complete_condition.wait()


def plan_finished_cb(message: msg.TCPMessage):
    global complete_condition, complete_plans
    response: WorkResponse.PlanResponse = WorkResponse.PlanResponse()
    response.ParseFromString(message.payload)
    with complete_condition:
        # print(f"Received response: {response}")
        complete_plans[response.planId] = response.success
        complete_condition.notify_all()

    print(datetime.now().strftime('%H:%M:%S'))
    print(response)


def run_query(client: tcp_client.TCPClient, query: msg.TCPMessage, query_name: str, mode: str, scale_factor: int, iteration: int, workers: int) -> tuple[int, str]:
    request = WorkRequest.WorkRequest()
    request.ParseFromString(query.payload)

    t_s = datetime.now()
    client.send_message(query)
    success = wait_next_completion(client, request.queryPlan.planid)
    t_e = datetime.now()
    return QueryResult(start=t_s.timestamp(), end=t_e.timestamp(), elapsed=(t_e-t_s).total_seconds(), name=query_name, success=success, mode=mode, scale_factor=scale_factor, iteration=iteration, workers=workers, window_size=0, max_overhead=0.0)


# ---------------------------------------------------------------------------
# Query function registry
# ---------------------------------------------------------------------------

QUERY_FUNCTIONS: Dict[str, Callable] = {
    "q_1_1": q_1_1, "q_1_2": q_1_2, "q_1_3": q_1_3,
    "q_2_1": q_2_1, "q_2_2": q_2_2, "q_2_3": q_2_3,
    "q_3_1": q_3_1, "q_3_2": q_3_2, "q_3_3": q_3_3, "q_3_4": q_3_4,
    "q_4_1": q_4_1, "q_4_2": q_4_2, "q_4_3": q_4_3,
}

AVAILABLE_QUERIES = list(QUERY_FUNCTIONS.keys())


# ---------------------------------------------------------------------------
# In-flight tracker (thread-safe)
# ---------------------------------------------------------------------------

class InFlightTracker:
    """
    Thread-safe counter for tracking the number of queries currently in-flight.

    Usage:
        tracker = InFlightTracker()
        snapshot = tracker.acquire()   # increments and returns count BEFORE increment
        ...                            # query executes
        tracker.release()              # decrements
    """

    def __init__(self):
        self._count = 0
        self._lock = threading.Lock()

    def acquire(self) -> int:
        """Increment in-flight count. Returns the count BEFORE incrementing
        (= concurrent queries already running at dispatch time)."""
        with self._lock:
            snapshot = self._count
            self._count += 1
            return snapshot

    def release(self):
        """Decrement in-flight count."""
        with self._lock:
            self._count -= 1

    @property
    def count(self) -> int:
        with self._lock:
            return self._count


# ---------------------------------------------------------------------------
# Sequence counter (thread-safe)
# ---------------------------------------------------------------------------

class SequenceCounter:
    """Thread-safe monotonically increasing counter for global dispatch ordering."""

    def __init__(self):
        self._counter = 0
        self._lock = threading.Lock()

    def next(self) -> int:
        with self._lock:
            val = self._counter
            self._counter += 1
            return val

    def reset(self):
        with self._lock:
            self._counter = 0


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _pick_query(query_mix: Dict[str, float]) -> str:
    """Weighted random selection from a query mix."""
    names = list(query_mix.keys())
    weights = list(query_mix.values())
    return random.choices(names, weights=weights, k=1)[0]


def _build_query(query_name: str, src_uuid: int, tgt_uuid: int, scale_factor: int):
    """Build a query TCPMessage from a query name."""
    func = QUERY_FUNCTIONS[query_name]
    plan_id = new_uint32_id()
    return func(planId=plan_id, src_uuid=src_uuid, target_uuid=tgt_uuid, scale_factor=scale_factor)


def _discover_compute_unit(client: tcp_client.TCPClient) -> tuple:
    """
    Fetch all registered units and return (src_uuid, tgt_uuid).
    Blocks until UUIDs are received.
    """
    work = util.create_uuid_request_item(type=UnitType.COMPUTE_UNIT)
    client.send_message(work)
    with uuid_condition:
        if len(uuid_list) == 0 and client.connection_up:
            uuid_condition.wait()

    tgt_uuid = 0
    for name, uuid in zip(name_list, uuid_list):
        if "ComputeUnit" in name:
            tgt_uuid = uuid
            break

    if tgt_uuid == 0:
        raise RuntimeError("Could not find a ComputeUnit.")

    return client.uuid, tgt_uuid


def _configure_workers(client: tcp_client.TCPClient, src_uuid: int, workers: int):
    """Set worker count on all ComputeUnits."""
    for name, uuid in zip(name_list, uuid_list):
        if "ComputeUnit" in name:
            work = util.create_configuration_item(
                source_uuid=src_uuid,
                target_uuid=uuid,
                type=ConfigurationRequests.ConfigType.SET_WORKER,
                worker_count=workers,
            )
            client.send_message(work)
    time.sleep(1)  # allow config to propagate


# ---------------------------------------------------------------------------
# Instrumented query runner
# ---------------------------------------------------------------------------

def _tracked_run_query(
    client: tcp_client.TCPClient,
    query: msg.TCPMessage,
    query_name: str,
    mode: str,
    scale_factor: int,
    iteration: int,
    workers: int,
    tracker: InFlightTracker,
    # --- Instrumentation context ---
    workload_name: str,
    phase_name: Optional[str],
    user_id: Optional[int],
    sequence_number: int,
    planned_delay_s: Optional[float],
    submit_timestamp: float,
) -> QueryResult:
    """
    Execute a query and return a fully instrumented QueryResult.

    Wraps the core send-and-wait logic and adds in-flight tracking plus
    all instrumentation fields.
    """
    request = WorkRequest.WorkRequest()
    request.ParseFromString(query.payload)

    concurrent_snapshot = tracker.acquire()
    try:
        t_s = datetime.now()
        client.send_message(query)
        success = wait_next_completion(client, request.queryPlan.planid)
        t_e = datetime.now()
    finally:
        tracker.release()

    return QueryResult(
        # Core
        name=query_name,
        start=t_s.timestamp(),
        end=t_e.timestamp(),
        elapsed=(t_e - t_s).total_seconds(),
        success=success,
        mode=mode,
        scale_factor=scale_factor,
        iteration=iteration,
        workers=workers,
        # Instrumentation
        workload_name=workload_name,
        query_flight=extract_query_flight(query_name),
        phase_name=phase_name,
        user_id=user_id,
        sequence_number=sequence_number,
        concurrent_in_flight=concurrent_snapshot,
        planned_delay_s=planned_delay_s,
        submit_timestamp=submit_timestamp,
        window_size=0, 
        max_overhead=0.0
    )


# ---------------------------------------------------------------------------
# Pattern executors
# ---------------------------------------------------------------------------

def _execute_mix(
    client: tcp_client.TCPClient,
    config: WorkloadConfig,
    src_uuid: int,
    tgt_uuid: int,
    iteration: int,
    tracker: InFlightTracker,
    seq: SequenceCounter,
) -> List[QueryResult]:
    """
    Mix pattern: fire total_queries with weighted random selection and
    inter-arrival times governed by the configured arrival model.
    """
    pattern = config.pattern
    arrival = create_arrival_model(pattern.arrival)
    results: List[QueryResult] = []

    print(f"  [mix] Dispatching {pattern.total_queries} queries "
          f"(arrival={arrival}, max_concurrent={pattern.max_concurrent})")

    with ThreadPoolExecutor(max_workers=pattern.max_concurrent) as pool:
        futures = []
        for i in range(pattern.total_queries):
            # Compute delay BEFORE dispatching (so we can record it)
            planned_delay = arrival.next_delay() if i < pattern.total_queries - 1 else None

            query_name = _pick_query(config.query_mix)
            query = _build_query(query_name, src_uuid, tgt_uuid, config.scale_factor)
            submit_ts = datetime.now().timestamp()
            seq_num = seq.next()

            future = pool.submit(
                _tracked_run_query,
                client, query, query_name,
                f"mix/{config.pattern.arrival.model}",
                config.scale_factor, iteration, config.workers,
                tracker,
                workload_name=config.name,
                phase_name=None,
                user_id=None,
                sequence_number=seq_num,
                planned_delay_s=planned_delay,
                submit_timestamp=submit_ts,
            )
            futures.append(future)

            # Inter-arrival delay (don't sleep after the last query)
            if planned_delay is not None:
                time.sleep(planned_delay)

        for fut in as_completed(futures):
            try:
                results.append(fut.result())
            except Exception as e:
                print(f"  [mix] Query failed: {e}")

    return results


def _execute_phases(
    client: tcp_client.TCPClient,
    config: WorkloadConfig,
    src_uuid: int,
    tgt_uuid: int,
    iteration: int,
    tracker: InFlightTracker,
    seq: SequenceCounter,
) -> List[QueryResult]:
    """
    Phases pattern: execute a sequence of timed phases, each with its own
    arrival model and optional query mix override.

    The planned_delay_s recorded per query is the delay computed by the
    arrival model AFTER dispatching that query (i.e., the time the dispatcher
    waits before sending the next one).
    """
    results: List[QueryResult] = []

    with ThreadPoolExecutor(max_workers=config.pattern.max_concurrent) as pool:
        all_futures = []

        for phase in config.pattern.phases:
            query_mix = phase.query_mix or config.query_mix
            arrival = create_arrival_model(phase.arrival)

            if isinstance(arrival, RampingArrival):
                arrival.reset()

            print(f"  [phases/{phase.name}] Starting — duration={phase.duration_s}s, arrival={arrival}")

            phase_start = time.monotonic()
            phase_deadline = phase_start + phase.duration_s
            query_count = 0
            pending_delay = None  # delay to sleep BEFORE dispatching the next query

            while client.connection_up:
                # Sleep the delay from the PREVIOUS dispatch
                if pending_delay is not None:
                    remaining = phase_deadline - time.monotonic()
                    if pending_delay >= remaining:
                        break
                    time.sleep(pending_delay)

                # Check deadline after sleeping
                if time.monotonic() >= phase_deadline:
                    break

                # Compute the delay that will follow THIS query
                next_delay = arrival.next_delay()

                query_name = _pick_query(query_mix)
                query = _build_query(query_name, src_uuid, tgt_uuid, config.scale_factor)
                submit_ts = datetime.now().timestamp()
                seq_num = seq.next()

                future = pool.submit(
                    _tracked_run_query,
                    client, query, query_name,
                    f"phases/{phase.name}",
                    config.scale_factor, iteration, config.workers,
                    tracker,
                    workload_name=config.name,
                    phase_name=phase.name,
                    user_id=None,
                    sequence_number=seq_num,
                    planned_delay_s=next_delay,
                    submit_timestamp=submit_ts,
                )
                all_futures.append(future)
                query_count += 1
                pending_delay = next_delay

            print(f"  [phases/{phase.name}] Done — dispatched {query_count} queries")

        for fut in as_completed(all_futures):
            try:
                results.append(fut.result())
            except Exception as e:
                print(f"  [phases] Query failed: {e}")

    return results


def _execute_virtual_users(
    client: tcp_client.TCPClient,
    config: WorkloadConfig,
    src_uuid: int,
    tgt_uuid: int,
    iteration: int,
    tracker: InFlightTracker,
    seq: SequenceCounter,
) -> List[QueryResult]:
    """
    Virtual users pattern: simulate N analysts, each issuing queries
    sequentially with random think time between completions.
    """
    pattern = config.pattern
    user_results: List[QueryResult] = []
    results_lock = threading.Lock()

    def _user_session(user_id: int):
        """Single virtual user session."""
        local_results = []
        for q_idx in range(pattern.queries_per_user):
            query_name = _pick_query(config.query_mix)
            query = _build_query(query_name, src_uuid, tgt_uuid, config.scale_factor)

            # Think time is the "planned delay" for virtual users
            if q_idx < pattern.queries_per_user - 1:
                think_s = random.uniform(
                    pattern.think_time_min_ms / 1000.0,
                    pattern.think_time_max_ms / 1000.0,
                )
            else:
                think_s = None

            submit_ts = datetime.now().timestamp()
            seq_num = seq.next()

            result = _tracked_run_query(
                client, query, query_name,
                f"virtual_user/{user_id}",
                config.scale_factor, iteration, config.workers,
                tracker,
                workload_name=config.name,
                phase_name=None,
                user_id=user_id,
                sequence_number=seq_num,
                planned_delay_s=think_s,
                submit_timestamp=submit_ts,
            )
            local_results.append(result)

            # Think time (don't sleep after the last query)
            if think_s is not None:
                time.sleep(think_s)

        with results_lock:
            user_results.extend(local_results)

    print(f"  [virtual_users] Starting {pattern.num_users} users, "
          f"{pattern.queries_per_user} queries each, "
          f"think_time=[{pattern.think_time_min_ms}, {pattern.think_time_max_ms}]ms")

    threads = []
    for uid in range(pattern.num_users):
        t = threading.Thread(target=_user_session, args=(uid,), daemon=True)
        threads.append(t)
        t.start()
        # Stagger user starts
        if uid < pattern.num_users - 1:
            time.sleep(pattern.stagger_ms / 1000.0)

    for t in threads:
        t.join()

    print(f"  [virtual_users] All users finished — {len(user_results)} total queries")
    return user_results


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

EXECUTORS = {
    "mix": _execute_mix,
    "phases": _execute_phases,
    "virtual_users": _execute_virtual_users,
}


def execute_workload(
    client: tcp_client.TCPClient,
    config: WorkloadConfig,
) -> List[QueryResult]:
    """
    Execute a full workload (all iterations) according to the given config.

    Returns a flat list of QueryResult across all iterations, with full
    instrumentation fields populated.
    """
    # Discover compute units
    src_uuid, tgt_uuid = _discover_compute_unit(client)
    print(f"[workload] Found ComputeUnit: src={src_uuid}, tgt={tgt_uuid}")

    # Configure worker count
    _configure_workers(client, src_uuid, config.workers)
    print(f"[workload] Set workers={config.workers}")

    # Validate query names
    from utils.workload_config import validate_config
    validate_config(config, AVAILABLE_QUERIES)

    executor = EXECUTORS.get(config.pattern.type)
    if executor is None:
        raise ValueError(f"Unknown pattern type: {config.pattern.type}")

    # Shared instrumentation state
    tracker = InFlightTracker()
    seq = SequenceCounter()

    all_results: List[QueryResult] = []

    for iteration in range(1, config.iterations + 1):
        print(f"\n{'='*60}")
        print(f"[workload] '{config.name}' — iteration {iteration}/{config.iterations}")
        print(f"{'='*60}")

        seq.reset()  # reset per iteration for clean ordering
        results = executor(client, config, src_uuid, tgt_uuid, iteration, tracker, seq)
        all_results.extend(results)
        complete_plans.clear()

        if iteration < config.iterations:
            time.sleep(2)  # cooldown between iterations

    return all_results