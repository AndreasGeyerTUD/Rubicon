#!/usr/bin/env python3
"""
Fire-and-forget load generator.
Sends N queries per iteration without waiting for responses,
sleeps 2 seconds between iterations.
"""
import argparse
import threading
import time
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Tuple

from concurrent.futures import ThreadPoolExecutor

from utils import msg
from utils import client as tcp_client
from utils import util

from utils.unique_id import new_uint32_id

from proto import NetworkRequests_pb2 as NetworkRequests
from proto import ConfigurationRequests_pb2 as ConfigurationRequests
from proto.UnitDefinition_pb2 import TcpPackageType, UnitType

from utils.queries import (
    q_1_1, q_1_2, q_1_3,
    q_2_1, q_2_2, q_2_3,
    q_3_1, q_3_2, q_3_3, q_3_4,
    q_4_1, q_4_2, q_4_3,
)

queries_dict = {
    "q_1_1": q_1_1, "q_1_2": q_1_2, "q_1_3": q_1_3,
    "q_2_1": q_2_1, "q_2_2": q_2_2, "q_2_3": q_2_3,
    "q_3_1": q_3_1, "q_3_2": q_3_2, "q_3_3": q_3_3, "q_3_4": q_3_4,
    "q_4_1": q_4_1, "q_4_2": q_4_2, "q_4_3": q_4_3,
}

# ----------------------------
# Global state
# ----------------------------
config_replies: List[bool] = []
config_condition = threading.Condition()


# ----------------------------
# Callbacks
# ----------------------------

def server_config_response_cb(message: msg.TCPMessage):
    resp = NetworkRequests.ServerConfigurationResponse()
    resp.ParseFromString(message.payload)
    with config_condition:
        config_replies.append(bool(resp.success))
        config_condition.notify_all()


def wait_for_config_reply(client: tcp_client.TCPClient) -> bool:
    with config_condition:
        while len(config_replies) == 0 and client.connection_up:
            config_condition.wait()
        if config_replies:
            return config_replies.pop(0)
    return False

def plan_finished_cb(message: msg.TCPMessage):
    pass # We don't care about plan responses in this experiment, but we need to register a callback to avoid "no callback for package type" warnings and dropped messages.

# ----------------------------
# Helpers
# ----------------------------

def send_server_configuration(
    client: tcp_client.TCPClient,
    *,
    callback_fn,
    callback_pkg_type=TcpPackageType.QUERY_PLAN,
    window_size=None,
    threshold=None,
    maxOverhead=None,
    src_uuid=0,
    tgt_uuid=0,
):
    req = NetworkRequests.ServerConfigurationRequest()

    cb = NetworkRequests.CallbackChangeRequest()
    cb.packageType = int(callback_pkg_type)
    cb.callbackFunction = int(callback_fn)
    req.callbackChange.CopyFrom(cb)

    if window_size is not None:
        req.windowSizeMs = int(window_size)
    if threshold is not None:
        req.threshold = int(threshold)
    if maxOverhead is not None:
        req.maxOverhead = float(maxOverhead)

    msg_out = msg.TCPMessage(
        unit_type=UnitType.QUERY_PLANER,
        package_type=TcpPackageType.SERVER_CONFIGURATION,
        src_uuid=src_uuid,
        tgt_uuid=tgt_uuid,
    )
    msg_out.payload = req.SerializeToString()
    client.send_message(msg_out)

    ok = wait_for_config_reply(client)
    if not ok:
        print("[Warn] ServerConfigurationResponse.success == False or no reply.")


def prebuild_messages(
    query_fn,
    count: int,
    src_uuid: int,
    tgt_uuid: int,
    scale_factor: int,
) -> List[msg.TCPMessage]:
    """Build all query messages upfront so send threads do zero computation."""
    return [
        query_fn(
            planId=new_uint32_id(),
            src_uuid=src_uuid,
            target_uuid=tgt_uuid,
            scale_factor=scale_factor,
            extendedResult=False,
        )
        for _ in range(count)
    ]


# ----------------------------
# Fire-and-forget send
# ----------------------------
def fire_batch(
    client: tcp_client.TCPClient,
    messages: List[msg.TCPMessage],
):
    """Send pre-built messages as fast as possible. Threads only call send_message."""
    n = len(messages)
    with ThreadPoolExecutor(max_workers=min(n, 128)) as pool:
        for m in messages:
            pool.submit(client.send_message, m)


# ----------------------------
# Main experiment
# ----------------------------
def run_experiment(
    client: tcp_client.TCPClient,
    query: str,
    *,
    workers_per_cu: int,
    n_values: List[int],
    threshold: int,
    window_size: int,
    iterations: int,
    scale_factor: int,
    pause: float,
):
    src_uuid = client.uuid

    # Configure: QUERY_PLAN_CB with the given window + threshold
    print(f"\n--- Configuring: QUERY_PLAN_CB, window={window_size}, threshold={threshold} ---")
    send_server_configuration(
        client,
        callback_fn=NetworkRequests.CallbackFunction.QUERY_PLAN_CB,
        callback_pkg_type=TcpPackageType.QUERY_PLAN,
        window_size=window_size,
        threshold=threshold,
        maxOverhead=2.0,
        src_uuid=src_uuid,
        tgt_uuid=0,
    )
    time.sleep(1.0)

    query_fn = queries_dict[query]

    for n in n_values:
        # Pre-build all messages for every iteration of this n-value
        all_batches = [
            prebuild_messages(query_fn, n, src_uuid, 0, scale_factor)
            for _ in range(iterations)
        ]
        print(f"  [{query}] pre-built {n} x {iterations} = {n * iterations} messages")

        for i, batch in enumerate(all_batches):
            t0 = time.perf_counter()
            fire_batch(client, batch)
            send_elapsed = time.perf_counter() - t0
            print(f"  [{query}] n={n:>4d}  iter={i+1}/{iterations} sent in {send_elapsed:.3f}s  — sleeping {pause}s"
            )
            time.sleep(pause)

    print(f"[Done] {query}")


# ----------------------------
# CLI
# ----------------------------
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Fire-and-forget query load generator")
    parser.add_argument("-ip", default="127.0.0.1")
    parser.add_argument("-port", type=int, default=23232)
    parser.add_argument("-info", default="Load generator")
    parser.add_argument("-name", default="Load generator client")
    parser.add_argument("-q", help="Which quer(ies) to run (CSV). Default: all.", default=None)
    parser.add_argument("--iterations", type=int, default=10)
    parser.add_argument("--scale-factor", type=int, default=100)
    parser.add_argument("--workers-per-cu", type=int, default=48)
    parser.add_argument("--n-values", default="10,25,50,100,150,200,300,400,500,650,800,1000", help="Concurrency sweep (CSV)")
    parser.add_argument("--threshold", type=int, default=10)
    parser.add_argument("--window-size", type=int, default=1000)
    parser.add_argument("--pause", type=float, default=2.0, help="Seconds between iterations")

    args = parser.parse_args()

    n_values = [int(x.strip()) for x in args.n_values.split(",") if x.strip()]

    if args.q:
        query_list = [q.strip() for q in args.q.split(",")]
    else:
        query_list = list(queries_dict.keys())

    client = tcp_client.TCPClient(unit_type=UnitType.QUERY_PLANER, unit_info=args.info, name=args.name)

    # Register callback (plan responses are not registered — silently dropped)
    client.register_callback(TcpPackageType.SERVER_CONFIGURATION_RESPONSE, server_config_response_cb)
    client.register_callback(TcpPackageType.PLAN_RESPONSE, plan_finished_cb)

    client.connect(args.ip, args.port)

    try:
        for q in query_list:
            run_experiment(
                client,
                q,
                workers_per_cu=args.workers_per_cu,
                n_values=n_values,
                threshold=args.threshold,
                window_size=args.window_size,
                iterations=args.iterations,
                scale_factor=args.scale_factor,
                pause=args.pause,
            )
    finally:
        client.disconnect()