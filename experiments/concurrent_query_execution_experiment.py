#!/usr/bin/env python3
import argparse
import threading
import time
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import humanize
import pandas as pd
import matplotlib.pyplot as plt

from concurrent.futures import ThreadPoolExecutor, as_completed

from utils import msg
from utils import client as tcp_client
from utils import util

from utils.unique_id import new_uint32_id

from proto import NetworkRequests_pb2 as NetworkRequests
from proto import WorkRequest_pb2 as WorkRequest
from proto import WorkResponse_pb2 as WorkResponse
from proto import QueryPlan_pb2 as QueryPlan
from proto import ConfigurationRequests_pb2 as ConfigurationRequests
from proto import NetworkRequests_pb2 as NetworkRequests
from proto.UnitDefinition_pb2 import TcpPackageType, UnitType

from utils.queries import q_1_1, q_1_2, q_1_3, q_2_1, q_2_2, q_2_3, q_3_1, q_3_2, q_3_3, q_3_4, q_4_1, q_4_2, q_4_3


queries_dict = {"q_1_1": q_1_1, "q_1_2": q_1_2, "q_1_3": q_1_3, "q_2_1": q_2_1, "q_2_2": q_2_2, "q_2_3": q_2_3, "q_3_1": q_3_1, "q_3_2": q_3_2, "q_3_3": q_3_3, "q_3_4": q_3_4, "q_4_1": q_4_1, "q_4_2": q_4_2, "q_4_3": q_4_3}

# ----------------------------
# Global state (mirrors your script style)
# ----------------------------
name_list: List[str] = []
uuid_list: List[int] = []

uuid_condition = threading.Condition()

complete_plans: Dict[int, bool] = {}
complete_condition = threading.Condition()

config_replies: List[bool] = []
config_condition = threading.Condition()


# ----------------------------
# Helpers: callbacks
# ----------------------------
def update_uuids(message: msg.TCPMessage):
    global name_list, uuid_list, uuid_condition
    response = NetworkRequests.UuidForUnitResponse()
    response.ParseFromString(message.payload)

    with uuid_condition:
        name_list.clear()
        uuid_list.clear()
        name_list.extend(list(response.names))
        uuid_list.extend(list(response.uuids))
        uuid_condition.notify_all()
        

def plan_finished_cb(message: msg.TCPMessage):
    global complete_condition, complete_plans
    response: WorkResponse.PlanResponse = WorkResponse.PlanResponse()
    response.ParseFromString(message.payload)
    # print(f"[DEBUG] plan_finished_cb: planId={response.planId}")

    with complete_condition:
        complete_plans[response.planId] = response.success
        complete_condition.notify_all()


def server_config_response_cb(message: msg.TCPMessage):
    """Optional: wait/confirm config changes were applied."""
    resp = NetworkRequests.ServerConfigurationResponse()
    resp.ParseFromString(message.payload)

    with config_condition:
        config_replies.append(bool(resp.success))
        config_condition.notify_all()


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


def wait_for_config_reply(client: tcp_client.TCPClient) -> Optional[bool]:
    """Returns success True/False if response comes, else None."""
    with config_condition:
        while len(config_replies) == 0 and client.connection_up:
            config_condition.wait()

        if len(config_replies) > 0:
            return config_replies.pop(0)
    return None


# ----------------------------
# Message construction (adjust here if needed)
# ----------------------------
def send_server_configuration(
    client: tcp_client.TCPClient,
    callback_fn: Optional[int] = None,
    callback_pkg_type: Optional[TcpPackageType] = None,
    window_size: Optional[int] = None,
    threshold: Optional[int] = None,
    maxOverhead: Optional[float] = None,
    wait_ack: bool = True,
    src_uuid: int = 0,
    tgt_uuid: int = 0
):
    """
    Builds and sends NetworkRequests.ServerConfigurationRequest.
    - callback_fn uses NetworkRequests.CallbackFunction enum values.
    - callback_pkg_type is the package type you want to change callback for (QUERY_PLAN).
    """
    req = NetworkRequests.ServerConfigurationRequest()

    if callback_fn is not None:
        cb = NetworkRequests.CallbackChangeRequest()
        cb.packageType = int(callback_pkg_type) if callback_pkg_type is not None else int(TcpPackageType.QUERY_PLAN)
        cb.callbackFunction = int(callback_fn)
        req.callbackChange.CopyFrom(cb)

    if window_size is not None:
        req.windowSizeMs = int(window_size)

    if threshold is not None:
        req.threshold = int(threshold)
        
    if maxOverhead is not None:
        req.maxOverhead = float(maxOverhead)

    msg_out = msg.TCPMessage(unit_type=UnitType.QUERY_PLANER, package_type=TcpPackageType.SERVER_CONFIGURATION, src_uuid=src_uuid, tgt_uuid=tgt_uuid)
    msg_out.payload = req.SerializeToString()
    client.send_message(msg_out)

    if wait_ack:
        ok = wait_for_config_reply(client)
        if ok is None:
            print("[Warn] No ServerConfigurationResponse received (continuing).")
        elif not ok:
            print("[Warn] ServerConfigurationResponse.success == False.")


# ----------------------------
# Experiment logic
# ----------------------------
@dataclass
class PerQueryRow:
    query:str
    iteration: int
    phase: str                   # "forward" or "query_plan"
    n_concurrent: int
    threshold: int
    window_size: float
    query_idx: int
    plan_id: int
    start_ts: float
    end_ts: float
    elapsed_s: float
    success: bool


@dataclass
class PerBatchRow:
    query:str
    iteration: int
    phase: str
    n_concurrent: int
    threshold: int
    window_size: float
    batch_start_ts: float
    batch_end_ts: float
    batch_elapsed_s: float
    successes: int
    failures: int


def discover_compute_unit(client: tcp_client.TCPClient) -> Tuple[int, int]:
    """Returns (src_uuid, tgt_compute_uuid)."""
    # Ask for compute units
    work = util.create_uuid_request_item(type=UnitType.COMPUTE_UNIT)
    client.send_message(work)

    with uuid_condition:
        if len(uuid_list) == 0 and client.connection_up:
            uuid_condition.wait(timeout=5.0)

    tgt_uuid = 0
    for name, uuid in zip(name_list, uuid_list):
        if "ComputeUnit" in name:
            tgt_uuid = uuid
            break

    if tgt_uuid == 0:
        raise RuntimeError("Could not find a ComputeUnit.")

    return client.uuid, tgt_uuid


def set_workers_for_compute_units(client: tcp_client.TCPClient, workers: int):
    """Same as in your script, but extracted."""
    src_uuid = client.uuid
    for name, uuid in zip(name_list, uuid_list):
        if "ComputeUnit" not in name:
            continue
        work = util.create_configuration_item(
            source_uuid=src_uuid,
            target_uuid=uuid,
            type=ConfigurationRequests.ConfigType.SET_WORKER,
            worker_count=workers,
        )
        client.send_message(work)


def run_one_query(client: tcp_client.TCPClient, query_msg: msg.TCPMessage, query_idx: int) -> Tuple[int, bool, float, float, float]:
    """Returns (plan_id, success, start_ts, end_ts, elapsed_s)."""
    req = WorkRequest.WorkRequest()
    req.ParseFromString(query_msg.payload)
    plan_id = req.queryPlan.planid

    t_s = datetime.now()
    client.send_message(query_msg)
    success = wait_next_completion(client, plan_id)
    t_e = datetime.now()

    return plan_id, success, t_s.timestamp(), t_e.timestamp(), (t_e - t_s).total_seconds()


def execute_concurrent_batch(
    *,
    query: str,
    client: tcp_client.TCPClient,
    src_uuid: int,
    tgt_uuid: int,
    scale_factor: int,
    n_concurrent: int,
    phase: str,
    window_size: float,
    threshold: int,
    iteration: int = 0
) -> Tuple[List[PerQueryRow], PerBatchRow]:
    """
    Fires the SAME query n_concurrent times concurrently with fresh plan IDs.
    """
    # Fresh plan IDs/messages
    query_msgs: List[msg.TCPMessage] = []
    for i in range(n_concurrent):
        pid = new_uint32_id()
        q = queries_dict[query]
        query_msgs.append(q(planId=pid, src_uuid=src_uuid, target_uuid=tgt_uuid, scale_factor=scale_factor, extendedResult=False))

    batch_start = datetime.now().timestamp()

    per_query_rows: List[PerQueryRow] = []
    successes = 0
    failures = 0

    # Run concurrently
    with ThreadPoolExecutor(max_workers=n_concurrent) as pool:
        futures = [
            pool.submit(run_one_query, client, qm, idx)
            for idx, qm in enumerate(query_msgs)
        ]
        for fut in as_completed(futures):
            plan_id, success, s_ts, e_ts, elapsed = fut.result()
            if success:
                successes += 1
            else:
                failures += 1
            per_query_rows.append(
                PerQueryRow(
                    query=query,
                    iteration=iteration,
                    phase=phase,
                    n_concurrent=n_concurrent,
                    threshold=threshold,
                    window_size=window_size,
                    query_idx=len(per_query_rows),
                    plan_id=plan_id,
                    start_ts=s_ts,
                    end_ts=e_ts,
                    elapsed_s=elapsed,
                    success=success
                )
            )

    batch_end = datetime.now().timestamp()
    batch_elapsed = batch_end - batch_start

    # Reset completion tracking for the next batch
    with complete_condition:
        complete_plans.clear()

    per_batch_row = PerBatchRow(
        query=query,
        iteration=iteration,
        phase=phase,
        n_concurrent=n_concurrent,
        threshold=threshold,
        window_size=window_size,
        batch_start_ts=batch_start,
        batch_end_ts=batch_end,
        batch_elapsed_s=batch_elapsed,
        successes=successes,
        failures=failures,
    )
    return per_query_rows, per_batch_row


def to_per_query_df(rows: List[PerQueryRow]) -> pd.DataFrame:
    return pd.DataFrame([r.__dict__ for r in rows])


def to_per_batch_df(rows: List[PerBatchRow]) -> pd.DataFrame:
    return pd.DataFrame([r.__dict__ for r in rows])


# ----------------------------
# Summary printing helpers
# ----------------------------
def print_iteration_min(per_batch_rows: List[PerBatchRow], phase: str, n: int, threshold: int, window_size: float, query: str):
    """Print the min batch time across iterations for a given (phase, n, threshold) combo."""
    matching = [r for r in per_batch_rows if r.phase == phase and r.n_concurrent == n and r.threshold == threshold]
    if not matching:
        return

    min_elapsed = min(r.batch_elapsed_s for r in matching)
    total_ok = sum(r.successes for r in matching)
    total_fail = sum(r.failures for r in matching)
    n_iters = len(matching)

    thr_str = f", threshold={threshold}" if phase == "query_plan" else ""
    win_str = f", window={window_size}" if phase == "query_plan" else ""
    print(
        f"  [{phase}] {query}, n={n}{thr_str}{win_str}, "
        f"min over {n_iters} iterations: "
        f"{humanize.precisedelta(min_elapsed, minimum_unit='milliseconds', format='%0.000f')} "
        f"(total ok={total_ok}, fail={total_fail})"
    )


def build_comparison_summary(per_batch_df: pd.DataFrame) -> Optional[str]:
    """
    Build a formatted table comparing FORWARD_CB (baseline) against each threshold
    in QUERY_PLAN_CB.  Returns the full string, or None if data is missing.
    """
    df_fwd = per_batch_df[per_batch_df["phase"] == "forward"]
    df_qp = per_batch_df[per_batch_df["phase"] == "query_plan"]

    if df_fwd.empty or df_qp.empty:
        return None

    # Mean + std batch time per (phase, n_concurrent, threshold)
    fwd_means = df_fwd.groupby("n_concurrent")["batch_elapsed_s"].mean()
    fwd_stds = df_fwd.groupby("n_concurrent")["batch_elapsed_s"].std()
    fwd_counts = df_fwd.groupby("n_concurrent")["batch_elapsed_s"].count()
    qp_means = df_qp.groupby(["n_concurrent", "threshold"])["batch_elapsed_s"].mean()
    qp_stds = df_qp.groupby(["n_concurrent", "threshold"])["batch_elapsed_s"].std()

    thresholds = sorted(df_qp["threshold"].unique())
    n_values = sorted(set(fwd_means.index) | set(qp_means.index.get_level_values("n_concurrent")))
    n_iters = int(fwd_counts.iloc[0]) if len(fwd_counts) > 0 else "?"

    lines: List[str] = []

    # Header
    thr_headers = [f"thr={t}" for t in thresholds]
    speedup_headers = [f"Δ% thr={t}" for t in thresholds]
    col_headers = ["n", "FORWARD (mean±std)"] + [h for pair in zip(thr_headers, speedup_headers) for h in pair]
    header_line = " | ".join(f"{h:>18s}" for h in col_headers)
    sep = "=" * len(header_line)

    lines.append(sep)
    lines.append("COMPARISON: FORWARD_CB (baseline) vs QUERY_PLAN_CB per threshold")
    lines.append(f"  Mean ± std batch elapsed time (s) across {n_iters} iterations")
    lines.append(f"  Δ% = (threshold − forward) / forward × 100")
    lines.append(sep)
    lines.append(header_line)
    lines.append("-" * len(header_line))

    for n in n_values:
        fwd_val = fwd_means.get(n)
        fwd_sd = fwd_stds.get(n, 0.0)
        cols: List[str] = [f"{n:>18d}"]

        if fwd_val is not None:
            cols.append(f"{fwd_val:>11.4f}±{fwd_sd:<5.4f}")
        else:
            cols.append(f"{'N/A':>18s}")

        for thr in thresholds:
            qp_val = qp_means.get((n, thr))
            qp_sd = qp_stds.get((n, thr), 0.0)
            if qp_val is not None:
                cols.append(f"{qp_val:>11.4f}±{qp_sd:<5.4f}")
                if fwd_val is not None and fwd_val > 0:
                    delta_pct = (qp_val - fwd_val) / fwd_val * 100
                    sign = "+" if delta_pct >= 0 else ""
                    cols.append(f"{sign}{delta_pct:>15.1f}%")
                else:
                    cols.append(f"{'N/A':>18s}")
            else:
                cols.append(f"{'N/A':>18s}")
                cols.append(f"{'N/A':>18s}")

        lines.append(" | ".join(cols))

    lines.append(sep)
    lines.append("  Negative Δ% = QUERY_PLAN_CB is faster than FORWARD_CB")
    lines.append("")

    return "\n".join(lines)


def print_and_save_comparison_summary(per_batch_df: pd.DataFrame, out_dir: Path):
    """Print the comparison summary to stdout and write it to a text file."""
    summary = build_comparison_summary(per_batch_df)
    if summary is None:
        print("\n[Skip] Cannot compare: missing forward or query_plan data.")
        return

    # Print to console
    print("\n" + summary)

    # Write to file
    summary_path = out_dir / "comparison_summary.txt"
    summary_path.write_text(summary, encoding="utf-8")
    print(f"[OK] Wrote {summary_path}")


def plot_results(per_query_df: pd.DataFrame, per_batch_df: pd.DataFrame, out_dir: Path):
    out_dir.mkdir(parents=True, exist_ok=True)

    df_fwd = per_batch_df[per_batch_df["phase"] == "forward"].copy()
    df_qp = per_batch_df[per_batch_df["phase"] == "query_plan"].copy()

    # Compute mean batch time across iterations
    fwd_mean = df_fwd.groupby("n_concurrent")["batch_elapsed_s"].mean().reset_index().sort_values("n_concurrent") if len(df_fwd) > 0 else pd.DataFrame()
    qp_mean = df_qp.groupby(["n_concurrent", "threshold"])["batch_elapsed_s"].mean().reset_index() if len(df_qp) > 0 else pd.DataFrame()

    # Also compute std for error bars
    fwd_std = df_fwd.groupby("n_concurrent")["batch_elapsed_s"].std().reset_index().sort_values("n_concurrent") if len(df_fwd) > 0 else pd.DataFrame()
    qp_std = df_qp.groupby(["n_concurrent", "threshold"])["batch_elapsed_s"].std().reset_index() if len(df_qp) > 0 else pd.DataFrame()

    # ------------------------------------------------------------------
    # 1) Mean batch time vs concurrency: FORWARD + all thresholds
    # ------------------------------------------------------------------
    if len(fwd_mean) > 0 or len(qp_mean) > 0:
        plt.figure(figsize=(10, 6))

        if len(fwd_mean) > 0:
            yerr = fwd_std["batch_elapsed_s"].values if len(fwd_std) > 0 else None
            plt.errorbar(
                fwd_mean["n_concurrent"], fwd_mean["batch_elapsed_s"],
                yerr=yerr, marker="s", linewidth=2, capsize=4,
                label="FORWARD_CB", color="black", linestyle="--"
            )

        if len(qp_mean) > 0:
            for thr in sorted(qp_mean["threshold"].unique()):
                sub = qp_mean[qp_mean["threshold"] == thr].sort_values("n_concurrent")
                sub_std = qp_std[qp_std["threshold"] == thr].sort_values("n_concurrent")
                yerr = sub_std["batch_elapsed_s"].values if len(sub_std) > 0 else None
                plt.errorbar(
                    sub["n_concurrent"], sub["batch_elapsed_s"],
                    yerr=yerr, marker="o", capsize=4,
                    label=f"QUERY_PLAN_CB (thr={thr})"
                )

        plt.xlabel("Concurrent queries (n)")
        plt.ylabel("Mean batch elapsed time (s)")
        plt.title("Mean batch time vs concurrency: FORWARD_CB vs QUERY_PLAN_CB thresholds")
        plt.legend()
        plt.grid(True, alpha=0.3)
        plt.tight_layout()
        plt.savefig(out_dir / "mean_batch_time_forward_vs_all_thresholds.png", dpi=160)

    # ------------------------------------------------------------------
    # 2) Speedup (Δ%) vs concurrency for each threshold relative to FORWARD
    # ------------------------------------------------------------------
    if len(fwd_mean) > 0 and len(qp_mean) > 0:
        fwd_lookup = fwd_mean.set_index("n_concurrent")["batch_elapsed_s"]

        plt.figure(figsize=(10, 6))
        for thr in sorted(qp_mean["threshold"].unique()):
            sub = qp_mean[qp_mean["threshold"] == thr].sort_values("n_concurrent")
            delta_pct = []
            ns = []
            for _, row in sub.iterrows():
                n = row["n_concurrent"]
                if n in fwd_lookup.index and fwd_lookup[n] > 0:
                    ns.append(n)
                    delta_pct.append((row["batch_elapsed_s"] - fwd_lookup[n]) / fwd_lookup[n] * 100)
            plt.plot(ns, delta_pct, marker="o", label=f"threshold={thr}")

        plt.axhline(0, color="black", linewidth=1, linestyle="--", label="FORWARD_CB baseline")
        plt.xlabel("Concurrent queries (n)")
        plt.ylabel("Δ% vs FORWARD_CB")
        plt.title("Relative change: QUERY_PLAN_CB vs FORWARD_CB (negative = faster)")
        plt.legend()
        plt.grid(True, alpha=0.3)
        plt.tight_layout()
        plt.savefig(out_dir / "delta_pct_vs_forward.png", dpi=160)

    # ------------------------------------------------------------------
    # 3) Per-query latency boxplot at largest n: FORWARD + each threshold
    # ------------------------------------------------------------------
    if len(per_query_df) > 0:
        n_star = int(per_query_df["n_concurrent"].max())
        sub_all = per_query_df[per_query_df["n_concurrent"] == n_star].copy()

        labels: List[str] = []
        data: List = []

        # Forward first
        sub_fwd = sub_all[sub_all["phase"] == "forward"]
        if len(sub_fwd) > 0:
            labels.append("FORWARD")
            data.append(sub_fwd["elapsed_s"].values)

        # Then each threshold
        sub_qp = sub_all[sub_all["phase"] == "query_plan"]
        if len(sub_qp) > 0:
            for thr in sorted(sub_qp["threshold"].unique()):
                chunk = sub_qp[sub_qp["threshold"] == thr]
                labels.append(f"thr={thr}")
                data.append(chunk["elapsed_s"].values)

        if data:
            plt.figure(figsize=(10, 6))
            plt.boxplot(data, tick_labels=labels, showfliers=False)
            plt.xlabel("Configuration")
            plt.ylabel("Per-query elapsed time (s)")
            plt.title(f"Per-query latency distribution at n={n_star}")
            plt.grid(True, axis="y", alpha=0.3)
            plt.tight_layout()
            plt.savefig(out_dir / "per_query_latency_boxplot_nmax.png", dpi=160)

    plt.close("all")
    print(f"[OK] Plots written to: {out_dir}")


def experiment(client: tcp_client.TCPClient, query: str, workers_per_cu: int, out_dir: Path, n_values: List[int], thresholds: List[int], iterations: int, scale_factor: int, window_size: int):
    out_dir = out_dir / query
    out_dir.mkdir(parents=True, exist_ok=True)
    
    # Discover CU + set workers
    src_uuid, tgt_uuid = discover_compute_unit(client)
    set_workers_for_compute_units(client, workers_per_cu)
    time.sleep(1.0)
    
    per_query_rows: List[PerQueryRow] = []
    per_batch_rows: List[PerBatchRow] = []

    # ----------------------------
    # Phase 1: set callback FORWARD_CB for QUERY_PLAN
    # ----------------------------
    print("=== Phase 1: FORWARD_CB (QUERY_PLAN) ===")
    send_server_configuration(client, callback_fn=NetworkRequests.CallbackFunction.FORWARD_CB, callback_pkg_type=TcpPackageType.QUERY_PLAN, wait_ack=True, src_uuid=src_uuid, tgt_uuid=tgt_uuid)
    time.sleep(1)

    for n in n_values:
        for i in range(0, iterations):
            pq, pb = execute_concurrent_batch(query=query, client=client, src_uuid=src_uuid, tgt_uuid=tgt_uuid, scale_factor=scale_factor, n_concurrent=n, phase="forward", window_size=0.0, threshold=0, iteration=i)
            per_query_rows.extend(pq)
            per_batch_rows.append(pb)
            time.sleep(3)

        # Print mean after all iterations for this n
        print_iteration_min(per_batch_rows, phase="forward", n=n, threshold=0, window_size=0.0, query=query)

    # ----------------------------
    # Phase 2+: QUERY_PLAN_CB with window + thresholds
    # ----------------------------
    print("\n=== Phase 2: QUERY_PLAN_CB (QUERY_PLAN), windowSize + threshold sweep ===")

    for thr in thresholds:
        # update only threshold (and keep windowSize)
        send_server_configuration(client, callback_fn=NetworkRequests.CallbackFunction.QUERY_PLAN_CB, callback_pkg_type=TcpPackageType.QUERY_PLAN, window_size=window_size, maxOverhead=2.0, threshold=thr, wait_ack=True, src_uuid=src_uuid, tgt_uuid=tgt_uuid)
        time.sleep(1)

        for n in n_values:
            for i in range(0, iterations):
                pq, pb = execute_concurrent_batch(query=query, client=client, src_uuid=src_uuid, tgt_uuid=tgt_uuid, scale_factor=scale_factor, n_concurrent=n, phase="query_plan", window_size=window_size, threshold=thr, iteration=i)
                per_query_rows.extend(pq)
                per_batch_rows.append(pb)
                time.sleep(3)

            # Print mean after all iterations for this (n, threshold)
            print_iteration_min(per_batch_rows, phase="query_plan", n=n, threshold=thr, window_size=args.window_size, query=query)

    # ----------------------------
    # Save + Plot
    # ----------------------------
    per_query_df = to_per_query_df(per_query_rows)
    per_batch_df = to_per_batch_df(per_batch_rows)

    per_query_csv = out_dir / "per_query_latencies.csv"
    per_batch_csv = out_dir / "per_batch_times.csv"
    per_query_df.to_csv(per_query_csv, index=False)
    per_batch_df.to_csv(per_batch_csv, index=False)

    print(f"\n[OK] Wrote {per_query_csv}")
    print(f"[OK] Wrote {per_batch_csv}")

    # ----------------------------
    # Comparison summary: FORWARD vs each threshold
    # ----------------------------
    print_and_save_comparison_summary(per_batch_df, out_dir)

    plot_results(per_query_df, per_batch_df, out_dir)

    # Show the plots interactively (if you run locally)
    # plt.show()



# ----------------------------
# Main
# ----------------------------
if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-ip", default="127.0.0.1")
    parser.add_argument("-port", type=int, default=23232)
    parser.add_argument("-info", default="Threshold experiment")
    parser.add_argument("-name", default="Threshold experiment client")
    parser.add_argument('-q', help='Which quer[ies] to run. If multiple queries are given, write as CSV.', default=False, required=False)
    parser.add_argument("--iterations", type=int, default=10)
    parser.add_argument("--scale-factor", type=int, default=100)
    parser.add_argument("--workers-per-cu", type=int, default=48)
    parser.add_argument("--n-values", default="5,10,15,20,25,30,40,50,60,80,100")    # Concurrency sweep
    parser.add_argument("--thresholds", default="10")    # Threshold sweep (applied in QUERY_PLAN_CB phase)
    parser.add_argument("--window-size", type=float, default=100)   # Window size for grouping
    parser.add_argument("--out", default="results/threshold_experiment")    # Output

    args = parser.parse_args()
    
    start_time = datetime.now().strftime('%Y_%m_%d-%H_%M_%S')

    n_values = [int(x.strip()) for x in args.n_values.split(",") if x.strip()]
    thresholds = [int(x.strip()) for x in args.thresholds.split(",") if x.strip()]
    
    query_list = []
    if args.q:
        qs = args.q.split(",")
        query_list.extend([query.strip() for query in qs])
    else:
        query_list = queries_dict.keys()

    out_dir = Path(args.out)
    out_dir = out_dir / start_time

    client = tcp_client.TCPClient(unit_type=UnitType.QUERY_PLANER, unit_info=args.info, name=args.name)

    # Register callbacks
    client.register_callback(TcpPackageType.UUID_FOR_UNIT_RESPONSE, update_uuids)
    client.register_callback(TcpPackageType.SERVER_CONFIGURATION_RESPONSE, server_config_response_cb)
    client.register_callback(TcpPackageType.PLAN_RESPONSE, plan_finished_cb)

    # Connect
    client.connect(args.ip, args.port)

    try:
        for q in query_list:
            experiment(client=client, query=q, workers_per_cu=args.workers_per_cu, out_dir=out_dir, n_values=n_values, thresholds=thresholds, iterations=args.iterations, scale_factor=args.scale_factor, window_size=args.window_size)

    finally:
        client.disconnect()