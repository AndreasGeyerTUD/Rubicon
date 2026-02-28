"""
Workload configuration: dataclasses + YAML loading.

Supports three pattern types:
    mix            - Weighted random query selection with a single arrival model
    phases         - Time-segmented stages, each with its own arrival model and optional query mix override
    virtual_users  - Simulated analysts issuing queries sequentially with configurable think time
"""

from dataclasses import dataclass, field
from typing import Dict, List, Optional

import yaml


# ---------------------------------------------------------------------------
# Dataclasses
# ---------------------------------------------------------------------------

@dataclass
class ArrivalConfig:
    """
    Arrival time model configuration.

    For constant-rate models, set `rate` (QPS).
    For fixed_jitter, set `interval_ms` and `jitter_ms`.
    For ramping (inside phases), set `start_rate` and `end_rate`.
    """
    model: str  # "poisson" | "fixed_jitter" | "target_qps"
    rate: Optional[float] = None
    interval_ms: Optional[float] = None
    jitter_ms: Optional[float] = None
    variance: Optional[float] = 0.1
    start_rate: Optional[float] = None
    end_rate: Optional[float] = None
    # Attached by the loader when this config belongs to a phase (for RampingArrival)
    duration_s: Optional[float] = None


@dataclass
class PhaseConfig:
    """A single phase in a phased workload."""
    name: str
    duration_s: float
    arrival: ArrivalConfig
    query_mix: Optional[Dict[str, float]] = None  # overrides top-level mix


@dataclass
class PatternConfig:
    """
    Top-level pattern definition.  Exactly one sub-config should be populated
    depending on `type`.
    """
    type: str  # "mix" | "phases" | "virtual_users"

    # --- mix ---
    total_queries: Optional[int] = None
    arrival: Optional[ArrivalConfig] = None
    max_concurrent: int = 32

    # --- phases ---
    phases: Optional[List[PhaseConfig]] = None

    # --- virtual_users ---
    num_users: Optional[int] = None
    queries_per_user: Optional[int] = None
    think_time_min_ms: float = 500.0
    think_time_max_ms: float = 3000.0
    stagger_ms: float = 200.0


@dataclass
class WorkloadConfig:
    """Root configuration for a workload run."""
    name: str
    scale_factor: int
    workers: int
    query_mix: Dict[str, float]
    pattern: PatternConfig
    iterations: int = 1
    output_file: Optional[str] = None


# ---------------------------------------------------------------------------
# YAML parsing helpers
# ---------------------------------------------------------------------------

def _parse_arrival(raw: dict, parent_duration_s: Optional[float] = None) -> ArrivalConfig:
    cfg = ArrivalConfig(
        model=raw["model"],
        rate=raw.get("rate"),
        interval_ms=raw.get("interval_ms"),
        jitter_ms=raw.get("jitter_ms"),
        variance=raw.get("variance", 0.1),
        start_rate=raw.get("start_rate"),
        end_rate=raw.get("end_rate"),
    )
    if parent_duration_s is not None:
        cfg.duration_s = parent_duration_s
    return cfg


def _parse_phase(raw: dict) -> PhaseConfig:
    duration = raw["duration_s"]
    return PhaseConfig(
        name=raw["name"],
        duration_s=duration,
        arrival=_parse_arrival(raw["arrival"], parent_duration_s=duration),
        query_mix=raw.get("query_mix"),
    )


def _parse_pattern(raw: dict) -> PatternConfig:
    ptype = raw["type"]
    cfg = PatternConfig(type=ptype)

    if ptype == "mix":
        cfg.total_queries = raw["total_queries"]
        cfg.arrival = _parse_arrival(raw["arrival"])
        cfg.max_concurrent = raw.get("max_concurrent", 32)

    elif ptype == "phases":
        cfg.phases = [_parse_phase(p) for p in raw["phases"]]
        cfg.max_concurrent = raw.get("max_concurrent", 32)

    elif ptype == "virtual_users":
        cfg.num_users = raw["num_users"]
        cfg.queries_per_user = raw["queries_per_user"]
        cfg.think_time_min_ms = raw.get("think_time_min_ms", 500.0)
        cfg.think_time_max_ms = raw.get("think_time_max_ms", 3000.0)
        cfg.stagger_ms = raw.get("stagger_ms", 200.0)

    else:
        raise ValueError(f"Unknown pattern type: {ptype}")

    return cfg


def _normalize_query_mix(mix: Dict[str, float]) -> Dict[str, float]:
    """Normalize weights so they sum to 1.0."""
    total = sum(mix.values())
    if total <= 0:
        raise ValueError("Query mix weights must sum to > 0")
    return {k: v / total for k, v in mix.items()}


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def load_workload_config(path: str) -> WorkloadConfig:
    """Load and validate a workload configuration from a YAML file."""
    with open(path) as f:
        raw = yaml.safe_load(f)

    query_mix = _normalize_query_mix(raw["query_mix"])

    # Also normalize any per-phase query mixes
    pattern = _parse_pattern(raw["pattern"])
    if pattern.phases:
        for phase in pattern.phases:
            if phase.query_mix:
                phase.query_mix = _normalize_query_mix(phase.query_mix)

    return WorkloadConfig(
        name=raw["name"],
        scale_factor=raw["scale_factor"],
        workers=raw["workers"],
        query_mix=query_mix,
        pattern=pattern,
        iterations=raw.get("iterations", 1),
        output_file=raw.get("output_file"),
    )


def validate_config(cfg: WorkloadConfig, available_queries: List[str]):
    """Validate that all referenced queries exist."""
    errors = []
    for qname in cfg.query_mix:
        if qname not in available_queries:
            errors.append(f"Unknown query in top-level mix: {qname}")
    if cfg.pattern.phases:
        for phase in cfg.pattern.phases:
            if phase.query_mix:
                for qname in phase.query_mix:
                    if qname not in available_queries:
                        errors.append(f"Unknown query in phase '{phase.name}': {qname}")
    if errors:
        raise ValueError("Config validation errors:\n" + "\n".join(f"  - {e}" for e in errors))