"""
Arrival time models for OLAP workload simulation.

Each model implements next_delay() -> float, returning the number of seconds
to wait before dispatching the next query.

Models:
    PoissonArrival       - Exponentially distributed inter-arrival times (memoryless, naturally bursty)
    FixedJitterArrival   - Constant interval ± uniform jitter (predictable, good for controlled experiments)
    TargetQPSArrival     - Fixed QPS with configurable variance (intuitive throughput-based parameterization)
    RampingArrival       - Linearly interpolates rate over a duration (used inside phases for ramp-up/cooldown)
"""

import random
import time


class ArrivalModel:
    """Base class for arrival time models."""

    def next_delay(self) -> float:
        """Return the next inter-arrival delay in seconds."""
        raise NotImplementedError


class PoissonArrival(ArrivalModel):
    """
    Poisson process: inter-arrival times are exponentially distributed.

    Args:
        rate: Mean queries per second (λ).
    """

    def __init__(self, rate: float):
        if rate <= 0:
            raise ValueError(f"Poisson rate must be > 0, got {rate}")
        self.rate = rate

    def next_delay(self) -> float:
        return random.expovariate(self.rate)

    def __repr__(self):
        return f"PoissonArrival(rate={self.rate})"


class FixedJitterArrival(ArrivalModel):
    """
    Fixed interval with uniform jitter.

    Delay = interval_ms ± jitter_ms (clamped to >= 0).

    Args:
        interval_ms: Base interval between queries in milliseconds.
        jitter_ms:   Maximum deviation from the base interval in milliseconds.
    """

    def __init__(self, interval_ms: float, jitter_ms: float = 0.0):
        if interval_ms <= 0:
            raise ValueError(f"Interval must be > 0, got {interval_ms}")
        if jitter_ms < 0:
            raise ValueError(f"Jitter must be >= 0, got {jitter_ms}")
        self.interval_s = interval_ms / 1000.0
        self.jitter_s = jitter_ms / 1000.0

    def next_delay(self) -> float:
        return max(0.0, self.interval_s + random.uniform(-self.jitter_s, self.jitter_s))

    def __repr__(self):
        return f"FixedJitterArrival(interval={self.interval_s*1000}ms, jitter={self.jitter_s*1000}ms)"


class TargetQPSArrival(ArrivalModel):
    """
    Target queries-per-second with variance.

    Delay = (1/rate) * uniform(1 - variance, 1 + variance).

    Args:
        rate:     Target queries per second.
        variance: Fractional variance around the mean interval (default 0.1 = ±10%).
    """

    def __init__(self, rate: float, variance: float = 0.1):
        if rate <= 0:
            raise ValueError(f"Rate must be > 0, got {rate}")
        if not 0 <= variance < 1:
            raise ValueError(f"Variance must be in [0, 1), got {variance}")
        self.interval_s = 1.0 / rate
        self.variance = variance

    def next_delay(self) -> float:
        factor = random.uniform(1.0 - self.variance, 1.0 + self.variance)
        return max(0.0, self.interval_s * factor)

    def __repr__(self):
        return f"TargetQPSArrival(rate={1.0/self.interval_s}, variance={self.variance})"


class RampingArrival(ArrivalModel):
    """
    Linearly ramps between start_rate and end_rate over duration_s.

    Uses an underlying arrival model (Poisson or TargetQPS) whose rate is
    continuously adjusted based on elapsed time.

    Args:
        start_rate:  Initial QPS at the beginning of the ramp.
        end_rate:    Final QPS at the end of the ramp.
        duration_s:  Duration over which to ramp (seconds).
        base_model:  Underlying model type: 'poisson' or 'target_qps'.
        variance:    Variance for target_qps base model (ignored for poisson).
    """

    def __init__(self, start_rate: float, end_rate: float, duration_s: float,
                 base_model: str = "poisson", variance: float = 0.1):
        self.start_rate = start_rate
        self.end_rate = end_rate
        self.duration_s = duration_s
        self.base_model = base_model
        self.variance = variance
        self._start_time = None

    def reset(self):
        """Reset the ramp timer. Called when the phase begins execution."""
        self._start_time = time.monotonic()

    def _current_rate(self) -> float:
        if self._start_time is None:
            self._start_time = time.monotonic()
        elapsed = time.monotonic() - self._start_time
        progress = min(elapsed / self.duration_s, 1.0)
        return self.start_rate + (self.end_rate - self.start_rate) * progress

    def next_delay(self) -> float:
        rate = self._current_rate()
        if rate <= 0:
            return 1.0  # fallback: 1 QPS

        if self.base_model == "poisson":
            return random.expovariate(rate)
        else:  # target_qps
            interval = 1.0 / rate
            factor = random.uniform(1.0 - self.variance, 1.0 + self.variance)
            return max(0.0, interval * factor)

    def __repr__(self):
        return (f"RampingArrival(start={self.start_rate}, end={self.end_rate}, "
                f"duration={self.duration_s}s, base={self.base_model})")


def create_arrival_model(config) -> ArrivalModel:
    """
    Factory: build an ArrivalModel from an ArrivalConfig dataclass.

    Handles both constant-rate and ramping (start_rate/end_rate) configurations.
    """
    # Ramping: start_rate and end_rate are set and differ
    if config.start_rate is not None and config.end_rate is not None:
        return RampingArrival(
            start_rate=config.start_rate,
            end_rate=config.end_rate,
            duration_s=getattr(config, "duration_s", 60.0),
            base_model=config.model,
            variance=config.variance or 0.1,
        )

    if config.model == "poisson":
        return PoissonArrival(rate=config.rate)
    elif config.model == "fixed_jitter":
        return FixedJitterArrival(
            interval_ms=config.interval_ms,
            jitter_ms=config.jitter_ms or 0.0,
        )
    elif config.model == "target_qps":
        return TargetQPSArrival(rate=config.rate, variance=config.variance or 0.1)
    else:
        raise ValueError(f"Unknown arrival model: {config.model}")