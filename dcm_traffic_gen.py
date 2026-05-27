#!/usr/bin/env python3
"""
dcm_traffic_gen.py — DCM Traffic Generator (Prometheus scrape-endpoint mode)

Exposes QoS counter metrics on a /metrics HTTP endpoint for scraping by any
Prometheus-compatible collector (FFWD Universal Collector, Prometheus,
Grafana Agent, etc.).

Models a fleet of network interfaces (1–400 Gbps), 8 QoS traffic classes,
2 directions, and 6 counter columns per series — with realistic sparsity,
diurnal load variation, and ±3 % traffic noise.

─────────────────────────────────────────────────────────────────────────────
USAGE (Docker — recommended)
─────────────────────────────────────────────────────────────────────────────
    docker run --rm -p 8000:8000 \\
      -e SCALE=0.01 \\
      ghcr.io/ys1173/dcm-traffic-gen:latest

─────────────────────────────────────────────────────────────────────────────
USAGE (CLI)
─────────────────────────────────────────────────────────────────────────────
    python dcm_traffic_gen.py --scale 0.01

Point your collector at:  http://<host>:8000/metrics

─────────────────────────────────────────────────────────────────────────────
ENVIRONMENT VARIABLES
─────────────────────────────────────────────────────────────────────────────
  PORT        HTTP port to expose /metrics on           (default: 8000)
  SCALE       Fraction of full 250K-interface workload  (default: 0.01)
  DURATION    Run duration in seconds; 0 = run forever  (default: 0)
  RATE        Series updates/sec; 0 = auto              (default: 0)
  BATCH       Series updated per tick                   (default: 500)
  TABLE       Metric name prefix                        (default: dcm_telemetry)
  SEED        Topology random seed (reproducibility)    (default: 42)

─────────────────────────────────────────────────────────────────────────────
SCALE MAP  (250 K interfaces × 8 QoS × 2 directions)
─────────────────────────────────────────────────────────────────────────────
  SCALE=0.001  →    250 interfaces,    4,000 series
  SCALE=0.01   →  2,500 interfaces,   40,000 series
  SCALE=0.10   → 25,000 interfaces,  400,000 series
  SCALE=1.00   →250,000 interfaces,4,000,000 series

─────────────────────────────────────────────────────────────────────────────
METRICS EMITTED  (6 per series, TYPE counter)
─────────────────────────────────────────────────────────────────────────────
  <TABLE>_match_bytes     <TABLE>_match_packets
  <TABLE>_trans_bytes     <TABLE>_trans_packets
  <TABLE>_drop_bytes      <TABLE>_drop_packets

  Labels: direction, host, interface, qos_class
"""

from __future__ import annotations

import argparse
import math
import os
import sys
import time

import numpy as np
from prometheus_client import Counter, start_http_server


# ─────────────────────────────────────────────────────────────────────────────
# Constants
# ─────────────────────────────────────────────────────────────────────────────

FULL_INTERFACES  = 250_000
FULL_TARGET_RATE = 21_000          # series/sec at scale 1.0

QOS_CLASSES = [
    "TC1", "TC2", "TC3", "TC4", "TC5", "TC6", "TC7", "class-default"
]
DIRECTIONS = ["input", "output"]

QOS_SPARSITY = {
    "TC1":           0.05,
    "TC2":           0.02,
    "TC3":           0.10,
    "TC4":           0.72,
    "TC5":           0.73,
    "TC6":           0.15,
    "TC7":           0.20,
    "class-default": 0.01,
}

QOS_SPLIT = np.array([0.30, 0.25, 0.15, 0.10, 0.05, 0.08, 0.05, 0.02],
                     dtype=np.float64)

BANDWIDTH_TIERS = [
    (0.40, 1_000_000_000 // 8),     # 40 %: 1 Gbps
    (0.30, 10_000_000_000 // 8),    # 30 %: 10 Gbps
    (0.20, 100_000_000_000 // 8),   # 20 %: 100 Gbps
    (0.10, 400_000_000_000 // 8),   # 10 %: 400 Gbps
]

METRIC_COLS = [
    "match_packets", "match_bytes",
    "trans_packets", "trans_bytes",
    "drop_packets",  "drop_bytes",
]

COLLECTION_INTERVAL = 300   # seconds (simulated router polling interval)

LABEL_NAMES = ["direction", "host", "interface", "qos_class"]


# ─────────────────────────────────────────────────────────────────────────────
# Topology
# ─────────────────────────────────────────────────────────────────────────────

def build_topology(n_interfaces: int, seed: int = 42) -> dict:
    rng      = np.random.default_rng(seed)
    n_qos    = len(QOS_CLASSES)
    n_dir    = len(DIRECTIONS)
    n_series = n_interfaces * n_qos * n_dir

    print(f"[topology] {n_interfaces:,} interfaces "
          f"× {n_qos} QoS × {n_dir} directions = {n_series:,} series")

    tier_probs = np.array([t[0] for t in BANDWIDTH_TIERS])
    tier_rates = np.array([t[1] for t in BANDWIDTH_TIERS], dtype=np.float64)
    tier_idx   = rng.choice(len(BANDWIDTH_TIERS), size=n_interfaces, p=tier_probs)
    iface_bw   = tier_rates[tier_idx]

    iface_idx  = np.repeat(np.arange(n_interfaces), n_qos * n_dir)
    qos_idx    = np.tile(np.repeat(np.arange(n_qos), n_dir), n_interfaces)
    dir_idx    = np.tile(np.arange(n_dir), n_interfaces * n_qos)

    assert abs(QOS_SPLIT.sum() - 1.0) < 1e-9
    series_bw  = iface_bw[iface_idx] * QOS_SPLIT[qos_idx]
    series_bw *= rng.uniform(0.40, 0.90, size=n_series)

    sparsity_p    = np.array([QOS_SPARSITY[QOS_CLASSES[q]] for q in qos_idx])
    shuffle_order = rng.permutation(n_series)

    return dict(
        n_series=n_series, n_interfaces=n_interfaces,
        iface_idx=iface_idx, qos_idx=qos_idx, dir_idx=dir_idx,
        series_bw=series_bw, sparsity_p=sparsity_p,
        shuffle_order=shuffle_order,
    )


# ─────────────────────────────────────────────────────────────────────────────
# Counter State Machine
# ─────────────────────────────────────────────────────────────────────────────

class CounterState:
    """Tracks per-series traffic state; returns deltas on each visit."""

    def __init__(self, topo: dict, seed: int = 1):
        n           = topo["n_series"]
        self.topo   = topo
        self.rng    = np.random.default_rng(seed)
        # Seed last_ts one full collection interval in the past so the very
        # first visit yields a realistic non-zero delta (≈ 5 min of traffic).
        self.last_ts = np.full(n, time.time() - COLLECTION_INTERVAL,
                               dtype=np.float64)

    @staticmethod
    def diurnal_factor(ts: float) -> float:
        hour   = (ts / 3600 + 9) % 24
        factor = 0.65 + 0.35 * math.sin(math.pi * (hour - 4) / 20)
        return max(0.30, min(1.00, factor))

    def advance_slice(self, indices: np.ndarray, ts_now: float) -> np.ndarray:
        """Return per-series deltas (shape: len(indices) × 6) and update state."""
        rng     = self.rng
        n       = len(indices)
        elapsed = np.maximum(ts_now - self.last_ts[indices], 0.0)
        self.last_ts[indices] = ts_now

        df         = self.diurnal_factor(ts_now)
        bw         = self.topo["series_bw"][indices]
        sp         = self.topo["sparsity_p"][indices]
        active     = (rng.random(n) >= sp).astype(np.float64)
        noise      = 1.0 + rng.uniform(-0.03, 0.03, size=n)
        byte_delta = (bw * elapsed * df * noise * active).astype(np.uint64)

        pkt_size  = np.maximum(rng.integers(500, 1501, size=n, dtype=np.uint64), 1)
        pkt_delta = (byte_delta // pkt_size).astype(np.uint64)
        trans_r   = rng.uniform(0.80, 1.00, size=n)
        trans_b   = (byte_delta * trans_r).astype(np.uint64)
        trans_p   = (pkt_delta  * trans_r).astype(np.uint64)
        drop_b    = byte_delta - trans_b
        drop_p    = pkt_delta  - trans_p

        return np.stack(
            [pkt_delta, byte_delta, trans_p, trans_b, drop_p, drop_b], axis=1
        ).astype(np.uint64)


# ─────────────────────────────────────────────────────────────────────────────
# Update loop
# ─────────────────────────────────────────────────────────────────────────────

def run_stream(
    topo: dict,
    state: CounterState,
    prom_counters: dict,
    target_rate: float,
    duration_secs: int,
    batch_size: int = 500,
) -> None:
    n_series         = topo["n_series"]
    order            = topo["shuffle_order"]
    iface_idx        = topo["iface_idx"]
    qos_idx          = topo["qos_idx"]
    dir_idx          = topo["dir_idx"]

    ideal_batch_secs = batch_size / target_rate
    series_interval  = n_series  / target_rate
    forever          = duration_secs <= 0

    print(f"\n{'─'*62}")
    print(f"  Series            : {n_series:,}")
    print(f"  Update rate       : {target_rate:,.0f} series/sec")
    print(f"  Series revisit    : {series_interval:.1f} s")
    print(f"  Batch size        : {batch_size}")
    print(f"  Duration          : {'∞' if forever else f'{duration_secs} s'}")
    print(f"{'─'*62}\n")

    wall_start       = time.monotonic()
    pointer          = 0
    lap              = 0
    total_series     = 0
    sleep_debt       = 0.0
    last_report_t    = wall_start
    last_report_rows = 0

    while True:
        now = time.monotonic()
        if not forever and now >= wall_start + duration_secs:
            break

        batch_start = now
        end = pointer + batch_size
        if end <= n_series:
            indices = order[pointer:end]
            pointer = end
        else:
            indices = np.concatenate([order[pointer:], order[:end - n_series]])
            pointer = end - n_series
            lap    += 1

        ts_now = time.time()
        deltas = state.advance_slice(indices, ts_now)

        # Increment prometheus_client counters for each series in the batch
        for k, si in enumerate(indices):
            ne_id = int(iface_idx[si]) // 8
            port  = int(iface_idx[si]) % 8
            host  = f"NE{ne_id:05d}"
            iface = f"GigabitEthernet{ne_id}/{port}"
            qos   = QOS_CLASSES[int(qos_idx[si])]
            dirn  = DIRECTIONS[int(dir_idx[si])]

            for col_idx, col_name in enumerate(METRIC_COLS):
                delta = int(deltas[k, col_idx])
                if delta > 0:
                    prom_counters[col_name].labels(
                        direction=dirn, host=host, interface=iface, qos_class=qos
                    ).inc(delta)

        total_series += len(indices)

        elapsed_batch = time.monotonic() - batch_start
        sleep_needed  = ideal_batch_secs - elapsed_batch - sleep_debt
        if sleep_needed > 0.0002:
            time.sleep(sleep_needed)
            sleep_debt = 0.0
        else:
            sleep_debt = max(0.0, -sleep_needed)

        now = time.monotonic()
        if now - last_report_t >= 10.0:
            dt          = now - last_report_t
            actual_rate = (total_series - last_report_rows) / dt
            if forever:
                progress = f"{now - wall_start:6.0f}s elapsed"
            else:
                pct      = min(100.0, 100.0 * (now - wall_start) / duration_secs)
                progress = f"[{pct:5.1f}%] {now - wall_start:6.0f}s"
            print(f"  {progress}  rate={actual_rate:8,.0f} series/sec  "
                  f"updated={total_series:,}  lap={lap}")
            last_report_t    = now
            last_report_rows = total_series

    wall_elapsed = time.monotonic() - wall_start
    print(f"\n{'═'*62}")
    print(f"  Elapsed           : {wall_elapsed:.1f} s")
    print(f"  Series updated    : {total_series:,}")
    print(f"  Avg rate          : {total_series / wall_elapsed:,.0f} series/sec")
    print(f"  Full laps         : {lap}")
    print(f"{'═'*62}\n")


# ─────────────────────────────────────────────────────────────────────────────
# CLI
# ─────────────────────────────────────────────────────────────────────────────

def _env_bool(key: str) -> bool:
    return os.environ.get(key, "").lower() in ("1", "true", "yes")


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="DCM Traffic Generator — Prometheus scrape endpoint",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    p.add_argument(
        "--port", type=int,
        default=int(os.environ.get("PORT", "8000")),
        help="HTTP port to expose /metrics on (default: %(default)s). Env: PORT",
    )
    p.add_argument(
        "--scale", type=float,
        default=float(os.environ.get("SCALE", "0.01")),
        help="Fraction of full workload (default: %(default)s). Env: SCALE",
    )
    p.add_argument(
        "--duration", type=int,
        default=int(os.environ.get("DURATION", "0")),
        help="Run duration in seconds; 0 = run forever (default: %(default)s). Env: DURATION",
    )
    p.add_argument(
        "--rate", type=float,
        default=float(os.environ.get("RATE", "0")),
        help="Series updates/sec; 0 = auto from scale (default: %(default)s). Env: RATE",
    )
    p.add_argument(
        "--batch", type=int,
        default=int(os.environ.get("BATCH", "500")),
        help="Series updated per tick (default: %(default)s). Env: BATCH",
    )
    p.add_argument(
        "--table",
        default=os.environ.get("TABLE", "dcm_telemetry"),
        help="Metric name prefix (default: %(default)s). Env: TABLE",
    )
    p.add_argument(
        "--seed", type=int,
        default=int(os.environ.get("SEED", "42")),
        help="Random seed (default: %(default)s). Env: SEED",
    )
    return p.parse_args()


def main() -> None:
    args = parse_args()

    n_interfaces = max(1, int(FULL_INTERFACES * args.scale))
    target_rate  = args.rate if args.rate > 0 else FULL_TARGET_RATE * args.scale
    target_rate  = max(target_rate, 1.0)

    n_series       = n_interfaces * len(QOS_CLASSES) * len(DIRECTIONS)
    series_revisit = n_series / target_rate

    print(f"\nDCM Traffic Generator  (scrape-endpoint mode)")
    print(f"  Scale            : {args.scale:.4f}  ({n_interfaces:,} interfaces)")
    print(f"  Series           : {n_series:,}")
    print(f"  Update rate      : {target_rate:,.0f} series/sec")
    print(f"  Series revisit   : {series_revisit:.0f} s")
    print(f"  Duration         : {'∞' if args.duration <= 0 else f'{args.duration} s'}")
    print(f"  Metric prefix    : {args.table}_*")
    print(f"  Scrape endpoint  : http://0.0.0.0:{args.port}/metrics")

    # Create prometheus_client Counter objects (6 metrics, 4 labels each)
    prom_counters = {}
    for col in METRIC_COLS:
        prom_counters[col] = Counter(
            f"{args.table}_{col}",
            f"DCM QoS counter: {col}",
            LABEL_NAMES,
        )

    # Start the HTTP scrape endpoint
    start_http_server(args.port)
    print(f"\n[ready] /metrics listening on port {args.port}\n")

    topo  = build_topology(n_interfaces, seed=args.seed)
    state = CounterState(topo, seed=args.seed + 1)

    batch_size = args.batch

    run_stream(
        topo          = topo,
        state         = state,
        prom_counters = prom_counters,
        target_rate   = target_rate,
        duration_secs = args.duration,
        batch_size    = batch_size,
    )


if __name__ == "__main__":
    main()
