#!/usr/bin/env python3
"""
dcm_traffic_gen.py  —  DCM Traffic Generator

Simulates the steady-state telemetry workload produced by many distributed
collectors, each running on their own 5-min interval.  The net effect in the
field is a smoothed, constant stream rather than periodic bursts.

Sends data via Prometheus remote write (protobuf + Snappy) to any compatible
endpoint: VictoriaMetrics, Grafana Cloud, Thanos, custom ingest APIs, etc.

─────────────────────────────────────────────────────────────────────────────
USAGE (Docker — recommended)
─────────────────────────────────────────────────────────────────────────────
    docker run --rm \\
      -e PROMETHEUS_URL="https://example.com/write/TOKEN?key=KEY" \\
      -e SCALE=0.01 \\
      -e DURATION=300 \\
      ghcr.io/ys1173/dcm-traffic-gen:latest

─────────────────────────────────────────────────────────────────────────────
USAGE (CLI)
─────────────────────────────────────────────────────────────────────────────
    python dcm_traffic_gen.py \\
        --prometheus-url "https://..." \\
        --scale 0.01 --duration 60

─────────────────────────────────────────────────────────────────────────────
ENVIRONMENT VARIABLES
─────────────────────────────────────────────────────────────────────────────
  PROMETHEUS_URL    Full remote write URL incl. auth params   (required)
  SCALE             Fraction of full 250K-interface workload  (default: 0.01)
  DURATION          Run duration in seconds; 0 = run forever  (default: 300)
  RATE              Rows/sec override; 0 = SCALE × 21,000    (default: 0)
  BATCH             Rows per HTTP POST                        (default: 500)
  TABLE             Metric name prefix                        (default: dcm_telemetry)
  SEED              Topology random seed (reproducibility)    (default: 42)
  NO_PING           Skip endpoint reachability check          (default: false)

─────────────────────────────────────────────────────────────────────────────
SCALE MAP  (250 K interfaces × 8 QoS × 2 directions)
─────────────────────────────────────────────────────────────────────────────
  SCALE=0.001  →    250 interfaces,    4,000 series,    ~21 rows/sec
  SCALE=0.01   →  2,500 interfaces,   40,000 series,   ~210 rows/sec
  SCALE=0.10   → 25,000 interfaces,  400,000 series, ~2,100 rows/sec
  SCALE=1.00   →250,000 interfaces,4,000,000 series,~21,000 rows/sec
─────────────────────────────────────────────────────────────────────────────
METRICS EMITTED  (6 per series)
─────────────────────────────────────────────────────────────────────────────
  <TABLE>_match_bytes     <TABLE>_match_packets
  <TABLE>_trans_bytes     <TABLE>_trans_packets
  <TABLE>_drop_bytes      <TABLE>_drop_packets

  Labels: __name__, direction, host, interface, qos_class
"""

from __future__ import annotations

import argparse
import math
import os
import struct
import sys
import time
from typing import Union

import numpy as np
import requests

try:
    import snappy as _snappy
    _HAS_SNAPPY = True
except ImportError:
    _HAS_SNAPPY = False

# ─────────────────────────────────────────────────────────────────────────────
# Constants
# ─────────────────────────────────────────────────────────────────────────────

FULL_INTERFACES  = 250_000
FULL_TARGET_RATE = 21_000          # rows/sec at scale 1.0

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

COLLECTION_INTERVAL = 300   # seconds


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

    tier_probs  = np.array([t[0] for t in BANDWIDTH_TIERS])
    tier_rates  = np.array([t[1] for t in BANDWIDTH_TIERS], dtype=np.float64)
    tier_idx    = rng.choice(len(BANDWIDTH_TIERS), size=n_interfaces, p=tier_probs)
    iface_bw    = tier_rates[tier_idx]

    iface_idx   = np.repeat(np.arange(n_interfaces), n_qos * n_dir)
    qos_idx     = np.tile(np.repeat(np.arange(n_qos), n_dir), n_interfaces)
    dir_idx     = np.tile(np.arange(n_dir), n_interfaces * n_qos)

    assert abs(QOS_SPLIT.sum() - 1.0) < 1e-9
    series_bw   = iface_bw[iface_idx] * QOS_SPLIT[qos_idx]
    series_bw  *= rng.uniform(0.40, 0.90, size=n_series)

    sparsity_p    = np.array([QOS_SPARSITY[QOS_CLASSES[q]] for q in qos_idx])
    shuffle_order = rng.permutation(n_series)

    return dict(n_series=n_series, n_interfaces=n_interfaces,
                iface_idx=iface_idx, qos_idx=qos_idx, dir_idx=dir_idx,
                series_bw=series_bw, sparsity_p=sparsity_p,
                shuffle_order=shuffle_order)


# ─────────────────────────────────────────────────────────────────────────────
# Counter State Machine
# ─────────────────────────────────────────────────────────────────────────────

class CounterState:
    """Cumulative counters for all series, advanced on each visit."""

    def __init__(self, topo: dict, seed: int = 1):
        n               = topo["n_series"]
        self.topo       = topo
        self.rng        = np.random.default_rng(seed)
        self.cumulative = np.zeros((n, 6), dtype=np.uint64)
        self.last_ts    = np.full(n, time.time(), dtype=np.float64)

    @staticmethod
    def diurnal_factor(ts: float) -> float:
        hour   = (ts / 3600 + 9) % 24
        factor = 0.65 + 0.35 * math.sin(math.pi * (hour - 4) / 20)
        return max(0.30, min(1.00, factor))

    def advance_slice(self, indices: np.ndarray, ts_now: float) -> np.ndarray:
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

        pkt_size   = np.maximum(rng.integers(500, 1501, size=n, dtype=np.uint64), 1)
        pkt_delta  = (byte_delta // pkt_size).astype(np.uint64)
        trans_r    = rng.uniform(0.80, 1.00, size=n)
        trans_b    = (byte_delta * trans_r).astype(np.uint64)
        trans_p    = (pkt_delta  * trans_r).astype(np.uint64)
        drop_b     = byte_delta - trans_b
        drop_p     = pkt_delta  - trans_p

        deltas = np.stack(
            [pkt_delta, byte_delta, trans_p, trans_b, drop_p, drop_b], axis=1
        ).astype(np.uint64)
        self.cumulative[indices] += deltas
        return self.cumulative[indices].copy()


# ─────────────────────────────────────────────────────────────────────────────
# Minimal Protobuf encoder  (no .proto compilation required)
# ─────────────────────────────────────────────────────────────────────────────

def _pb_varint(n: int) -> bytes:
    out = []
    while True:
        bits = n & 0x7F;  n >>= 7
        out.append(bits | (0x80 if n else 0))
        if not n:
            break
    return bytes(out)

def _pb_ld(field: int, data: bytes) -> bytes:
    return _pb_varint((field << 3) | 2) + _pb_varint(len(data)) + data

def _pb_str(field: int, s: str) -> bytes:
    return _pb_ld(field, s.encode())

def _pb_double(field: int, v: float) -> bytes:
    return _pb_varint((field << 3) | 1) + struct.pack('<d', v)

def _pb_int64(field: int, v: int) -> bytes:
    return _pb_varint((field << 3) | 0) + _pb_varint(v)

def _label(name: str, value: str) -> bytes:
    return _pb_str(1, name) + _pb_str(2, value)

def _sample(value: float, ts_ms: int) -> bytes:
    return _pb_double(1, value) + _pb_int64(2, ts_ms)

def _timeseries(labels: list[bytes], sample: bytes) -> bytes:
    return b"".join(_pb_ld(1, lb) for lb in labels) + _pb_ld(2, sample)

def _write_request(ts_list: list[bytes]) -> bytes:
    return b"".join(_pb_ld(1, ts) for ts in ts_list)


# ─────────────────────────────────────────────────────────────────────────────
# Prometheus Remote Write
# ─────────────────────────────────────────────────────────────────────────────

class PrometheusWriter:
    """
    Sends snappy-compressed WriteRequest batches to a Prometheus remote write
    endpoint.  Each row produces 6 TimeSeries (one per counter column).
    Labels are sorted alphabetically as required by Prometheus.
    """

    HEADERS = {
        "Content-Type":                      "application/x-protobuf",
        "Content-Encoding":                  "snappy",
        "X-Prometheus-Remote-Write-Version": "0.1.0",
        "User-Agent":                        "dcm-traffic-gen/1.0",
    }

    def __init__(self, url: str, table: str = "dcm_telemetry"):
        if not _HAS_SNAPPY:
            sys.exit("ERROR: python-snappy is required — pip install python-snappy")
        self.url     = url
        self.table   = table
        self.session = requests.Session()
        self.session.headers.update(self.HEADERS)
        self.rows_sent           = 0
        self.errors              = 0
        self._consecutive_errors = 0

    def format(self, topo: dict, indices: np.ndarray,
               counters: np.ndarray, ts_ns: int) -> bytes:
        ts_ms     = ts_ns // 1_000_000
        iface_idx = topo["iface_idx"]
        qos_idx   = topo["qos_idx"]
        dir_idx   = topo["dir_idx"]
        all_ts: list[bytes] = []

        for k, si in enumerate(indices):
            ne_id     = int(iface_idx[si]) // 8
            port      = int(iface_idx[si]) % 8
            qos       = QOS_CLASSES[int(qos_idx[si])]
            dirn      = DIRECTIONS[int(dir_idx[si])]
            # Labels sorted: __name__ < direction < host < interface < qos_class
            tag_labels = [
                _label("direction", dirn),
                _label("host",      f"NE{ne_id:05d}"),
                _label("interface", f"GigabitEthernet{ne_id}/{port}"),
                _label("qos_class", qos),
            ]
            for col_idx, col_name in enumerate(METRIC_COLS):
                labels = [_label("__name__", f"{self.table}_{col_name}")] + tag_labels
                all_ts.append(_timeseries(labels, _sample(float(counters[k, col_idx]), ts_ms)))

        return _snappy.compress(_write_request(all_ts))

    def send(self, payload: bytes) -> bool:
        try:
            r  = self.session.post(self.url, data=payload, timeout=30)
            ok = r.status_code in (200, 204)
            if ok:
                self._consecutive_errors = 0
                self.rows_sent += 1
            else:
                self.errors += 1
                self._consecutive_errors += 1
                if self.errors <= 3 or self.errors % 20 == 0:
                    print(f"  [WARN] HTTP {r.status_code}: {r.text[:300]}")
            return ok
        except Exception as exc:
            self.errors += 1
            self._consecutive_errors += 1
            if self.errors <= 3 or self.errors % 20 == 0:
                print(f"  [ERR]  {exc}")
            return False

    @property
    def too_many_errors(self) -> bool:
        return self._consecutive_errors >= 10


# ─────────────────────────────────────────────────────────────────────────────
# Stream loop
# ─────────────────────────────────────────────────────────────────────────────

def run_stream(topo: dict, state: CounterState, writer: PrometheusWriter,
               target_rate: float, duration_secs: int,
               batch_size: int = 500) -> None:
    n_series = topo["n_series"]
    order    = topo["shuffle_order"]

    ideal_batch_secs = batch_size / target_rate
    series_interval  = n_series  / target_rate
    forever          = duration_secs <= 0

    print(f"\n{'─'*62}")
    print(f"  Series            : {n_series:,}")
    print(f"  Target rate       : {target_rate:,.0f} rows/sec")
    print(f"  Series revisit    : {series_interval:.1f} s  "
          f"(vs {COLLECTION_INTERVAL} s collection interval)")
    print(f"  Batch size        : {batch_size}  "
          f"({batch_size * len(METRIC_COLS):,} samples/POST)")
    print(f"  Duration          : {'∞' if forever else f'{duration_secs} s'}")
    print(f"  Metric prefix     : {writer.table}_*")
    print(f"{'─'*62}\n")

    wall_start       = time.monotonic()
    pointer          = 0
    lap              = 0
    total_rows       = 0
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

        ts_ns    = int(time.time() * 1e9)
        counters = state.advance_slice(indices, ts_ns / 1e9)
        writer.send(writer.format(topo, indices, counters, ts_ns))
        total_rows += len(indices)

        if writer.too_many_errors:
            print("\n[FATAL] 10 consecutive send errors — aborting.")
            break

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
            actual_rate = (total_rows - last_report_rows) / dt
            if forever:
                progress = f"{now - wall_start:6.0f}s elapsed"
            else:
                pct      = min(100.0, 100.0 * (now - wall_start) / duration_secs)
                progress = f"[{pct:5.1f}%] {now - wall_start:6.0f}s"
            print(f"  {progress}  rate={actual_rate:8,.0f} rows/sec  "
                  f"sent={total_rows:,}  errors={writer.errors}  lap={lap}")
            last_report_t    = now
            last_report_rows = total_rows

    wall_elapsed = time.monotonic() - wall_start
    print(f"\n{'═'*62}")
    print(f"  Elapsed           : {wall_elapsed:.1f} s")
    print(f"  Rows emitted      : {total_rows:,}")
    print(f"  Avg rate          : {total_rows / wall_elapsed:,.0f} rows/sec")
    print(f"  Batches sent      : {writer.rows_sent}")
    print(f"  Send errors       : {writer.errors}")
    print(f"  Full laps         : {lap}")
    print(f"{'═'*62}\n")


# ─────────────────────────────────────────────────────────────────────────────
# CLI
# ─────────────────────────────────────────────────────────────────────────────

def _env_bool(key: str) -> bool:
    return os.environ.get(key, "").lower() in ("1", "true", "yes")


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="DCM Traffic Generator — Prometheus remote write",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    p.add_argument(
        "--prometheus-url",
        default=os.environ.get("PROMETHEUS_URL", ""),
        metavar="URL",
        help="Full remote write URL incl. auth params. Env: PROMETHEUS_URL",
    )
    p.add_argument(
        "--scale", type=float,
        default=float(os.environ.get("SCALE", "0.01")),
        help="Fraction of full workload (default: %(default)s). Env: SCALE",
    )
    p.add_argument(
        "--duration", type=int,
        default=int(os.environ.get("DURATION", "300")),
        help="Run duration in seconds; 0 = run forever (default: %(default)s). Env: DURATION",
    )
    p.add_argument(
        "--rate", type=float,
        default=float(os.environ.get("RATE", "0")),
        help="Rows/sec override; 0 = SCALE × 21,000 (default: %(default)s). Env: RATE",
    )
    p.add_argument(
        "--batch", type=int,
        default=int(os.environ.get("BATCH", "500")),
        help="Rows per HTTP POST (default: %(default)s). Env: BATCH",
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
    p.add_argument(
        "--no-ping", action="store_true",
        default=_env_bool("NO_PING"),
        help="Skip endpoint reachability check. Env: NO_PING",
    )
    return p.parse_args()


def main() -> None:
    args = parse_args()

    if not args.prometheus_url:
        sys.exit("ERROR: --prometheus-url (or PROMETHEUS_URL env var) is required.")

    n_interfaces = max(1, int(FULL_INTERFACES * args.scale))
    target_rate  = args.rate if args.rate > 0 else FULL_TARGET_RATE * args.scale

    print(f"\nDCM Traffic Generator")
    print(f"  Scale            : {args.scale:.4f}  ({n_interfaces:,} interfaces)")
    print(f"  Target rate      : {target_rate:,.0f} rows/sec")
    print(f"  Duration         : {'∞' if args.duration <= 0 else f'{args.duration} s'}")
    print(f"  Metric prefix    : {args.table}_*")
    print(f"  Endpoint         : {args.prometheus_url[:72]}{'…' if len(args.prometheus_url) > 72 else ''}")

    writer = PrometheusWriter(args.prometheus_url, table=args.table)

    if not args.no_ping:
        print(f"\nEndpoint reachability … SKIP  "
              f"(Prometheus remote write has no standard /health)")

    topo  = build_topology(n_interfaces, seed=args.seed)
    state = CounterState(topo, seed=args.seed + 1)

    max_batch  = max(1, int(target_rate * 5))
    batch_size = min(args.batch, max_batch)
    if batch_size != args.batch:
        print(f"  [info] Batch clamped to {batch_size} (≤ 5 s worth at {target_rate:.0f} rows/sec)")

    run_stream(
        topo          = topo,
        state         = state,
        writer        = writer,
        target_rate   = target_rate,
        duration_secs = args.duration,
        batch_size    = batch_size,
    )


if __name__ == "__main__":
    main()
