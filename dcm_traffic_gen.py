#!/usr/bin/env python3
"""
dcm_traffic_gen.py — DCM Traffic Generator (Prometheus scrape-endpoint mode)

Exposes QoS counter metrics on a /metrics HTTP endpoint for scraping by any
Prometheus-compatible collector (FFWD Universal Collector, Prometheus,
Grafana Agent, Vector, etc.).

The /metrics response is pre-rendered in a background thread so the HTTP
handler always returns instantly — scrape timeouts are never hit regardless
of scale.

─────────────────────────────────────────────────────────────────────────────
USAGE (Docker)
─────────────────────────────────────────────────────────────────────────────
    docker run --rm -p 8000:8000 -e SCALE=0.01 ghcr.io/ys1173/dcm-traffic-gen:latest

─────────────────────────────────────────────────────────────────────────────
USAGE (CLI)
─────────────────────────────────────────────────────────────────────────────
    python dcm_traffic_gen.py --scale 0.01

─────────────────────────────────────────────────────────────────────────────
ENVIRONMENT VARIABLES
─────────────────────────────────────────────────────────────────────────────
  PORT        HTTP port to expose /metrics on           (default: 8000)
  SCALE       Fraction of full 250K-interface workload  (default: 0.01)
  DURATION    Run duration in seconds; 0 = run forever  (default: 0)
  RATE        Series updates/sec; 0 = auto              (default: 0)
  BATCH       Series updated per tick                   (default: 500)
  TABLE       Metric name prefix                        (default: dcm_telemetry)
  SEED        Topology random seed                      (default: 42)

─────────────────────────────────────────────────────────────────────────────
METRICS EMITTED  (6 per series, TYPE counter)
─────────────────────────────────────────────────────────────────────────────
  <TABLE>_match_bytes_total     <TABLE>_match_packets_total
  <TABLE>_trans_bytes_total     <TABLE>_trans_packets_total
  <TABLE>_drop_bytes_total      <TABLE>_drop_packets_total

  Labels: direction, host, interface, qos_class
"""

from __future__ import annotations

import argparse
import gzip
import http.server
import io
import math
import os
import threading
import time

import numpy as np


# ─────────────────────────────────────────────────────────────────────────────
# Constants
# ─────────────────────────────────────────────────────────────────────────────

FULL_INTERFACES  = 250_000
FULL_TARGET_RATE = 21_000          # series/sec at scale 1.0

QOS_CLASSES = ["TC1", "TC2", "TC3", "TC4", "TC5", "TC6", "TC7", "class-default"]
DIRECTIONS  = ["input", "output"]

QOS_SPARSITY = {
    "TC1": 0.05, "TC2": 0.02, "TC3": 0.10, "TC4": 0.72,
    "TC5": 0.73, "TC6": 0.15, "TC7": 0.20, "class-default": 0.01,
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
RENDER_INTERVAL     = 30    # seconds between background buffer re-renders


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
    """Tracks per-series cumulative counters, advanced on each visit."""

    def __init__(self, topo: dict, seed: int = 1):
        n             = topo["n_series"]
        self.topo     = topo
        self.rng      = np.random.default_rng(seed)
        self.cumulative = np.zeros((n, 6), dtype=np.uint64)
        # Bootstrap one full collection interval in the past so the first
        # visit to every series yields a realistic non-zero delta.
        self.last_ts  = np.full(n, time.time() - COLLECTION_INTERVAL,
                                dtype=np.float64)

    @staticmethod
    def diurnal_factor(ts: float) -> float:
        hour = (ts / 3600 + 9) % 24
        return max(0.30, min(1.00, 0.65 + 0.35 * math.sin(math.pi * (hour - 4) / 20)))

    def advance_slice(self, indices: np.ndarray, ts_now: float) -> None:
        """Advance counters for the given series indices (updates cumulative in-place)."""
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

        deltas = np.stack(
            [pkt_delta, byte_delta, trans_p, trans_b, drop_p, drop_b], axis=1
        ).astype(np.uint64)
        self.cumulative[indices] += deltas


# ─────────────────────────────────────────────────────────────────────────────
# Pre-rendered metrics buffer
# ─────────────────────────────────────────────────────────────────────────────

class MetricsBuffer:
    """
    Thread-safe holder for a pre-rendered, gzip-compressed /metrics payload.

    The HTTP handler reads from here (instant), while a background thread
    updates the buffer asynchronously — scrape timeouts are never hit.
    """

    def __init__(self) -> None:
        self._lock        = threading.Lock()
        self._plain: bytes = b"# initialising...\n"
        self._gz: bytes    = gzip.compress(self._plain, compresslevel=1)
        self._rendered_at  = 0.0

    def update(self, plain: bytes) -> None:
        gz = gzip.compress(plain, compresslevel=1)
        with self._lock:
            self._plain       = plain
            self._gz          = gz
            self._rendered_at = time.time()

    def get(self, want_gz: bool) -> tuple[bytes, bool]:
        with self._lock:
            return (self._gz, True) if want_gz else (self._plain, False)

    @property
    def age_secs(self) -> float:
        return time.time() - self._rendered_at


# ─────────────────────────────────────────────────────────────────────────────
# Label prefix pre-computation and fast rendering
# ─────────────────────────────────────────────────────────────────────────────

def build_label_prefixes(topo: dict, table: str) -> tuple[bytes, list[bytes]]:
    """
    Pre-compute static label prefix bytes for every series.

    Returns:
        headers  – HELP/TYPE block bytes for all 6 metrics
        label_bytes – per-series label bytes (reused across all 6 metrics)
                      e.g. b'direction="input",host="NE00001",...'
    """
    iface_idx = topo["iface_idx"]
    qos_idx   = topo["qos_idx"]
    dir_idx   = topo["dir_idx"]
    n         = topo["n_series"]

    # Build HELP/TYPE headers
    hdr = io.BytesIO()
    for col in METRIC_COLS:
        mname = f"{table}_{col}_total"
        hdr.write(f"# HELP {mname} DCM QoS counter: {col}\n".encode())
        hdr.write(f"# TYPE {mname} counter\n".encode())
    headers = hdr.getvalue()

    # Pre-compute per-series label block (metric-name-agnostic)
    print(f"  Building label prefix map for {n:,} series …", end="", flush=True)
    t0 = time.monotonic()
    label_bytes: list[bytes] = []
    for si in range(n):
        ne_id = int(iface_idx[si]) // 8
        port  = int(iface_idx[si]) % 8
        qos   = QOS_CLASSES[int(qos_idx[si])]
        dirn  = DIRECTIONS[int(dir_idx[si])]
        label_bytes.append(
            f'direction="{dirn}",host="NE{ne_id:05d}",'
            f'interface="GigabitEthernet{ne_id}/{port}",'
            f'qos_class="{qos}"'.encode()
        )
    print(f" done ({time.monotonic() - t0:.1f}s)")
    return headers, label_bytes


def render_metrics(topo: dict, snap: np.ndarray,
                   headers: bytes, label_bytes: list[bytes],
                   table: str) -> bytes:
    """
    Fast render of the full Prometheus text payload.

    Uses pre-computed per-series label bytes and list-comprehension joins
    to minimise Python overhead per line.
    """
    n   = topo["n_series"]
    buf = io.BytesIO()
    buf.write(headers)

    # Convert numpy array to Python list once for fast scalar access
    vals = snap.tolist()   # list[list[int]], shape (n, 6)

    for col_idx, col in enumerate(METRIC_COLS):
        mname_b = f"{table}_{col}_total{{".encode()
        closing = b"} "
        nl      = b"\n"
        # list-comprehension + single join: fastest pure-Python bulk string build
        chunk = b"".join(
            mname_b + label_bytes[si] + closing
            + str(vals[si][col_idx]).encode() + nl
            for si in range(n)
        )
        buf.write(chunk)

    return buf.getvalue()


# ─────────────────────────────────────────────────────────────────────────────
# HTTP server  (instant response from pre-built buffer)
# ─────────────────────────────────────────────────────────────────────────────

def start_http_server(port: int, mbuf: MetricsBuffer) -> http.server.HTTPServer:
    class Handler(http.server.BaseHTTPRequestHandler):
        def do_GET(self) -> None:
            if self.path not in ("/metrics", "/"):
                self.send_response(404)
                self.end_headers()
                return
            want_gz    = "gzip" in self.headers.get("Accept-Encoding", "")
            data, is_gz = mbuf.get(want_gz)
            self.send_response(200)
            self.send_header("Content-Type",
                             "text/plain; version=0.0.4; charset=utf-8")
            if is_gz:
                self.send_header("Content-Encoding", "gzip")
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)

        def log_message(self, *_) -> None:
            pass  # suppress per-request logs

    srv = http.server.ThreadingHTTPServer(("", port), Handler)
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    return srv


# ─────────────────────────────────────────────────────────────────────────────
# Update + render loop
# ─────────────────────────────────────────────────────────────────────────────

def run_stream(
    topo: dict,
    state: CounterState,
    mbuf: MetricsBuffer,
    headers: bytes,
    label_bytes: list[bytes],
    table: str,
    target_rate: float,
    duration_secs: int,
    batch_size: int,
) -> None:
    n_series         = topo["n_series"]
    order            = topo["shuffle_order"]
    ideal_batch_secs = batch_size / target_rate
    series_interval  = n_series / target_rate
    forever          = duration_secs <= 0

    # Render flag prevents concurrent renders if one is still running
    _rendering = threading.Event()

    def do_render(snap: np.ndarray) -> None:
        t0    = time.monotonic()
        plain = render_metrics(topo, snap, headers, label_bytes, table)
        mbuf.update(plain)
        elapsed  = time.monotonic() - t0
        plain_mb = len(plain) / 1e6
        gz_mb    = len(mbuf.get(True)[0]) / 1e6
        print(f"  [render] {plain_mb:.1f} MB plain → {gz_mb:.1f} MB gzip  "
              f"({elapsed:.1f}s)  buf_age={mbuf.age_secs:.0f}s")
        _rendering.clear()

    def trigger_render() -> None:
        if not _rendering.is_set():
            _rendering.set()
            snap = state.cumulative.copy()
            threading.Thread(target=do_render, args=(snap,), daemon=True).start()

    # Initial render so first scrape gets real data
    print("  [render] building initial metrics buffer …")
    do_render(state.cumulative.copy())

    print(f"\n{'─'*62}")
    print(f"  Series            : {n_series:,}")
    print(f"  Update rate       : {target_rate:,.0f} series/sec")
    print(f"  Series revisit    : {series_interval:.1f} s")
    print(f"  Buffer refresh    : every ~{RENDER_INTERVAL}s or each full lap")
    print(f"  Batch size        : {batch_size}")
    print(f"  Duration          : {'∞' if forever else f'{duration_secs} s'}")
    print(f"{'─'*62}\n")

    wall_start    = time.monotonic()
    pointer       = 0
    lap           = 0
    total_series  = 0
    sleep_debt    = 0.0
    last_report_t = wall_start
    last_report_n = 0
    last_render_t = wall_start

    while True:
        now = time.monotonic()
        if not forever and now >= wall_start + duration_secs:
            break

        # Build batch
        batch_start = now
        end = pointer + batch_size
        if end <= n_series:
            indices = order[pointer:end]
            pointer = end
        else:
            indices = np.concatenate([order[pointer:], order[:end - n_series]])
            pointer = end - n_series
            lap    += 1
            trigger_render()   # re-render after each full lap

        state.advance_slice(indices, time.time())
        total_series += len(indices)

        # Also re-render every RENDER_INTERVAL seconds (first lap at large scales)
        now = time.monotonic()
        if now - last_render_t >= RENDER_INTERVAL:
            trigger_render()
            last_render_t = now

        # Pace to target rate
        elapsed_batch = time.monotonic() - batch_start
        sleep_needed  = ideal_batch_secs - elapsed_batch - sleep_debt
        if sleep_needed > 0.0002:
            time.sleep(sleep_needed)
            sleep_debt = 0.0
        else:
            sleep_debt = max(0.0, -sleep_needed)

        # Progress report every 10s
        now = time.monotonic()
        if now - last_report_t >= 10.0:
            dt   = now - last_report_t
            rate = (total_series - last_report_n) / dt
            if forever:
                progress = f"{now - wall_start:6.0f}s elapsed"
            else:
                pct      = min(100.0, 100.0 * (now - wall_start) / duration_secs)
                progress = f"[{pct:5.1f}%] {now - wall_start:6.0f}s"
            print(f"  {progress}  rate={rate:8,.0f} series/sec  "
                  f"updated={total_series:,}  lap={lap}  buf_age={mbuf.age_secs:.0f}s")
            last_report_t = now
            last_report_n = total_series

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

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="DCM Traffic Generator — Prometheus scrape endpoint",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    p.add_argument("--port",     type=int,   default=int(os.environ.get("PORT",     "8000")))
    p.add_argument("--scale",    type=float, default=float(os.environ.get("SCALE",  "0.01")))
    p.add_argument("--duration", type=int,   default=int(os.environ.get("DURATION", "0")))
    p.add_argument("--rate",     type=float, default=float(os.environ.get("RATE",   "0")))
    p.add_argument("--batch",    type=int,   default=int(os.environ.get("BATCH",    "500")))
    p.add_argument("--table",    default=os.environ.get("TABLE", "dcm_telemetry"))
    p.add_argument("--seed",     type=int,   default=int(os.environ.get("SEED",     "42")))
    return p.parse_args()


def main() -> None:
    args = parse_args()

    n_interfaces   = max(1, int(FULL_INTERFACES * args.scale))
    target_rate    = max(args.rate if args.rate > 0 else FULL_TARGET_RATE * args.scale, 1.0)
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
    print()

    topo  = build_topology(n_interfaces, seed=args.seed)
    state = CounterState(topo, seed=args.seed + 1)

    headers, label_bytes = build_label_prefixes(topo, args.table)

    mbuf = MetricsBuffer()
    start_http_server(args.port, mbuf)
    print(f"\n[ready] /metrics listening on port {args.port}\n")

    run_stream(
        topo          = topo,
        state         = state,
        mbuf          = mbuf,
        headers       = headers,
        label_bytes   = label_bytes,
        table         = args.table,
        target_rate   = target_rate,
        duration_secs = args.duration,
        batch_size    = args.batch,
    )


if __name__ == "__main__":
    main()
