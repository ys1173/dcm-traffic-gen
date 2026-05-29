# dcm-traffic-gen

Constant-stream telemetry traffic generator for network QoS counters.

Models a fleet of network interfaces (1 Gbps – 400 Gbps bandwidth tiers),
8 QoS traffic classes, 2 directions, and 6 counter columns per series — with
realistic sparsity, diurnal load variation, and ±3 % traffic noise.

Two output modes:

| Mode | How it works |
|---|---|
| **scrape** (default) | Exposes a Prometheus `/metrics` endpoint. Any Prometheus-compatible collector (FFWD UC, Prometheus, Grafana Agent, Vector) scrapes it. |
| **otlp** | Pushes metrics to an OTLP gRPC endpoint every N seconds. Use with an OpenTelemetry Collector or a FFWD UC configured as an OTLP receiver. |

---

## Quick start

```bash
# Scrape mode (default) — expose /metrics on port 8000
docker run --rm -p 8000:8000 \
  -e SCALE=0.01 \
  ghcr.io/ys1173/dcm-traffic-gen:latest

# OTLP gRPC push mode
docker run --rm \
  -e MODE=otlp \
  -e OTLP_ENDPOINT=collector:4317 \
  -e SCALE=0.01 \
  ghcr.io/ys1173/dcm-traffic-gen:latest
```

---

## Configuration

All options are available as environment variables (`-e`) or CLI flags.

### Common options

| Env var | CLI flag | Default | Description |
|---|---|---|---|
| `MODE` | `--mode` | `scrape` | Output mode: `scrape` or `otlp` |
| `SCALE` | `--scale` | `0.01` | Fraction of full 250 K-interface workload |
| `DURATION` | `--duration` | `0` | Run duration in seconds; `0` = run forever |
| `RATE` | `--rate` | `0` | Series updates/sec; `0` = `SCALE × 21,000` |
| `BATCH` | `--batch` | `500` | Series updated per counter-update tick |
| `TABLE` | `--table` | `dcm_telemetry` | Metric name prefix |
| `SEED` | `--seed` | `42` | Random seed for reproducible topology |

### Scrape mode options

| Env var | CLI flag | Default | Description |
|---|---|---|---|
| `PORT` | `--port` | `8000` | HTTP port to expose `/metrics` on |

### OTLP mode options

| Env var | CLI flag | Default | Description |
|---|---|---|---|
| `OTLP_ENDPOINT` | `--otlp-endpoint` | `localhost:4317` | OTLP gRPC endpoint `host:port` |
| `OTLP_INTERVAL` | `--otlp-interval` | `30` | Seconds between exports |
| `OTLP_INSECURE` | `--otlp-insecure` | `true` | Use insecure channel (no TLS) |
| `OTLP_BATCH` | `--otlp-batch` | `5000` | Series per `ExportMetricsServiceRequest` |

### Scale map

| `SCALE` | Interfaces | Series |
|---|---|---|
| `0.001` | 250 | 4,000 |
| `0.01` | 2,500 | 40,000 |
| `0.10` | 25,000 | 400,000 |
| `1.00` | 250,000 | 4,000,000 |

---

## Metrics emitted

Six counter metrics per series, labelled by `direction`, `host`, `interface`, and `qos_class`:

```
dcm_telemetry_match_bytes_total      dcm_telemetry_match_packets_total
dcm_telemetry_trans_bytes_total      dcm_telemetry_trans_packets_total
dcm_telemetry_drop_bytes_total       dcm_telemetry_drop_packets_total
```

Override the prefix with `TABLE=my_prefix`.

---

## Examples

```bash
# Scrape mode — 1% scale, run forever, default port 8000
docker run --rm -p 8000:8000 \
  -e SCALE=0.01 \
  ghcr.io/ys1173/dcm-traffic-gen:latest

# Scrape mode — 10% scale, custom port, custom metric prefix
docker run --rm -p 9090:9090 \
  -e SCALE=0.10 \
  -e PORT=9090 \
  -e TABLE=network_telemetry \
  ghcr.io/ys1173/dcm-traffic-gen:latest

# OTLP push mode — send to local OTel Collector every 30s
docker run --rm \
  -e MODE=otlp \
  -e OTLP_ENDPOINT=otel-collector:4317 \
  -e SCALE=0.01 \
  ghcr.io/ys1173/dcm-traffic-gen:latest

# OTLP push mode — TLS, faster export interval
docker run --rm \
  -e MODE=otlp \
  -e OTLP_ENDPOINT=collector.example.com:4317 \
  -e OTLP_INSECURE=false \
  -e OTLP_INTERVAL=15 \
  -e SCALE=0.01 \
  ghcr.io/ys1173/dcm-traffic-gen:latest

# CLI (without Docker)
pip install numpy opentelemetry-proto grpcio
python dcm_traffic_gen.py --scale 0.01                           # scrape
python dcm_traffic_gen.py --mode otlp --otlp-endpoint host:4317  # otlp
```

---

## OTLP export performance notes

Python protobuf object construction is the bottleneck in OTLP mode. Approximate
export durations at different scales with the default `--otlp-batch 5000`:

| `SCALE` | Series | Approx export time |
|---|---|---|
| `0.01` | 40,000 | ~0.5 s |
| `0.05` | 200,000 | ~3 s |
| `0.10` | 400,000 | ~6 s |

The generator warns if an export takes more than 80% of `--otlp-interval`.
Increase `--otlp-interval` or `--otlp-batch` (fewer, larger requests) to tune.

---

## Docker image

Built for `linux/amd64` and `linux/arm64`. Published automatically to
`ghcr.io/ys1173/dcm-traffic-gen` on every push to `main` and on version tags
(`v*.*.*`).

```
ghcr.io/ys1173/dcm-traffic-gen:latest
ghcr.io/ys1173/dcm-traffic-gen:main
ghcr.io/ys1173/dcm-traffic-gen:2.1.0
```
