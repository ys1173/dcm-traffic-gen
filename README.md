# dcm-traffic-gen

Constant-stream telemetry traffic generator that exposes realistic QoS counter
metrics on a **Prometheus `/metrics` scrape endpoint**.

Models a fleet of network interfaces (1 Gbps – 400 Gbps bandwidth tiers),
8 QoS traffic classes, 2 directions, and 6 counter columns per series — with
realistic sparsity, diurnal load variation, and ±3 % traffic noise.

Point any Prometheus-compatible collector (FFWD Universal Collector, Prometheus,
Grafana Agent, etc.) at `http://<host>:8000/metrics` to collect the data.

---

## Quick start

```bash
docker run --rm -p 8000:8000 \
  -e SCALE=0.01 \
  ghcr.io/ys1173/dcm-traffic-gen:latest
```

Then scrape: `http://localhost:8000/metrics`

---

## Configuration

All options are available as environment variables (`-e`) or CLI flags.

| Env var | CLI flag | Default | Description |
|---|---|---|---|
| `PORT` | `--port` | `8000` | HTTP port to expose `/metrics` on |
| `SCALE` | `--scale` | `0.01` | Fraction of full 250 K-interface workload |
| `DURATION` | `--duration` | `0` | Run duration in seconds; `0` = run forever |
| `RATE` | `--rate` | `0` | Series updates/sec; `0` = `SCALE × 21,000` |
| `BATCH` | `--batch` | `500` | Series updated per tick |
| `TABLE` | `--table` | `dcm_telemetry` | Metric name prefix |
| `SEED` | `--seed` | `42` | Random seed for reproducible topology |

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
dcm_telemetry_match_bytes      dcm_telemetry_match_packets
dcm_telemetry_trans_bytes      dcm_telemetry_trans_packets
dcm_telemetry_drop_bytes       dcm_telemetry_drop_packets
```

Override the prefix with `TABLE=my_prefix`.

---

## Examples

```bash
# 1% scale, run forever, default port 8000
docker run --rm -p 8000:8000 \
  -e SCALE=0.01 \
  ghcr.io/ys1173/dcm-traffic-gen:latest

# 10% scale, custom port, custom metric prefix
docker run --rm -p 9090:9090 \
  -e SCALE=0.10 \
  -e PORT=9090 \
  -e TABLE=network_telemetry \
  ghcr.io/ys1173/dcm-traffic-gen:latest

# CLI (without Docker)
pip install numpy prometheus-client
python dcm_traffic_gen.py --scale 0.01
```

---

## Docker image

Built for `linux/amd64` and `linux/arm64`. Published automatically to
`ghcr.io/ys1173/dcm-traffic-gen` on every push to `main` and on version tags
(`v*.*.*`).

```
ghcr.io/ys1173/dcm-traffic-gen:latest
ghcr.io/ys1173/dcm-traffic-gen:main
ghcr.io/ys1173/dcm-traffic-gen:2.0.0
```
