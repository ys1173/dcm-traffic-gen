# dcm-traffic-gen

Constant-stream telemetry traffic generator that sends realistic QoS counter
metrics to any **Prometheus remote write** compatible endpoint.

Models a fleet of network interfaces (1 Gbps – 400 Gbps bandwidth tiers),
8 QoS traffic classes, 2 directions, and 6 counter columns per series — with
realistic sparsity, diurnal load variation, and ±3 % traffic noise.

---

## Quick start

```bash
docker run --rm \
  -e PROMETHEUS_URL="https://your-endpoint/write/TOKEN?key=KEY" \
  -e SCALE=0.01 \
  -e DURATION=60 \
  ghcr.io/ys1173/dcm-traffic-gen:latest
```

---

## Configuration

All options are available as environment variables (`-e`) or CLI flags.

| Env var | CLI flag | Default | Description |
|---|---|---|---|
| `PROMETHEUS_URL` | `--prometheus-url` | *(required)* | Full remote write URL including auth params |
| `SCALE` | `--scale` | `0.01` | Fraction of full 250 K-interface workload |
| `DURATION` | `--duration` | `300` | Run duration in seconds; `0` = run forever |
| `RATE` | `--rate` | `0` | Rows/sec override; `0` = `SCALE × 21,000` |
| `BATCH` | `--batch` | `500` | Rows per HTTP POST |
| `TABLE` | `--table` | `dcm_telemetry` | Metric name prefix |
| `SEED` | `--seed` | `42` | Random seed for reproducible topology |
| `NO_PING` | `--no-ping` | `false` | Skip endpoint reachability check |

### Scale map

| `SCALE` | Interfaces | Series | Rows/sec |
|---|---|---|---|
| `0.001` | 250 | 4,000 | ~21 |
| `0.01` | 2,500 | 40,000 | ~210 |
| `0.10` | 25,000 | 400,000 | ~2,100 |
| `1.00` | 250,000 | 4,000,000 | ~21,000 |

---

## Metrics emitted

Six counter metrics per series, labelled by `host`, `interface`, `qos_class`, and `direction`:

```
dcm_telemetry_match_bytes      dcm_telemetry_match_packets
dcm_telemetry_trans_bytes      dcm_telemetry_trans_packets
dcm_telemetry_drop_bytes       dcm_telemetry_drop_packets
```

Override the prefix with `TABLE=my_prefix`.

---

## Examples

```bash
# 10 % scale, run forever
docker run --rm \
  -e PROMETHEUS_URL="https://..." \
  -e SCALE=0.10 \
  -e DURATION=0 \
  ghcr.io/ys1173/dcm-traffic-gen:latest

# Full scale, custom metric prefix, larger batches
docker run --rm \
  -e PROMETHEUS_URL="https://..." \
  -e SCALE=1.0 \
  -e TABLE=network_telemetry \
  -e BATCH=2000 \
  ghcr.io/ys1173/dcm-traffic-gen:latest

# CLI (without Docker)
pip install numpy requests python-snappy
python dcm_traffic_gen.py \
  --prometheus-url "https://..." \
  --scale 0.01 --duration 60
```

---

## Docker image

Built for `linux/amd64` and `linux/arm64`. Published automatically to
`ghcr.io/ys1173/dcm-traffic-gen` on every push to `main` and on version tags
(`v*.*.*`).

```
ghcr.io/ys1173/dcm-traffic-gen:latest
ghcr.io/ys1173/dcm-traffic-gen:main
ghcr.io/ys1173/dcm-traffic-gen:1.0.0
```
