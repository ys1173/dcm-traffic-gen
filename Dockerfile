FROM python:3.12-slim

LABEL org.opencontainers.image.title="DCM Traffic Generator" \
      org.opencontainers.image.description="Constant-stream telemetry generator via Prometheus remote write" \
      org.opencontainers.image.source="https://github.com/ys1173/dcm-traffic-gen" \
      org.opencontainers.image.licenses="MIT"

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY dcm_traffic_gen.py .

# ── Config — all overridable at docker run with -e ────────────────────────────
ENV PROMETHEUS_URL=""
ENV SCALE=0.01
ENV DURATION=300
ENV RATE=0
ENV BATCH=500
ENV TABLE=dcm_telemetry
ENV SEED=42
ENV NO_PING=false

ENTRYPOINT ["python", "-u", "dcm_traffic_gen.py"]
