FROM python:3.12-slim

LABEL org.opencontainers.image.title="DCM Traffic Generator" \
      org.opencontainers.image.description="Constant-stream telemetry generator, Prometheus scrape endpoint" \
      org.opencontainers.image.source="https://github.com/ys1173/dcm-traffic-gen" \
      org.opencontainers.image.licenses="MIT"

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY dcm_traffic_gen.py .

# ── Config — all overridable at docker run with -e ────────────────────────────
ENV PORT=8000
ENV SCALE=0.01
ENV DURATION=0
ENV RATE=0
ENV BATCH=500
ENV TABLE=dcm_telemetry
ENV SEED=42

EXPOSE 8000

ENTRYPOINT ["python", "-u", "dcm_traffic_gen.py"]
