# WeSense Ingester - Government Air Quality (GovAQ)
# Build: docker build -t wesense-ingester-govaq-nz .
#
# Polls government air quality APIs for reference-grade readings.
# Configure sources in config/sources.json.
#
# Expects wesense-ingester-core to be available at ../wesense-ingester-core
# when building with docker-compose (which sets the build context).

FROM python:3.11-slim

WORKDIR /app

# Bust cache when ingester-core or app code changes
ARG CACHE_BUST=1

COPY wesense-ingester-core/ /tmp/wesense-ingester-core/
COPY wesense-ingester-govaq-nz/requirements-docker.txt .

# Install gcc, build all pip packages, then remove gcc in one layer
# Install core without [p2p] extra — this ingester does not participate
# in the Zenoh P2P network directly. Readings reach P2P consumers via
# the storage gateway which handles Zenoh distribution.
RUN apt-get update && \
    apt-get install -y --no-install-recommends gcc && \
    pip install --no-cache-dir /tmp/wesense-ingester-core && \
    pip install --no-cache-dir -r requirements-docker.txt && \
    apt-get purge -y --auto-remove gcc && \
    rm -rf /var/lib/apt/lists/* /tmp/wesense-ingester-core

# Copy application code and default config
COPY wesense-ingester-govaq-nz/govaq_ingester.py .
COPY wesense-ingester-govaq-nz/adapters/ ./adapters/
COPY wesense-ingester-govaq-nz/config/ ./config/

COPY wesense-ingester-govaq-nz/entrypoint.sh /app/entrypoint.sh
RUN chmod +x /app/entrypoint.sh

# Create directories for cache, logs, and config
RUN mkdir -p /app/cache /app/logs /app/config

ENV TZ=UTC

ENTRYPOINT ["/app/entrypoint.sh"]
