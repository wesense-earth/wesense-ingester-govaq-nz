#!/usr/bin/env python3
"""
WeSense Ingester — Government Air Quality, New Zealand (GovAQ-NZ)

Polls NZ regional council air quality APIs and writes reference-grade
readings to the WeSense pipeline (gateway + MQTT).

Sources loaded from config/sources.json:
  - ECan (Environment Canterbury) — custom REST API, 10-minute data
  - 7 regional councils via Hilltop API — Tasman, Nelson, Marlborough,
    Hawke's Bay, Gisborne, Horizons, West Coast

This ingester does NOT participate in the Zenoh P2P network directly.
Readings reach Zenoh via the storage gateway, which handles P2P
distribution for all ingesters.
"""

import atexit
import json
import logging
import os
import signal
import socket
import sys
import time
from datetime import datetime

from wesense_ingester import (
    DeduplicationCache,
    ReverseGeocoder,
    setup_logging,
)
from wesense_ingester.gateway.client import GatewayClient
from wesense_ingester.gateway.config import GatewayConfig
from wesense_ingester.mqtt.publisher import MQTTPublisherConfig, WeSensePublisher
from wesense_ingester.signing.keys import IngesterKeyManager, KeyConfig
from wesense_ingester.signing.signer import ReadingSigner

from adapters.ecan import ECanAdapter
from adapters.hilltop import HilltopAdapter

# ── Configuration ────────────────────────────────────────────────────
INGESTION_NODE_ID = os.getenv("INGESTION_NODE_ID", socket.gethostname())
POLL_INTERVAL = int(os.getenv("GOVAQ_POLL_INTERVAL", "600"))  # 10 minutes
STATS_INTERVAL = int(os.getenv("STATS_INTERVAL", "60"))
# Calibrated sources — only set calibration_status for sources we know are calibrated
CALIBRATED_SOURCES = {"ecan"}

# ── Adapter registry ─────────────────────────────────────────────────
ADAPTER_CLASSES = {
    "ecan": ECanAdapter,
    "hilltop": HilltopAdapter,
}


def load_sources_config(config_file: str = "config/sources.json") -> dict:
    """Load source configs from JSON file."""
    config_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), config_file)
    try:
        with open(config_path) as f:
            return json.load(f)
    except FileNotFoundError:
        print(f"ERROR: Configuration file not found at {config_path}")
        sys.exit(1)
    except json.JSONDecodeError as e:
        print(f"ERROR: Invalid JSON in {config_file}: {e}")
        sys.exit(1)


class GovAQIngester:
    """
    Government Air Quality ingester.

    Polls government APIs on a configurable interval and writes
    reference-grade readings through the WeSense core pipeline.
    """

    def __init__(self):
        # Logging
        self.logger = setup_logging("govaq_nz_ingester")

        # Core components
        self.dedup = DeduplicationCache()
        self.geocoder = ReverseGeocoder()

        # Storage gateway
        self.gateway_client = None
        try:
            self.gateway_client = GatewayClient(config=GatewayConfig.from_env())
        except Exception as e:
            print(f"Failed to create gateway client: {e}")
            print("  Continuing without storage (MQTT only)")

        # MQTT publisher for decoded output
        mqtt_config = MQTTPublisherConfig(
            broker=os.getenv("WESENSE_OUTPUT_BROKER", os.getenv("MQTT_BROKER", "localhost")),
            port=int(os.getenv("WESENSE_OUTPUT_PORT", os.getenv("MQTT_PORT", "1883"))),
            username=os.getenv("WESENSE_OUTPUT_USERNAME", os.getenv("MQTT_USERNAME")),
            password=os.getenv("WESENSE_OUTPUT_PASSWORD", os.getenv("MQTT_PASSWORD")),
            client_id="govaq_nz_publisher",
        )
        self.publisher = WeSensePublisher(config=mqtt_config)
        self.publisher.connect()

        # Ed25519 signing
        key_config = KeyConfig.from_env()
        self.key_manager = IngesterKeyManager(config=key_config)
        self.key_manager.load_or_generate()
        self.signer = ReadingSigner(self.key_manager)
        self.logger.info(
            "Ingester ID: %s (key version %d)",
            self.key_manager.ingester_id, self.key_manager.key_version,
        )

        # Load sources and create adapters
        self.sources = load_sources_config()
        self.adapters = {}
        for source_id, source_config in self.sources.items():
            if not source_config.get("enabled", False):
                continue
            adapter_name = source_config.get("adapter", source_id)
            adapter_class = ADAPTER_CLASSES.get(adapter_name)
            if not adapter_class:
                self.logger.error("Unknown adapter '%s' for source '%s'", adapter_name, source_id)
                continue
            self.adapters[source_id] = adapter_class(source_id, source_config)
            self.logger.info("Loaded adapter: %s (%s)", source_id, source_config.get("name", ""))

        # Stats
        self.stats = {
            source_id: {
                "polls": 0,
                "readings_fetched": 0,
                "readings_written": 0,
                "stations_polled": 0,
                "start_time": datetime.now(),
            }
            for source_id in self.adapters
        }

        # Restore adapter state from cache
        self._load_adapter_state()

        # Shutdown flag
        self._running = True

    # ── State persistence ────────────────────────────────────────────

    def _load_adapter_state(self) -> None:
        """Restore adapter state (last timestamps) from cache."""
        for source_id, adapter in self.adapters.items():
            cache_file = f"cache/govaq_nz_{source_id}_state.json"
            try:
                if os.path.exists(cache_file):
                    with open(cache_file) as f:
                        state = json.load(f)
                    if hasattr(adapter, "set_last_timestamps"):
                        adapter.set_last_timestamps(state.get("last_timestamps", {}))
                    saved_at = state.get("saved_at", 0)
                    age = int(time.time()) - saved_at
                    self.logger.info(
                        "Restored state for %s (age: %ds, %d stations tracked)",
                        source_id, age, len(state.get("last_timestamps", {})),
                    )
            except Exception as e:
                self.logger.warning("Failed to load state for %s: %s", source_id, e)

    def _save_adapter_state(self) -> None:
        """Persist adapter state to cache."""
        os.makedirs("cache", exist_ok=True)
        for source_id, adapter in self.adapters.items():
            cache_file = f"cache/govaq_nz_{source_id}_state.json"
            try:
                state = {"saved_at": int(time.time())}
                if hasattr(adapter, "get_last_timestamps"):
                    state["last_timestamps"] = adapter.get_last_timestamps()
                with open(cache_file, "w") as f:
                    json.dump(state, f, indent=2)
            except Exception as e:
                self.logger.warning("Failed to save state for %s: %s", source_id, e)

    # ── Core processing pipeline ─────────────────────────────────────

    def process_reading(
        self,
        source_id: str,
        station: dict,
        reading: dict,
    ) -> None:
        """
        Process a single reading: dedup -> geocode -> sign -> gateway + MQTT.
        """
        device_id = f"govaq_nz_{source_id}_{station['station_id']}"
        reading_type = reading["reading_type"]
        timestamp = reading["timestamp"]
        value = reading["value"]
        unit = reading["unit"]

        # Friendly name from sources.json config
        source_name = self.sources.get(source_id, {}).get("name", source_id)

        # Dedup check
        if self.dedup.is_duplicate(device_id, reading_type, timestamp):
            return

        # Geocode
        geo = self.geocoder.reverse_geocode(station["latitude"], station["longitude"])
        country_code = geo["geo_country"] if geo else ""
        subdivision_code = geo["geo_subdivision"] if geo else ""

        # Publish to MQTT
        mqtt_dict = {
            "timestamp": timestamp,
            "device_id": device_id,
            "name": station["name"],
            "latitude": station["latitude"],
            "longitude": station["longitude"],
            "altitude": None,
            "country": country_code,
            "subdivision": subdivision_code,
            "data_source": source_id,
            "geo_country": country_code,
            "geo_subdivision": subdivision_code,
            "reading_type": reading_type,
            "value": value,
            "unit": unit,
            "board_model": "",
        }
        self.publisher.publish_reading(mqtt_dict)

        # Sign the reading
        signing_dict = {
            "device_id": device_id,
            "data_source": source_id,
            "timestamp": timestamp,
            "reading_type": reading_type,
            "value": value,
            "latitude": station["latitude"],
            "longitude": station["longitude"],
            "transport_type": "",
        }
        signed = self.signer.sign(json.dumps(signing_dict, sort_keys=True).encode())

        # Write to storage gateway
        if self.gateway_client:
            self.gateway_client.add({
                "timestamp": timestamp,
                "device_id": device_id,
                "data_source": source_id,
                "data_source_name": source_name,
                "network_source": "api",
                "ingestion_node_id": INGESTION_NODE_ID,
                "reading_type": reading_type,
                "value": float(value),
                "unit": unit,
                "latitude": float(station["latitude"]),
                "longitude": float(station["longitude"]),
                "altitude": None,
                "geo_country": country_code,
                "geo_subdivision": subdivision_code,
                "board_model": "",
                "sensor_model": "",
                "calibration_status": "calibrated" if source_id in CALIBRATED_SOURCES else "",
                "deployment_type": "OUTDOOR",
                "deployment_type_source": "manual",
                "transport_type": "",
                "location_source": "manual",
                "node_name": station["name"],
                "signature": signed.signature.hex(),
                "ingester_id": self.key_manager.ingester_id,
                "key_version": self.key_manager.key_version,
            })

        self.stats[source_id]["readings_written"] += 1

    # ── Polling loop ─────────────────────────────────────────────────

    def poll_all_sources(self) -> None:
        """Poll all enabled sources for new readings."""
        for source_id, adapter in self.adapters.items():
            try:
                stations = adapter.fetch_stations()
                self.stats[source_id]["stations_polled"] = len(stations)

                source_readings = 0
                for station in stations:
                    readings = adapter.fetch_readings(station)
                    for reading in readings:
                        self.process_reading(source_id, station, reading)
                        source_readings += 1

                self.stats[source_id]["polls"] += 1
                self.stats[source_id]["readings_fetched"] += source_readings

                if source_readings > 0:
                    self.logger.info(
                        "Poll complete: %s — %d new readings from %d stations",
                        source_id, source_readings, len(stations),
                    )

            except Exception as e:
                self.logger.error("Error polling source %s: %s", source_id, e, exc_info=True)

        # Save state after each poll cycle
        self._save_adapter_state()

    # ── Stats ────────────────────────────────────────────────────────

    def print_stats(self) -> None:
        """Print statistics for all sources."""
        print("\n" + "=" * 70)
        for source_id, data in self.stats.items():
            elapsed = (datetime.now() - data["start_time"]).total_seconds()
            rate = data["readings_written"] / (elapsed / 3600) if elapsed > 0 else 0
            print(
                f"[{source_id:8}] Polls: {data['polls']:4} | "
                f"Stations: {data['stations_polled']:3} | "
                f"Fetched: {data['readings_fetched']:6} | "
                f"Written: {data['readings_written']:6} | "
                f"Rate: {rate:.0f}/hr"
            )

        dedup_stats = self.dedup.get_stats()
        storage_stats = (
            self.gateway_client.get_stats()
            if self.gateway_client
            else {"total_written": 0}
        )
        total = dedup_stats["duplicates_blocked"] + dedup_stats["unique_processed"]
        block_rate = dedup_stats["duplicates_blocked"] / total * 100 if total > 0 else 0
        print(
            f"\nDEDUP: Total: {total} | Dups: {dedup_stats['duplicates_blocked']} "
            f"({block_rate:.1f}%) | Unique: {dedup_stats['unique_processed']} | "
            f"Writes: {storage_stats['total_written']} | Cache: {dedup_stats['cache_size']}"
        )
        print("=" * 70)

    # ── Lifecycle ────────────────────────────────────────────────────

    def shutdown(self, signum=None, frame=None) -> None:
        """Graceful shutdown: save state, flush buffers, disconnect."""
        if not self._running:
            return
        self._running = False

        print("\n" + "=" * 60)
        print("Shutting down gracefully...")

        self._save_adapter_state()

        if self.gateway_client:
            print("  Flushing gateway buffer...")
            self.gateway_client.close()

        self.publisher.close()
        print("Shutdown complete.")
        print("=" * 60)

    def run(self) -> None:
        """Main entry point: poll sources on interval."""
        signal.signal(signal.SIGINT, self.shutdown)
        signal.signal(signal.SIGTERM, self.shutdown)
        atexit.register(self.shutdown)

        print("=" * 60)
        print("Government Air Quality Ingester — New Zealand")
        print(f"Poll interval: {POLL_INTERVAL}s")
        print(f"Sources: {', '.join(self.adapters.keys())}")
        print("=" * 60)

        # Initial poll immediately
        self.poll_all_sources()

        last_poll = time.time()
        last_stats = time.time()

        while self._running:
            now = time.time()

            if now - last_poll >= POLL_INTERVAL:
                self.poll_all_sources()
                last_poll = now

            if now - last_stats >= STATS_INTERVAL:
                self.print_stats()
                last_stats = now

            time.sleep(1)


def main():
    ingester = GovAQIngester()
    ingester.run()


if __name__ == "__main__":
    main()
