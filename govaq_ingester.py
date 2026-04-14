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

import json
import os
import sys
import time
from datetime import datetime

from wesense_ingester import ReadingPipeline, Shutdown, setup_logging
from wesense_ingester.mqtt.publisher import MQTTPublisherConfig

from adapters.ecan import ECanAdapter
from adapters.hilltop import HilltopAdapter

# ── Configuration ────────────────────────────────────────────────────
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

        # Unified pipeline: dedup → geocode → sign → MQTT + gateway
        mqtt_config = MQTTPublisherConfig(
            broker=os.getenv("WESENSE_OUTPUT_BROKER", os.getenv("MQTT_BROKER", "localhost")),
            port=int(os.getenv("WESENSE_OUTPUT_PORT", os.getenv("MQTT_PORT", "1883"))),
            username=os.getenv("WESENSE_OUTPUT_USERNAME", os.getenv("MQTT_USERNAME")),
            password=os.getenv("WESENSE_OUTPUT_PASSWORD", os.getenv("MQTT_PASSWORD")),
            client_id="govaq_nz_publisher",
        )
        self.pipeline = ReadingPipeline(name="govaq_nz", mqtt_config=mqtt_config)

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
        Process a single reading through the pipeline.
        """
        device_id = f"govaq_nz_{source_id}_{station['station_id']}"
        source_name = self.sources.get(source_id, {}).get("name", source_id)

        processed = self.pipeline.process({
            "device_id": device_id,
            "timestamp": reading["timestamp"],
            "reading_type": reading["reading_type"],
            "value": reading["value"],
            "unit": reading["unit"],
            "latitude": station["latitude"],
            "longitude": station["longitude"],
            "data_source": source_id,
            "data_source_name": source_name,
            "sensor_transport": "",
            "deployment_type": "OUTDOOR",
            "deployment_type_source": "manual",
            "location_source": "manual",
            "node_name": station["name"],
            "calibration_status": "calibrated" if source_id in CALIBRATED_SOURCES else "",
            "data_license": "NZGOAL",
            "network_source": "api",
        })

        if processed:
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

        pipeline_stats = self.pipeline.get_stats()
        dedup_stats = pipeline_stats.get("dedup", {})
        gateway_stats = pipeline_stats.get("gateway", {"total_written": 0})
        total = dedup_stats.get("duplicates_blocked", 0) + dedup_stats.get("unique_processed", 0)
        block_rate = dedup_stats.get("duplicates_blocked", 0) / total * 100 if total > 0 else 0
        print(
            f"\nDEDUP: Total: {total} | Dups: {dedup_stats.get('duplicates_blocked', 0)} "
            f"({block_rate:.1f}%) | Unique: {dedup_stats.get('unique_processed', 0)} | "
            f"Writes: {gateway_stats.get('total_written', 0)} | Cache: {dedup_stats.get('cache_size', 0)}"
        )
        print("=" * 70)

    # ── Lifecycle ────────────────────────────────────────────────────

    def shutdown(self) -> None:
        """Graceful shutdown: save state, flush buffers, disconnect."""
        print("\n" + "=" * 60)
        print("Shutting down gracefully...")

        self._save_adapter_state()

        print("  Closing pipeline...")
        self.pipeline.close()
        print("Shutdown complete.")
        print("=" * 60)

    def run(self) -> None:
        """Main entry point: poll sources on interval."""
        shutdown = Shutdown(name="govaq_nz")

        print("=" * 60)
        print("Government Air Quality Ingester — New Zealand")
        print(f"Poll interval: {POLL_INTERVAL}s")
        print(f"Sources: {', '.join(self.adapters.keys())}")
        print("=" * 60)

        self.logger.info("Starting (poll interval: %ds)", POLL_INTERVAL)

        # Initial poll immediately
        try:
            self.poll_all_sources()
        except Exception as e:
            self.logger.error("Poll cycle failed: %s", e, exc_info=True)

        last_poll = time.time()
        last_stats = time.time()

        # Tick at the shorter of the two intervals so stats remain responsive.
        tick = min(STATS_INTERVAL, POLL_INTERVAL)

        while not shutdown.requested:
            if shutdown.sleep(tick):
                break

            now = time.time()

            if now - last_poll >= POLL_INTERVAL:
                try:
                    self.poll_all_sources()
                except Exception as e:
                    self.logger.error("Poll cycle failed: %s", e, exc_info=True)
                last_poll = now

            if now - last_stats >= STATS_INTERVAL:
                self.print_stats()
                last_stats = now

        self.shutdown()


def main():
    ingester = GovAQIngester()
    ingester.run()


if __name__ == "__main__":
    main()
