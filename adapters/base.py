"""
Abstract base class for government air quality source adapters.

Each adapter fetches data from a specific government agency's API,
returning standardised reading dicts that the main ingester processes
through the core pipeline (dedup -> geocode -> gateway + MQTT + Zenoh).
"""

from abc import ABC, abstractmethod


class GovAQAdapter(ABC):
    """Base class for all government air quality adapters."""

    def __init__(self, source_id: str, config: dict):
        self.source_id = source_id
        self.config = config

    @abstractmethod
    def fetch_stations(self) -> list[dict]:
        """
        Return list of station dicts, each containing at minimum:
            station_id: str
            name: str
            latitude: float
            longitude: float
        """

    @abstractmethod
    def fetch_readings(self, station: dict) -> list[dict]:
        """
        Fetch latest readings for a single station.

        Return list of reading dicts, each containing:
            timestamp: int      (Unix epoch, UTC)
            reading_type: str   (WeSense standard: "pm10", "pm2_5", etc.)
            value: float
            unit: str           (e.g. "ug/m3", "mg/m3")
        """

    def get_network_source(self) -> str:
        """Return the network_source identifier (e.g. 'ecan')."""
        return self.source_id
