"""
Environment Canterbury (ECan) air quality adapter.

Fetches 10-minute air quality readings from ECan's Open Data Portal REST API.
Covers ~18 monitoring stations across the Canterbury region of New Zealand.

API docs: https://data.ecan.govt.nz/
No authentication required. JSON format.
"""

import logging
import re
import time
from datetime import datetime, timezone, timedelta
from zoneinfo import ZoneInfo

import requests

from adapters.base import GovAQAdapter

logger = logging.getLogger(__name__)

# ── Station coordinates ──────────────────────────────────────────────
# The ECan station list API does not return coordinates.
# These are sourced from OpenAQ's canterbury.js adapter and official records.
# Stations not in this table will be logged as warnings and skipped.

STATION_COORDS = {
    "1":  {"lat": -43.511257, "lon": 172.633700, "name": "St Albans"},
    "2":  {"lat": -43.530356, "lon": 172.589048, "name": "Riccarton Road"},
    "3":  {"lat": -43.557532, "lon": 172.681343, "name": "Woolston"},
    "4":  {"lat": -43.384643, "lon": 172.652000, "name": "Kaiapoi"},
    "5":  {"lat": -43.307439, "lon": 172.594745, "name": "Rangiora"},
    "7":  {"lat": -43.912238, "lon": 171.755200, "name": "Ashburton"},
    "9":  {"lat": -44.100188, "lon": 171.241443, "name": "Geraldine"},
    "10": {"lat": -44.404486, "lon": 171.249643, "name": "Timaru Anzac Square"},
    "11": {"lat": -44.356735, "lon": 171.236300, "name": "Washdyke Flat Road"},
    "12": {"lat": -44.735729, "lon": 171.049900, "name": "Waimate Stadium"},
    "36": {"lat": -43.492848, "lon": 172.593100, "name": "Burnside"},
    "54": {"lat": -44.399174, "lon": 171.246200, "name": "Timaru Grey Rd"},
    "64": {"lat": -44.732595, "lon": 171.049778, "name": "Waimate Kennedy"},
    "77": {"lat": -44.356199, "lon": 171.242334, "name": "Washdyke Alpine"},
    "87": {"lat": -43.508568, "lon": 172.635835, "name": "St Albans EP"},
}

# ── Field name mapping ───────────────────────────────────────────────
# ECan returns XML-encoded field names like "PM10_x0020__x0028_ug_x002F_m3_x0029_"
# We decode the XML encoding then match against known prefixes.

FIELD_MAP = {
    "PM10":             ("pm10", "ug/m3"),
    "PM2.5":            ("pm2_5", "ug/m3"),
    "NO2":              ("no2", "ug/m3"),
    "NO":               ("no", "ug/m3"),
    "CO":               ("co", "mg/m3"),
    "SO2":              ("so2", "ug/m3"),
    "Temperature 2m":   ("temperature", "C"),
    "Temperature 6m":   ("temperature_6m", "C"),
    "Wind speed":       ("wind_speed", "m/s"),
    "Wind direction":   ("wind_direction", "degrees"),
    "Wind maximum":     ("wind_gust", "m/s"),
}

# XML encoding replacements
XML_DECODE = {
    "_x0020_": " ",
    "_x0028_": "(",
    "_x0029_": ")",
    "_x002F_": "/",
    "_x002C_": ",",
}

# NZ timezone with automatic DST handling (NZST +12 / NZDT +13)
NZ_TZ = ZoneInfo("Pacific/Auckland")

# Max age before a station is considered inactive
STATION_INACTIVE_DAYS = 30


def _decode_xml_field(field_name: str) -> str:
    """Decode XML-encoded field names from ECan API."""
    result = field_name
    for encoded, decoded in XML_DECODE.items():
        result = result.replace(encoded, decoded)
    return result


def _match_reading_type(decoded_field: str) -> tuple[str, str] | None:
    """Match a decoded field name to a (reading_type, unit) pair."""
    for prefix, mapping in FIELD_MAP.items():
        if decoded_field.startswith(prefix):
            return mapping
    return None


class ECanAdapter(GovAQAdapter):
    """Adapter for Environment Canterbury air quality data."""

    def __init__(self, source_id: str, config: dict):
        super().__init__(source_id, config)
        self._stations_url = config["stations_url"]
        self._data_url_template = config["data_url_template"]
        self._last_timestamps: dict[str, int] = {}
        self._session = requests.Session()
        self._session.headers.update({
            "User-Agent": "WeSense-Ingester-GovAQ/1.0 (https://wesense.earth)",
            "Accept": "application/json",
        })

    def fetch_stations(self) -> list[dict]:
        """Fetch active monitoring stations from ECan API."""
        try:
            resp = self._session.get(self._stations_url, timeout=30)
            resp.raise_for_status()
            data = resp.json()
        except Exception as e:
            logger.error("Failed to fetch ECan station list: %s", e)
            return []

        items = data.get("data", {}).get("item", [])
        if isinstance(items, dict):
            items = [items]

        stations = []
        for item in items:
            site_no = str(item.get("SiteNo", ""))
            site_name = item.get("SiteName", "")

            coords = STATION_COORDS.get(site_no)
            if not coords:
                logger.warning(
                    "No coordinates for ECan station %s (%s) — add to STATION_COORDS",
                    site_no, site_name,
                )
                continue

            stations.append({
                "station_id": site_no,
                "name": coords["name"],
                "latitude": coords["lat"],
                "longitude": coords["lon"],
            })

        logger.info("ECan: %d stations with coordinates (of %d total)", len(stations), len(items))
        return stations

    def fetch_readings(self, station: dict) -> list[dict]:
        """Fetch 10-minute readings for a single station."""
        station_id = station["station_id"]

        # Use NZ local date for the query
        nz_now = datetime.now(NZ_TZ)
        today_str = nz_now.strftime("%d/%m/%Y")

        # Around midnight, also fetch yesterday to avoid missing boundary data
        dates_to_fetch = [today_str]
        if nz_now.hour == 0 and nz_now.minute < 15:
            yesterday = nz_now - timedelta(days=1)
            dates_to_fetch.insert(0, yesterday.strftime("%d/%m/%Y"))

        readings = []
        last_ts = self._last_timestamps.get(station_id, 0)

        for date_str in dates_to_fetch:
            url = self._data_url_template.format(
                site_id=station_id,
                date=date_str,
            )

            try:
                resp = self._session.get(url, timeout=30)
                resp.raise_for_status()
                data = resp.json()
            except Exception as e:
                logger.warning("Failed to fetch ECan data for station %s: %s", station_id, e)
                continue

            items = data.get("data", {}).get("item", [])
            if isinstance(items, dict):
                # Single item or nodata response
                if "nodata" in items:
                    continue
                items = [items]

            for item in items:
                timestamp = self._parse_timestamp(item.get("DateTime"))
                if timestamp is None:
                    continue
                if timestamp <= last_ts:
                    continue

                for field_name, field_value in item.items():
                    if field_name == "DateTime":
                        continue
                    if field_value is None:
                        continue

                    decoded = _decode_xml_field(field_name)
                    mapping = _match_reading_type(decoded)
                    if mapping is None:
                        continue

                    reading_type, unit = mapping
                    try:
                        value = float(field_value)
                    except (ValueError, TypeError):
                        continue

                    readings.append({
                        "timestamp": timestamp,
                        "reading_type": reading_type,
                        "value": value,
                        "unit": unit,
                    })

        # Update last-seen timestamp for this station
        if readings:
            max_ts = max(r["timestamp"] for r in readings)
            self._last_timestamps[station_id] = max_ts
            logger.debug(
                "ECan station %s (%s): %d new readings",
                station_id, station["name"], len(readings),
            )

        return readings

    def _parse_timestamp(self, dt_str: str | None) -> int | None:
        """Parse ECan ISO 8601 timestamp to Unix epoch (UTC)."""
        if not dt_str:
            return None
        try:
            dt = datetime.fromisoformat(dt_str)
            if dt.tzinfo is None:
                # Assume NZ time if no timezone
                dt = dt.replace(tzinfo=NZ_TZ)
            return int(dt.timestamp())
        except (ValueError, TypeError):
            logger.debug("Failed to parse ECan timestamp: %s", dt_str)
            return None

    def get_last_timestamps(self) -> dict[str, int]:
        """Return last-seen timestamps for cache persistence."""
        return dict(self._last_timestamps)

    def set_last_timestamps(self, timestamps: dict[str, int]) -> None:
        """Restore last-seen timestamps from cache."""
        self._last_timestamps = dict(timestamps)
