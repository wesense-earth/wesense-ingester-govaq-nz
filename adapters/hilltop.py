"""
Generic Hilltop API adapter for NZ regional council air quality data.

Hilltop is environmental monitoring software used by many NZ regional
councils. Each council runs its own Hilltop server but the API is
identical — XML-based, no authentication required.

This single adapter handles all councils: Tasman, Nelson, Marlborough,
Hawke's Bay, Gisborne, Horizons, West Coast, and Northland.

Hilltop API reference:
  SiteList:        ?Service=Hilltop&Request=SiteList&Location=LatLong
  MeasurementList: ?Service=Hilltop&Request=MeasurementList&Site={name}
  GetData:         ?Service=Hilltop&Request=GetData&Site={name}&Measurement={m}&From={date}&To={date}
"""

import logging
import re
import time
import xml.etree.ElementTree as ET
from datetime import datetime, timezone, timedelta
from urllib.parse import quote, urlencode
from zoneinfo import ZoneInfo

import requests

from adapters.base import GovAQAdapter

logger = logging.getLogger(__name__)

# NZ timezone with automatic DST handling (NZST +12 / NZDT +13)
NZ_TZ = ZoneInfo("Pacific/Auckland")

# ── Measurement name matching ────────────────────────────────────────
# Hilltop measurement names vary wildly across councils. We match the
# raw (non-averaged) measurements and map them to WeSense standard
# reading types.
#
# We SKIP anything containing "Average", "Monthly", "Yearly", "Annual",
# "LAWA", "VM", "Missing Record" — those are computed aggregates, not
# point-in-time readings.

SKIP_PATTERNS = re.compile(
    r"(Average|Monthly|Yearly|Annual|LAWA|Missing Record|\bVM\b|Exceedance|Gaps"
    r"|Moving Total|Cumulative|Hourly|Runoff|Modelling|SCADA|cosine|sine"
    r"|closed gaps|Inspection|Daily Precipitation|Precipitation \(Daily\)"
    r"|Enclosure Temp|Board Temp|Internal Temp|Partisol.*Temp"
    r"|Vacuum|Flow Pressure|Flow Volume|Flow Temp|Campbell|Software Version"
    r"|Voltage|NEMS|Dry Days|Dew Point)",
    re.IGNORECASE,
)

# Patterns matched against measurement Name. Order matters — first match wins.
# Each entry: (compiled regex, wesense_reading_type, unit)
MEASUREMENT_MAP = [
    # PM2.5 — match before PM10 to avoid "PM2.5" matching a "PM" rule
    (re.compile(r"PM\s*2\.?5", re.IGNORECASE), "pm2_5", "ug/m3"),
    # Particulate Matter 2.5 (Gisborne naming: "Particulate Matter 2.5 - 1 min interval")
    (re.compile(r"Particulate Matter 2\.5", re.IGNORECASE), "pm2_5", "ug/m3"),
    # PM10
    (re.compile(r"PM\s*10", re.IGNORECASE), "pm10", "ug/m3"),
    # Particulate Matter 10 (Gisborne naming: "Particulate Matter 10 - 1 min interval")
    (re.compile(r"Particulate Matter 10", re.IGNORECASE), "pm10", "ug/m3"),
    # Particulate Matter (generic, usually PM10)
    (re.compile(r"^Particulate Matter$", re.IGNORECASE), "pm10", "ug/m3"),
    # Nitrogen dioxide — prefer ug/m3 variants
    (re.compile(r"Nitrogen Dioxide.*ug/m3", re.IGNORECASE), "no2", "ug/m3"),
    (re.compile(r"Nitrogen Dioxide.*ppb", re.IGNORECASE), "no2", "ppb"),
    (re.compile(r"^Nitrogen Dioxide$", re.IGNORECASE), "no2", "ug/m3"),
    # Nitrogen oxide (NO)
    (re.compile(r"Nitrogen Oxide \(NO\).*ug/m3", re.IGNORECASE), "no", "ug/m3"),
    (re.compile(r"Nitrogen Oxide \(NO\).*ppb", re.IGNORECASE), "no", "ppb"),
    (re.compile(r"^Nitrogen Oxide$", re.IGNORECASE), "no", "ug/m3"),
    # NOx (total nitrogen oxides)
    (re.compile(r"Nitrogen Oxide \(NOx\).*ug/m3", re.IGNORECASE), "nox", "ug/m3"),
    (re.compile(r"Nitrogen Oxide \(NOx\).*ppb", re.IGNORECASE), "nox", "ppb"),
    # Carbon monoxide — prefer mg/m3
    (re.compile(r"Carbon Monoxide.*mg/m3", re.IGNORECASE), "co", "mg/m3"),
    (re.compile(r"Carbon Monoxide.*ppm", re.IGNORECASE), "co", "ppm"),
    (re.compile(r"^Carbon Monoxide$", re.IGNORECASE), "co", "mg/m3"),
    # Sulphur dioxide
    (re.compile(r"Sulphur Dioxide", re.IGNORECASE), "so2", "ug/m3"),
    # ── Weather measurements ─────────────────────────────────────────
    # Wind speed (match before "Wind" to avoid partial matches)
    (re.compile(r"^Wind Speed$", re.IGNORECASE), "wind_speed", "m/s"),
    (re.compile(r"^Maximum Wind Speed$", re.IGNORECASE), "wind_gust", "m/s"),
    # Wind direction
    (re.compile(r"^Wind Direction$", re.IGNORECASE), "wind_direction", "degrees"),
    (re.compile(r"^Maximum Wind Direction$", re.IGNORECASE), "wind_gust_direction", "degrees"),
    # Air temperature — various naming conventions
    (re.compile(r"^Air Temperature \(continuous\)$", re.IGNORECASE), "temperature", "C"),
    (re.compile(r"^Air Temperature \(1\.5m\)$", re.IGNORECASE), "temperature", "C"),
    (re.compile(r"^Air Temperature \(5m\)$", re.IGNORECASE), "temperature_5m", "C"),
    (re.compile(r"^Air Temperature$", re.IGNORECASE), "temperature", "C"),
    # Relative humidity
    (re.compile(r"^Relative [Hh]umidity$", re.IGNORECASE), "humidity", "%"),
    (re.compile(r"^Relative humidity \(%\)$", re.IGNORECASE), "humidity", "%"),
    # Barometric pressure
    (re.compile(r"^Barometric Pressure$", re.IGNORECASE), "pressure", "hPa"),
    # Rainfall — raw tipping bucket, not moving totals or aggregates
    (re.compile(r"^Rainfall$", re.IGNORECASE), "rainfall", "mm"),
    (re.compile(r"^Rainfall Total \(6 min\)$", re.IGNORECASE), "rainfall", "mm"),
    (re.compile(r"^Rainfall Total \(15 Min\)$", re.IGNORECASE), "rainfall", "mm"),
]

# Site name patterns that indicate air quality monitoring
AIR_SITE_PATTERNS = re.compile(
    r"(Air Quality|^AQ\b|Purple Air|\bFidas\b|\bEBAM\b|\bAir$)",
    re.IGNORECASE,
)


def _match_measurement(name: str) -> tuple[str, str] | None:
    """Match a Hilltop measurement name to (reading_type, unit), or None."""
    if SKIP_PATTERNS.search(name):
        return None
    for pattern, reading_type, unit in MEASUREMENT_MAP:
        if pattern.search(name):
            return (reading_type, unit)
    return None


class HilltopAdapter(GovAQAdapter):
    """Generic adapter for NZ regional council Hilltop servers."""

    def __init__(self, source_id: str, config: dict):
        super().__init__(source_id, config)
        self._base_url = config["hilltop_url"]
        self._site_patterns = config.get("site_patterns", [])
        self._last_timestamps: dict[str, int] = {}
        self._station_measurements: dict[str, list[dict]] = {}
        self._session = requests.Session()
        self._session.headers.update({
            "User-Agent": "WeSense-Ingester-GovAQ/1.0 (https://wesense.earth)",
            "Accept": "application/xml",
        })

    def _hilltop_get(self, params: dict) -> ET.Element | None:
        """Make a Hilltop API request and return parsed XML root."""
        params = {"Service": "Hilltop", **params}
        # Hilltop servers require %20 for spaces, not + (which requests uses).
        # Build the query string manually with quote_via=quote.
        qs = urlencode(params, quote_via=quote)
        url = f"{self._base_url}?{qs}"
        try:
            resp = self._session.get(
                url, timeout=30, allow_redirects=True,
            )
            resp.raise_for_status()
            root = ET.fromstring(resp.content)
            # Check for Hilltop error responses
            err = root.find("Error")
            if err is not None:
                logger.debug("[%s] Hilltop error: %s (params=%s)", self.source_id, err.text, params)
                return None
            return root
        except requests.RequestException as e:
            logger.warning("[%s] HTTP error: %s (params=%s)", self.source_id, e, params)
            return None
        except ET.ParseError as e:
            logger.warning("[%s] XML parse error: %s", self.source_id, e)
            return None

    def _is_air_site(self, site_name: str) -> bool:
        """Check if a site name matches air quality patterns."""
        # Use custom patterns from config if provided
        if self._site_patterns:
            for pattern in self._site_patterns:
                if re.search(pattern, site_name, re.IGNORECASE):
                    return True
            return False
        return bool(AIR_SITE_PATTERNS.search(site_name))

    def fetch_stations(self) -> list[dict]:
        """Fetch air quality stations from the Hilltop SiteList."""
        root = self._hilltop_get({
            "Request": "SiteList",
            "Location": "LatLong",
        })
        if root is None:
            return []

        stations = []
        for site in root.findall("Site"):
            name = site.get("Name", "")
            if not self._is_air_site(name):
                continue

            lat_el = site.find("Latitude")
            lon_el = site.find("Longitude")
            if lat_el is None or lon_el is None or not lat_el.text or not lon_el.text:
                logger.debug("[%s] Skipping %s — no coordinates", self.source_id, name)
                continue

            try:
                lat = float(lat_el.text)
                lon = float(lon_el.text)
            except ValueError:
                continue

            stations.append({
                "station_id": name,
                "name": name,
                "latitude": lat,
                "longitude": lon,
            })

        # Discover which measurements each station has (once per cycle)
        self._discover_measurements(stations)

        logger.info(
            "[%s] %d air quality stations found",
            self.source_id, len(stations),
        )
        return stations

    def _discover_measurements(self, stations: list[dict]) -> None:
        """Query MeasurementList for each station to find air quality parameters."""
        self._station_measurements = {}

        for station in stations:
            site_name = station["station_id"]
            root = self._hilltop_get({
                "Request": "MeasurementList",
                "Site": site_name,
            })
            if root is None:
                continue

            measurements = []
            for ds in root.findall("DataSource"):
                for meas in ds.findall("Measurement"):
                    meas_name = meas.get("Name", "")
                    # Use RequestAs if available — that's what GetData expects
                    request_as_el = meas.find("RequestAs")
                    request_as = request_as_el.text if request_as_el is not None and request_as_el.text else meas_name

                    mapping = _match_measurement(meas_name)
                    if mapping is None:
                        continue

                    reading_type, unit = mapping

                    # Extract unit from XML if available
                    units_el = meas.find("Units")
                    if units_el is not None and units_el.text:
                        xml_unit = units_el.text.strip()
                        # Normalise units — Hilltop sometimes prefixes with the chemical formula
                        if "ug/m" in xml_unit:
                            unit = "ug/m3"
                        elif "mg/m" in xml_unit:
                            unit = "mg/m3"
                        elif "ppm" in xml_unit:
                            unit = "ppm"
                        elif "ppb" in xml_unit:
                            unit = "ppb"

                    measurements.append({
                        "request_as": request_as,
                        "reading_type": reading_type,
                        "unit": unit,
                    })

            if measurements:
                self._station_measurements[site_name] = measurements
                logger.debug(
                    "[%s] %s: %d measurements — %s",
                    self.source_id, site_name, len(measurements),
                    ", ".join(m["reading_type"] for m in measurements),
                )

    def fetch_readings(self, station: dict) -> list[dict]:
        """Fetch latest readings for a single station."""
        site_name = station["station_id"]
        measurements = self._station_measurements.get(site_name, [])
        if not measurements:
            return []

        # On first poll (no prior state), look back 24h to catch infrequent stations.
        # Subsequent polls only need the last 2 hours.
        nz_now = datetime.now(NZ_TZ)
        last_ts = self._last_timestamps.get(site_name, 0)
        lookback_hours = 24 if last_ts == 0 else 2
        from_time = nz_now - timedelta(hours=lookback_hours)
        from_str = from_time.strftime("%Y-%m-%dT%H:%M:%S")
        to_str = nz_now.strftime("%Y-%m-%dT%H:%M:%S")

        readings = []

        for meas in measurements:
            root = self._hilltop_get({
                "Request": "GetData",
                "Site": site_name,
                "Measurement": meas["request_as"],
                "From": from_str,
                "To": to_str,
            })
            if root is None:
                continue

            data_el = root.find(".//Data")
            if data_el is None:
                continue

            for entry in data_el.findall("E"):
                t_el = entry.find("T")
                v_el = entry.find("I1")
                if t_el is None or v_el is None or not t_el.text or not v_el.text:
                    continue

                timestamp = self._parse_timestamp(t_el.text)
                if timestamp is None:
                    continue
                if timestamp <= last_ts:
                    continue

                try:
                    value = float(v_el.text)
                except (ValueError, TypeError):
                    continue

                # Skip negative values — instrument artefact (zero drift)
                if value < 0:
                    continue

                readings.append({
                    "timestamp": timestamp,
                    "reading_type": meas["reading_type"],
                    "value": value,
                    "unit": meas["unit"],
                })

        # Update last-seen timestamp
        if readings:
            max_ts = max(r["timestamp"] for r in readings)
            self._last_timestamps[site_name] = max_ts
            logger.debug(
                "[%s] %s: %d new readings",
                self.source_id, site_name, len(readings),
            )

        return readings

    def _parse_timestamp(self, dt_str: str) -> int | None:
        """Parse Hilltop timestamp (NZ local, no tz info) to Unix epoch UTC."""
        try:
            dt = datetime.fromisoformat(dt_str)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=NZ_TZ)
            return int(dt.timestamp())
        except (ValueError, TypeError):
            logger.debug("[%s] Failed to parse timestamp: %s", self.source_id, dt_str)
            return None

    def get_last_timestamps(self) -> dict[str, int]:
        """Return last-seen timestamps for cache persistence."""
        return dict(self._last_timestamps)

    def set_last_timestamps(self, timestamps: dict[str, int]) -> None:
        """Restore last-seen timestamps from cache."""
        self._last_timestamps = dict(timestamps)
