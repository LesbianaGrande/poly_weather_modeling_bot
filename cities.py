"""
cities.py — City lookup database.

Maps lowercase city name variants to:
  lat, lon       — coordinates for Open-Meteo
  mos_station    — ICAO station code for Iowa Mesonet MOS API
  timezone       — IANA timezone string
  display_name   — canonical human-readable name
"""

import logging

logger = logging.getLogger(__name__)

CITIES: dict[str, dict] = {
    "new york": {
        "display_name": "New York City",
        "lat": 40.7128, "lon": -74.0060,
        "mos_station": "KNYC",
        "timezone": "America/New_York",
    },
    "new york city": {
        "display_name": "New York City",
        "lat": 40.7128, "lon": -74.0060,
        "mos_station": "KNYC",
        "timezone": "America/New_York",
    },
    "nyc": {
        "display_name": "New York City",
        "lat": 40.7128, "lon": -74.0060,
        "mos_station": "KNYC",
        "timezone": "America/New_York",
    },
    "los angeles": {
        "display_name": "Los Angeles",
        "lat": 34.0522, "lon": -118.2437,
        "mos_station": "KLAX",
        "timezone": "America/Los_Angeles",
    },
    "la": {
        "display_name": "Los Angeles",
        "lat": 34.0522, "lon": -118.2437,
        "mos_station": "KLAX",
        "timezone": "America/Los_Angeles",
    },
    "chicago": {
        "display_name": "Chicago",
        "lat": 41.8781, "lon": -87.6298,
        "mos_station": "KORD",
        "timezone": "America/Chicago",
    },
    "miami": {
        "display_name": "Miami",
        "lat": 25.7617, "lon": -80.1918,
        "mos_station": "KMIA",
        "timezone": "America/New_York",
    },
    "dallas": {
        "display_name": "Dallas",
        "lat": 32.7767, "lon": -96.7970,
        "mos_station": "KDFW",
        "timezone": "America/Chicago",
    },
    "phoenix": {
        "display_name": "Phoenix",
        "lat": 33.4484, "lon": -112.0740,
        "mos_station": "KPHX",
        "timezone": "America/Phoenix",
    },
    "seattle": {
        "display_name": "Seattle",
        "lat": 47.6062, "lon": -122.3321,
        "mos_station": "KSEA",
        "timezone": "America/Los_Angeles",
    },
    "boston": {
        "display_name": "Boston",
        "lat": 42.3601, "lon": -71.0589,
        "mos_station": "KBOS",
        "timezone": "America/New_York",
    },
    "atlanta": {
        "display_name": "Atlanta",
        "lat": 33.7490, "lon": -84.3880,
        "mos_station": "KATL",
        "timezone": "America/New_York",
    },
    "denver": {
        "display_name": "Denver",
        "lat": 39.7392, "lon": -104.9903,
        "mos_station": "KDEN",
        "timezone": "America/Denver",
    },
    "san francisco": {
        "display_name": "San Francisco",
        "lat": 37.7749, "lon": -122.4194,
        "mos_station": "KSFO",
        "timezone": "America/Los_Angeles",
    },
    "sf": {
        "display_name": "San Francisco",
        "lat": 37.7749, "lon": -122.4194,
        "mos_station": "KSFO",
        "timezone": "America/Los_Angeles",
    },
    "houston": {
        "display_name": "Houston",
        "lat": 29.7604, "lon": -95.3698,
        "mos_station": "KHOU",
        "timezone": "America/Chicago",
    },
    "las vegas": {
        "display_name": "Las Vegas",
        "lat": 36.1699, "lon": -115.1398,
        "mos_station": "KLAS",
        "timezone": "America/Los_Angeles",
    },
    "washington": {
        "display_name": "Washington DC",
        "lat": 38.9072, "lon": -77.0369,
        "mos_station": "KDCA",
        "timezone": "America/New_York",
    },
    "washington dc": {
        "display_name": "Washington DC",
        "lat": 38.9072, "lon": -77.0369,
        "mos_station": "KDCA",
        "timezone": "America/New_York",
    },
    "dc": {
        "display_name": "Washington DC",
        "lat": 38.9072, "lon": -77.0369,
        "mos_station": "KDCA",
        "timezone": "America/New_York",
    },
    "minneapolis": {
        "display_name": "Minneapolis",
        "lat": 44.9778, "lon": -93.2650,
        "mos_station": "KMSP",
        "timezone": "America/Chicago",
    },
    "detroit": {
        "display_name": "Detroit",
        "lat": 42.3314, "lon": -83.0458,
        "mos_station": "KDTW",
        "timezone": "America/Detroit",
    },
    "portland": {
        "display_name": "Portland",
        "lat": 45.5051, "lon": -122.6750,
        "mos_station": "KPDX",
        "timezone": "America/Los_Angeles",
    },
}


def lookup_city(name: str) -> dict | None:
    """
    Look up a city by name. Case-insensitive.
    Tries exact match first, then substring matching.
    Returns city info dict or None.
    """
    name_lower = name.lower().strip()
    logger.debug(f"City lookup: '{name_lower}'")

    # 1) Exact match
    if name_lower in CITIES:
        city = CITIES[name_lower]
        logger.debug(f"  → Exact match: {city['display_name']}")
        return city

    # 2) City key is substring of query (e.g. "new york" in "new york city, ny")
    for key, info in CITIES.items():
        if key in name_lower:
            logger.debug(f"  → Substring match '{key}': {info['display_name']}")
            return info

    # 3) Query is substring of city key
    for key, info in CITIES.items():
        if name_lower in key:
            logger.debug(f"  → Reverse substring match '{key}': {info['display_name']}")
            return info

    logger.debug(f"  → No match found for '{name_lower}'")
    return None
