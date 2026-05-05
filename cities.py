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
    # ── Additional US cities ──────────────────────────────────────────────────
    "atlanta": {
        "display_name": "Atlanta",
        "lat": 33.7490, "lon": -84.3880,
        "mos_station": "KATL",
        "timezone": "America/New_York",
    },
    "phoenix": {
        "display_name": "Phoenix",
        "lat": 33.4484, "lon": -112.0740,
        "mos_station": "KPHX",
        "timezone": "America/Phoenix",
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
    "philadelphia": {
        "display_name": "Philadelphia",
        "lat": 39.9526, "lon": -75.1652,
        "mos_station": "KPHL",
        "timezone": "America/New_York",
    },
    "philly": {
        "display_name": "Philadelphia",
        "lat": 39.9526, "lon": -75.1652,
        "mos_station": "KPHL",
        "timezone": "America/New_York",
    },
    "nashville": {
        "display_name": "Nashville",
        "lat": 36.1627, "lon": -86.7816,
        "mos_station": "KBNA",
        "timezone": "America/Chicago",
    },
    "denver": {
        "display_name": "Denver",
        "lat": 39.7392, "lon": -104.9903,
        "mos_station": "KDEN",
        "timezone": "America/Denver",
    },
    "san antonio": {
        "display_name": "San Antonio",
        "lat": 29.4241, "lon": -98.4936,
        "mos_station": "KSAT",
        "timezone": "America/Chicago",
    },
    "san diego": {
        "display_name": "San Diego",
        "lat": 32.7157, "lon": -117.1611,
        "mos_station": "KSAN",
        "timezone": "America/Los_Angeles",
    },
    "orlando": {
        "display_name": "Orlando",
        "lat": 28.5383, "lon": -81.3792,
        "mos_station": "KMCO",
        "timezone": "America/New_York",
    },
    "charlotte": {
        "display_name": "Charlotte",
        "lat": 35.2271, "lon": -80.8431,
        "mos_station": "KCLT",
        "timezone": "America/New_York",
    },
    "indianapolis": {
        "display_name": "Indianapolis",
        "lat": 39.7684, "lon": -86.1581,
        "mos_station": "KIND",
        "timezone": "America/Indiana/Indianapolis",
    },
    "columbus": {
        "display_name": "Columbus",
        "lat": 39.9612, "lon": -82.9988,
        "mos_station": "KCMH",
        "timezone": "America/New_York",
    },
    "austin": {
        "display_name": "Austin",
        "lat": 30.2672, "lon": -97.7431,
        "mos_station": "KAUS",
        "timezone": "America/Chicago",
    },
    "memphis": {
        "display_name": "Memphis",
        "lat": 35.1495, "lon": -90.0490,
        "mos_station": "KMEM",
        "timezone": "America/Chicago",
    },
    "baltimore": {
        "display_name": "Baltimore",
        "lat": 39.2904, "lon": -76.6122,
        "mos_station": "KBWI",
        "timezone": "America/New_York",
    },
    "kansas city": {
        "display_name": "Kansas City",
        "lat": 39.0997, "lon": -94.5786,
        "mos_station": "KMCI",
        "timezone": "America/Chicago",
    },
    "salt lake city": {
        "display_name": "Salt Lake City",
        "lat": 40.7608, "lon": -111.8910,
        "mos_station": "KSLC",
        "timezone": "America/Denver",
    },
    "raleigh": {
        "display_name": "Raleigh",
        "lat": 35.7796, "lon": -78.6382,
        "mos_station": "KRDU",
        "timezone": "America/New_York",
    },
    "new orleans": {
        "display_name": "New Orleans",
        "lat": 29.9511, "lon": -90.0715,
        "mos_station": "KMSY",
        "timezone": "America/Chicago",
    },
    "pittsburgh": {
        "display_name": "Pittsburgh",
        "lat": 40.4406, "lon": -79.9959,
        "mos_station": "KPIT",
        "timezone": "America/New_York",
    },
    "cincinnati": {
        "display_name": "Cincinnati",
        "lat": 39.1031, "lon": -84.5120,
        "mos_station": "KCVG",
        "timezone": "America/New_York",
    },
    "cleveland": {
        "display_name": "Cleveland",
        "lat": 41.4993, "lon": -81.6944,
        "mos_station": "KCLE",
        "timezone": "America/New_York",
    },
    "richmond": {
        "display_name": "Richmond",
        "lat": 37.5407, "lon": -77.4360,
        "mos_station": "KRIC",
        "timezone": "America/New_York",
    },
    "louisville": {
        "display_name": "Louisville",
        "lat": 38.2527, "lon": -85.7585,
        "mos_station": "KSDF",
        "timezone": "America/Kentucky/Louisville",
    },
    "albuquerque": {
        "display_name": "Albuquerque",
        "lat": 35.0844, "lon": -106.6504,
        "mos_station": "KABQ",
        "timezone": "America/Denver",
    },
    "tucson": {
        "display_name": "Tucson",
        "lat": 32.2226, "lon": -110.9747,
        "mos_station": "KTUS",
        "timezone": "America/Phoenix",
    },
    "oklahoma city": {
        "display_name": "Oklahoma City",
        "lat": 35.4676, "lon": -97.5164,
        "mos_station": "KOKC",
        "timezone": "America/Chicago",
    },
    "st. louis": {
        "display_name": "St. Louis",
        "lat": 38.6270, "lon": -90.1994,
        "mos_station": "KSTL",
        "timezone": "America/Chicago",
    },
    "st louis": {
        "display_name": "St. Louis",
        "lat": 38.6270, "lon": -90.1994,
        "mos_station": "KSTL",
        "timezone": "America/Chicago",
    },
    "milwaukee": {
        "display_name": "Milwaukee",
        "lat": 43.0389, "lon": -87.9065,
        "mos_station": "KMKE",
        "timezone": "America/Chicago",
    },
    "jacksonville": {
        "display_name": "Jacksonville",
        "lat": 30.3322, "lon": -81.6557,
        "mos_station": "KJAX",
        "timezone": "America/New_York",
    },
    "tampa": {
        "display_name": "Tampa",
        "lat": 27.9506, "lon": -82.4572,
        "mos_station": "KTPA",
        "timezone": "America/New_York",
    },
    # ── International cities ──────────────────────────────────────────────────
    "london": {
        "display_name": "London",
        "lat": 51.5074, "lon": -0.1278,
        "mos_station": "EGLL",
        "timezone": "Europe/London",
    },
    "paris": {
        "display_name": "Paris",
        "lat": 48.8566, "lon": 2.3522,
        "mos_station": "LFPG",
        "timezone": "Europe/Paris",
    },
    "berlin": {
        "display_name": "Berlin",
        "lat": 52.5200, "lon": 13.4050,
        "mos_station": "EDDB",
        "timezone": "Europe/Berlin",
    },
    "tokyo": {
        "display_name": "Tokyo",
        "lat": 35.6762, "lon": 139.6503,
        "mos_station": "RJTT",
        "timezone": "Asia/Tokyo",
    },
    "sydney": {
        "display_name": "Sydney",
        "lat": -33.8688, "lon": 151.2093,
        "mos_station": "YSSY",
        "timezone": "Australia/Sydney",
    },
    "toronto": {
        "display_name": "Toronto",
        "lat": 43.6532, "lon": -79.3832,
        "mos_station": "CYYZ",
        "timezone": "America/Toronto",
    },
    "miami beach": {
        "display_name": "Miami",
        "lat": 25.7617, "lon": -80.1918,
        "mos_station": "KMIA",
        "timezone": "America/New_York",
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
