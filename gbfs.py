"""Step 2: turn raw GBFS JSON into clean rows that match our table.

Pure functions only: no network, no database. That makes them easy
to test (Step 3) and reuse in the producer later (Step 6).
"""
from datetime import datetime, timezone


def epoch_to_utc(seconds):
    """1792595980 -> datetime(2026, 10, 21, ..., tzinfo=UTC)"""
    return datetime.fromtimestamp(seconds, tz=timezone.utc)


def clean_station(s):
    """One raw station dict -> one clean row dict (our table's columns)."""
    return {
        "station_id": s["station_id"],
        "num_bikes_available": s["num_bikes_available"],
        "num_ebikes_available": s.get("num_ebikes_available", 0),  # .get = default if missing
        "num_bikes_disabled": s.get("num_bikes_disabled", 0),
        "num_docks_available": s["num_docks_available"],
        "num_docks_disabled": s.get("num_docks_disabled", 0),
        "is_installed": bool(s["is_installed"]),  # 0/1 -> False/True
        "is_renting": bool(s["is_renting"]),
        "is_returning": bool(s["is_returning"]),
        "last_reported": epoch_to_utc(s["last_reported"]),
        # legacy_id and eightd_has_available_keys are dropped on purpose
    }


def clean_status_feed(payload):
    """Whole station_status payload -> list of clean rows.

    Skips stations that are not installed: they report fake timestamps
    (like 86400 = Jan 2, 1970) and are not real stations yet.
    """
    rows = []
    for s in payload["data"]["stations"]:
        if not s.get("is_installed"):
            continue
        rows.append(clean_station(s))
    return rows


def clean_station_info(s):
    """One raw station_information dict -> one clean row for the dimension table."""
    return {
        "station_id": s["station_id"],
        "name": s["name"],
        "short_name": s.get("short_name"),
        "lat": s.get("lat"),
        "lon": s.get("lon"),
        "capacity": s.get("capacity"),
        "region_id": s.get("region_id"),
    }


def clean_info_feed(payload):
    """Whole station_information payload -> list of clean rows."""
    return [clean_station_info(s) for s in payload["data"]["stations"]]
