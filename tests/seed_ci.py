"""Create the raw tables and fill them with a small, known dataset.

Used by CI (and handy locally) so dbt can build and test every model
without touching the live API or the real trip files.

Run:  python tests/seed_ci.py
"""
import os
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import psycopg

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

import trips  # noqa: E402
from db import DSN, INFO_UPSERT_SQL, STATUS_INSERT_SQL  # noqa: E402

T0 = datetime(2025, 9, 2, 12, 0, tzinfo=timezone.utc)  # 8:00 in New York


def status(sid, minutes, bikes, docks):
    return {"station_id": sid, "num_bikes_available": bikes, "num_ebikes_available": 0,
            "num_bikes_disabled": 0, "num_docks_available": docks, "num_docks_disabled": 0,
            "is_installed": True, "is_renting": True, "is_returning": True,
            "last_reported": T0 + timedelta(minutes=minutes)}


def main():
    # This TRUNCATES the raw tables. Never let it run against real data by accident.
    if not os.getenv("CI") and "--force" not in sys.argv:
        sys.exit("seed_ci.py wipes raw.* tables. It runs automatically in CI; locally use --force.")
    with psycopg.connect(DSN, autocommit=True) as conn, conn.cursor() as cur:
        for f in sorted((ROOT / "sql").glob("0*.sql")):
            cur.execute(f.read_text())
        cur.execute("truncate raw.station_status, raw.station_information, raw.trips")
        cur.executemany(INFO_UPSERT_SQL, [
            {"station_id": "jh", "name": "34 Ave & 83 St", "short_name": "6969.08", "lat": 40.7545, "lon": -73.8850, "capacity": 22, "region_id": "71"},
            {"station_id": "mt", "name": "W 41 St & 8 Ave", "short_name": "6602.03", "lat": 40.7564, "lon": -73.9900, "capacity": 47, "region_id": "71"},
        ])
        cur.executemany(STATUS_INSERT_SQL, [
            status("jh", 0, 6, 16), status("jh", 40, 2, 20), status("jh", 70, 0, 22), status("jh", 200, 3, 19),
            status("mt", 0, 20, 27), status("mt", 90, 47, 0), status("mt", 200, 40, 7),
        ])
    out = ROOT / "data" / "trips_ci.parquet"
    out.parent.mkdir(exist_ok=True)
    out.unlink(missing_ok=True)
    trips.zip_to_parquet(ROOT / "tests" / "fixtures" / "202509-citibike-tripdata.zip", "202509", out)
    trips.load_month(out, "202509")
    print("seeded")


if __name__ == "__main__":
    main()
