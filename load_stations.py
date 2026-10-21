"""Step 5: load station names/locations with an UPSERT.

Run:  python load_stations.py
Run it twice: the second run should say 0 inserted, 0 updated.
"""
import psycopg
import requests

from gbfs import clean_info_feed

ROOT = "https://gbfs.citibikenyc.com/gbfs/gbfs.json"
from db import DSN  # localhost on your terminal, "postgres" inside Docker

# UPSERT = UPdate or inSERT.
#   * new station_id         -> plain insert
#   * existing station_id    -> "on conflict ... do update": overwrite with new values
#   * existing AND identical -> the WHERE clause skips it, so updated_at only
#                               moves when something REALLY changed
# "excluded" = the row we just tried to insert (the new values).
UPSERT_SQL = """
insert into raw.station_information
    (station_id, name, short_name, lat, lon, capacity, region_id)
values
    (%(station_id)s, %(name)s, %(short_name)s, %(lat)s, %(lon)s, %(capacity)s, %(region_id)s)
on conflict (station_id) do update set
    name       = excluded.name,
    short_name = excluded.short_name,
    lat        = excluded.lat,
    lon        = excluded.lon,
    capacity   = excluded.capacity,
    region_id  = excluded.region_id,
    updated_at = now()
where (raw.station_information.name, raw.station_information.lat,
       raw.station_information.lon, raw.station_information.capacity)
   is distinct from
      (excluded.name, excluded.lat, excluded.lon, excluded.capacity)
"""
# "is distinct from" is like != but treats NULL = NULL as equal.
# Plain != returns NULL when either side is NULL, which would break the check.

# ---- Extract ----
discovery = requests.get(ROOT, timeout=15).json()
feeds = {f["name"]: f["url"] for f in discovery["data"]["en"]["feeds"]}
payload = requests.get(feeds["station_information"], timeout=15).json()

# ---- Transform ----
rows = clean_info_feed(payload)

# ---- Load ----
with psycopg.connect(DSN) as conn:
    with conn.cursor() as cur:
        cur.execute("select count(*) from raw.station_information")
        before = cur.fetchone()[0]

        cur.executemany(UPSERT_SQL, rows)
        touched = cur.rowcount  # rows inserted + rows updated (skipped ones don't count)

        cur.execute("select count(*) from raw.station_information")
        after = cur.fetchone()[0]

inserted = after - before
updated = touched - inserted
print(f"Fetched:   {len(rows)} stations")
print(f"Inserted:  {inserted} new")
print(f"Updated:   {updated} changed")
print(f"Unchanged: {len(rows) - inserted - updated}")
