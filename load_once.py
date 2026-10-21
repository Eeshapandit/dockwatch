"""Step 3: fetch live status ONCE and save it to Postgres.

Run:  python load_once.py
Run it twice in a row and compare "inserted" counts. That is idempotency.
"""
import psycopg
import requests

from gbfs import clean_status_feed

ROOT = "https://gbfs.citibikenyc.com/gbfs/gbfs.json"

# Connection string format: postgresql://USER:PASSWORD@HOST:PORT/DATABASE
# These match docker-compose.yml.
DSN = "postgresql://dockwatch:dockwatch@localhost:5432/dockwatch"

# %(name)s placeholders get filled from each row dict by name.
# Never build SQL with f-strings: placeholders stop SQL injection and
# let the driver handle types (datetime -> timestamptz, bool -> boolean).
INSERT_SQL = """
insert into raw.station_status (
    station_id, num_bikes_available, num_ebikes_available, num_bikes_disabled,
    num_docks_available, num_docks_disabled, is_installed, is_renting,
    is_returning, last_reported
) values (
    %(station_id)s, %(num_bikes_available)s, %(num_ebikes_available)s, %(num_bikes_disabled)s,
    %(num_docks_available)s, %(num_docks_disabled)s, %(is_installed)s, %(is_renting)s,
    %(is_returning)s, %(last_reported)s
)
on conflict (station_id, last_reported) do nothing
"""
# ^ THE key line. If this exact (station, report time) is already stored,
#   skip it instead of crashing on the primary key. Rerunning is now safe.

# ---- Extract ----
discovery = requests.get(ROOT, timeout=15).json()
feeds = {f["name"]: f["url"] for f in discovery["data"]["en"]["feeds"]}
payload = requests.get(feeds["station_status"], timeout=15).json()

# ---- Transform ----
rows = clean_status_feed(payload)

# ---- Load ----
# "with" closes the connection for us and COMMITS if no error happened
# (or rolls back everything if one did). All rows land, or none do.
with psycopg.connect(DSN) as conn:
    with conn.cursor() as cur:
        cur.execute("select count(*) from raw.station_status")
        before = cur.fetchone()[0]

        cur.executemany(INSERT_SQL, rows)

        cur.execute("select count(*) from raw.station_status")
        after = cur.fetchone()[0]

print(f"Fetched:  {len(rows)} stations")
print(f"Inserted: {after - before} new rows")
print(f"Skipped:  {len(rows) - (after - before)} (already stored, unchanged)")
print(f"Table now has {after} rows")
