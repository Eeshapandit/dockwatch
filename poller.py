"""Step 4: poll the live feed every 60 seconds and save changes, forever.

Run in foreground:  python poller.py            (Ctrl+C to stop)
Run in background:  nohup python poller.py > poller.log 2>&1 &
"""
import logging
import time

import psycopg
import requests

from gbfs import clean_status_feed

ROOT = "https://gbfs.citibikenyc.com/gbfs/gbfs.json"
DSN = "postgresql://dockwatch:dockwatch@localhost:5432/dockwatch"
POLL_SECONDS = 60  # GBFS refreshes about once a minute; polling faster just wastes calls

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

# Logging instead of print: every line gets a timestamp and a level
# (INFO / WARNING / ERROR), and can be sent to a file. Long running
# jobs always log, because nobody is watching the screen.
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-7s %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("poller")


def get_status_url(session):
    """Look up the station_status url once, via the discovery file."""
    discovery = session.get(ROOT, timeout=15).json()
    feeds = {f["name"]: f["url"] for f in discovery["data"]["en"]["feeds"]}
    return feeds["station_status"]


def poll_once(session, conn, status_url):
    """One cycle of Extract -> Transform -> Load. Returns (fetched, inserted)."""
    resp = session.get(status_url, timeout=15)
    resp.raise_for_status()  # turn HTTP errors (500, 503...) into exceptions
    rows = clean_status_feed(resp.json())

    with conn.transaction():  # all rows of this cycle commit together, or none
        with conn.cursor() as cur:
            cur.execute("select count(*) from raw.station_status")
            before = cur.fetchone()[0]
            cur.executemany(INSERT_SQL, rows)
            cur.execute("select count(*) from raw.station_status")
            after = cur.fetchone()[0]
    return len(rows), after - before


def main():
    # A Session reuses the same HTTPS connection across requests: faster, politer.
    session = requests.Session()
    status_url = get_status_url(session)
    log.info("polling %s every %ss", status_url, POLL_SECONDS)

    # One DB connection for the whole run, instead of reconnecting every minute.
    with psycopg.connect(DSN, autocommit=True) as conn:
        cycle = 0
        while True:  # a poller runs until someone stops it
            cycle += 1
            started = time.monotonic()  # monotonic = a clock that never jumps

            try:
                fetched, inserted = poll_once(session, conn, status_url)
                log.info("cycle %d: fetched %d, inserted %d changed", cycle, fetched, inserted)
            except requests.RequestException as e:
                # Network blip or Citi Bike outage: log it and try again next cycle.
                # Crashing here would stop the whole pipeline for one bad minute.
                log.warning("cycle %d: fetch failed, will retry: %s", cycle, e)
            except psycopg.Error as e:
                log.error("cycle %d: database error: %s", cycle, e)

            # Sleep the REST of the minute, not a full 60s. If a cycle took
            # 3s, sleep 57s, so cycles stay on a steady rhythm (no drift).
            elapsed = time.monotonic() - started
            time.sleep(max(0, POLL_SECONDS - elapsed))


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:  # Ctrl+C lands here: exit cleanly, no ugly traceback
        log.info("stopped by user")
