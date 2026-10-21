"""Step 2: fetch live status and clean it.

Run:  python step2.py
"""
import requests

from gbfs import clean_status_feed

ROOT = "https://gbfs.citibikenyc.com/gbfs/gbfs.json"

discovery = requests.get(ROOT, timeout=15).json()
feeds = {f["name"]: f["url"] for f in discovery["data"]["en"]["feeds"]}
payload = requests.get(feeds["station_status"], timeout=15).json()

raw_count = len(payload["data"]["stations"])
rows = clean_status_feed(payload)

print(f"Raw stations:      {raw_count}")
print(f"Clean (installed): {len(rows)}")
print(f"Dropped:           {raw_count - len(rows)}")

empty = [r for r in rows if r["num_bikes_available"] == 0 and r["is_renting"]]
full = [r for r in rows if r["num_docks_available"] == 0 and r["is_returning"]]
print(f"\nRight now: {len(empty)} stations EMPTY, {len(full)} stations FULL")

print("\nOne clean row:")
for k, v in rows[0].items():
    print(f"  {k:22} {v!r}")
