"""Step 1: look at the source before designing anything.

Run:  python explore.py
"""
import json

import requests

# GBFS auto-discovery file. This is the ONLY url we hardcode.
# It lists every other feed, so if Citi Bike moves a feed, we still find it.
ROOT = "https://gbfs.citibikenyc.com/gbfs/gbfs.json"

# timeout=15 so a dead server can't hang our pipeline forever
discovery = requests.get(ROOT, timeout=15).json()

# discovery["data"]["en"]["feeds"] is a list like:
#   [{"name": "station_status", "url": "https://..."}, ...]
# Turn it into a dict {name: url} so we can look feeds up by name.
feeds = {f["name"]: f["url"] for f in discovery["data"]["en"]["feeds"]}
print("Feeds available:", list(feeds))

# station_status = live bikes/docks per station, refreshed about every minute
status = requests.get(feeds["station_status"], timeout=15).json()
stations = status["data"]["stations"]

print("\nFeed last_updated (epoch seconds):", status["last_updated"])
print("Station count:", len(stations))
print("\nFirst station record:")
print(json.dumps(stations[0], indent=2))
