import os

import pyarrow.parquet as pq
import pytest

import trips
from conftest import FIXTURES

LISTING = """<?xml version="1.0" encoding="UTF-8"?>
<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
  <IsTruncated>false</IsTruncated>
  <Contents><Key>2023-citibike-tripdata.zip</Key></Contents>
  <Contents><Key>202508-citibike-tripdata.zip</Key></Contents>
  <Contents><Key>202509-citibike-tripdata.zip</Key></Contents>
  <Contents><Key>JC-202509-citibike-tripdata.csv.zip</Key></Contents>
  <Contents><Key>index.html</Key></Contents>
</ListBucketResult>"""


def test_listing_latest_and_pick():
    keys, truncated = trips.parse_listing(LISTING)
    assert not truncated and len(keys) == 5
    assert trips.latest_month(keys) == "202509"
    assert trips.latest_month(keys, "jc") == "202509"
    assert trips.pick_key(keys, "202508") == "202508-citibike-tripdata.zip"
    assert trips.pick_key(keys, "202305") == "2023-citibike-tripdata.zip"  # falls back to yearly bundle
    assert trips.pick_key(keys, "202509", "jc").startswith("JC-")
    with pytest.raises(FileNotFoundError):
        trips.pick_key(keys, "201001")


def test_new_schema_zip_to_parquet(tmp_path):
    out = tmp_path / "t.parquet"
    n = trips.zip_to_parquet(FIXTURES / "202509-citibike-tripdata.zip", "202509", out, chunksize=1)
    assert n == 4  # both CSV parts, junk member skipped
    t = pq.read_table(out).to_pylist()
    first = t[0]
    assert first["start_station_id"] == "6969.08"  # stays text, not 6969.08 float
    assert first["started_at"].hour == 8 and first["source_month"] == "202509"
    assert t[3]["start_station_id"] is None


def test_old_schema_is_mapped(tmp_path):
    out = tmp_path / "old.parquet"
    trips.zip_to_parquet(FIXTURES / "201703-citibike-tripdata.csv.zip", "201703", out)
    (r,) = pq.read_table(out).to_pylist()
    assert r["member_casual"] == "member" and r["rideable_type"] == "classic_bike"
    assert r["start_station_id"] == "72" and len(r["ride_id"]) == 16
    assert abs(r["start_lat"] - 40.767) < 0.01


@pytest.mark.skipif(not os.getenv("PG_DSN"), reason="needs a database (set PG_DSN)")
def test_load_month_is_rerunnable(tmp_path):
    out = tmp_path / "t.parquet"
    trips.zip_to_parquet(FIXTURES / "202509-citibike-tripdata.zip", "202509", out)
    assert trips.load_month(out, "202509") == 4
    assert trips.load_month(out, "202509") == 4  # rerun replaces, never doubles
