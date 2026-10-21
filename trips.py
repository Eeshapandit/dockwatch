"""Steps 9 + 10: BATCH ingestion of monthly trip history.

Citi Bike publishes every ride as zipped CSVs on a public S3 bucket.
This module:
  9.  finds the right file, downloads it, and converts it to Parquet
      (normalizing two different CSV schemas into one)
  10. loads one month into raw.trips, safely rerunnable

Run it:
  python trips.py --month latest          # newest month published
  python trips.py --month 202509          # a specific month
  python trips.py --month 202509 --step extract   # only download + Parquet

Why Parquet in the middle instead of loading the CSV straight in?
  * typed: dates are dates, numbers are numbers, decided once, here
  * columnar + compressed: often 5 to 10x smaller than the CSV
  * a clean checkpoint: if the DB load fails, we rerun only the load,
    not the slow download
"""
import argparse
import hashlib
import io
import logging
import os
import re
import xml.etree.ElementTree as ET
import zipfile
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.csv as pacsv
import pyarrow.parquet as pq
import requests

BUCKET = "https://s3.amazonaws.com/tripdata"
DATA_DIR = Path(os.getenv("DATA_DIR", "data"))

log = logging.getLogger("trips")

# One schema for every year. Order matters: COPY uses it.
COLUMNS = [
    "ride_id", "rideable_type", "started_at", "ended_at",
    "start_station_id", "start_station_name", "end_station_id", "end_station_name",
    "start_lat", "start_lng", "end_lat", "end_lng", "member_casual", "source_month",
]
SCHEMA = pa.schema([
    ("ride_id", pa.string()), ("rideable_type", pa.string()),
    ("started_at", pa.timestamp("us")), ("ended_at", pa.timestamp("us")),
    ("start_station_id", pa.string()), ("start_station_name", pa.string()),
    ("end_station_id", pa.string()), ("end_station_name", pa.string()),
    ("start_lat", pa.float64()), ("start_lng", pa.float64()),
    ("end_lat", pa.float64()), ("end_lng", pa.float64()),
    ("member_casual", pa.string()), ("source_month", pa.string()),
])

# Pre 2021 files use different headers ("starttime", "Start Station ID"...).
# We compare headers with spaces/underscores removed and lowercased.
OLD_TO_NEW = {
    "starttime": "started_at", "stoptime": "ended_at",
    "startstationid": "start_station_id", "startstationname": "start_station_name",
    "startstationlatitude": "start_lat", "startstationlongitude": "start_lng",
    "endstationid": "end_station_id", "endstationname": "end_station_name",
    "endstationlatitude": "end_lat", "endstationlongitude": "end_lng",
    "usertype": "usertype", "bikeid": "bikeid",
}


# ---------------------------------------------------------------- find the file

def list_keys(session, prefix=""):
    """List object names in the public bucket (S3 ListBucket XML, paginated)."""
    keys, marker = [], ""
    while True:
        r = session.get(BUCKET, params={"prefix": prefix, "marker": marker}, timeout=30)
        r.raise_for_status()
        page, truncated = parse_listing(r.text)
        keys += page
        if not truncated or not page:
            return keys
        marker = page[-1]


def parse_listing(xml_text):
    """S3 listing XML -> ([keys], is_truncated). Namespace agnostic."""
    root = ET.fromstring(xml_text)
    keys = [el.text for el in root.iter() if el.tag.endswith("}Key") or el.tag == "Key"]
    trunc = [el.text for el in root.iter() if el.tag.endswith("IsTruncated")]
    return keys, bool(trunc) and trunc[0].strip().lower() == "true"


def latest_month(keys, region="nyc"):
    """Newest YYYYMM with its own monthly file."""
    pat = re.compile(r"^JC-(\d{6})-" if region == "jc" else r"^(\d{6})-citibike-tripdata")
    months = [m.group(1) for k in keys if (m := pat.match(k))]
    if not months:
        raise ValueError("no monthly files found in bucket listing")
    return max(months)


def pick_key(keys, month, region="nyc"):
    """The zip that holds `month`: its own monthly file, else that year's bundle."""
    zips = [k for k in keys if k.endswith(".zip")]
    if region == "jc":
        monthly = [k for k in zips if k.startswith(f"JC-{month}")]
    else:
        monthly = [k for k in zips if k.startswith(f"{month}-citibike-tripdata")]
    if monthly:
        return monthly[0]
    yearly = [k for k in zips if k.startswith(f"{month[:4]}-citibike-tripdata")]
    if region == "nyc" and yearly:
        return yearly[0]
    raise FileNotFoundError(f"no trip file for {region} {month}")


def download(session, key, dest_dir=DATA_DIR):
    """Stream the zip to disk. Skips the download if the same size file exists."""
    dest_dir.mkdir(parents=True, exist_ok=True)
    dest = dest_dir / key
    url = f"{BUCKET}/{key}"
    size = int(session.head(url, timeout=30).headers.get("Content-Length", 0))
    if dest.exists() and size and dest.stat().st_size == size:
        log.info("already downloaded %s", dest)
        return dest
    log.info("downloading %s (%.0f MB)", url, size / 1e6)
    with session.get(url, stream=True, timeout=120) as r:
        r.raise_for_status()
        tmp = dest.with_suffix(".part")
        with open(tmp, "wb") as f:
            for chunk in r.iter_content(chunk_size=1 << 20):
                f.write(chunk)
        tmp.rename(dest)  # rename at the end: a half download never looks finished
    return dest


# ---------------------------------------------------------------- transform

def _flat(name):
    return re.sub(r"[\s_]", "", str(name)).lower()


def normalize(df, month):
    """Any year's CSV chunk (all strings) -> our standard columns and types."""
    flat = {_flat(c): c for c in df.columns}
    if "rideid" in flat:  # 2021+ schema, already almost right
        out = df.rename(columns={flat[_flat(c)]: c for c in COLUMNS if _flat(c) in flat})
    else:  # pre 2021 schema
        out = df.rename(columns={flat[k]: v for k, v in OLD_TO_NEW.items() if k in flat})
        out["member_casual"] = out.get("usertype").map({"Subscriber": "member", "Customer": "casual"})
        out["rideable_type"] = "classic_bike"
        # No ride id before 2021: build a stable one from what identifies a ride.
        basis = out["bikeid"].astype(str) + "|" + out["started_at"].astype(str) + "|" + out["start_station_id"].astype(str)
        out["ride_id"] = basis.map(lambda s: hashlib.sha1(s.encode()).hexdigest()[:16])

    out = out.reindex(columns=COLUMNS)
    out["source_month"] = month
    for c in ("started_at", "ended_at"):
        out[c] = pd.to_datetime(out[c], errors="coerce", format="mixed").astype("datetime64[us]")
    for c in ("start_lat", "start_lng", "end_lat", "end_lng"):
        out[c] = pd.to_numeric(out[c], errors="coerce")
    for c in ("ride_id", "rideable_type", "start_station_id", "start_station_name",
              "end_station_id", "end_station_name", "member_casual"):
        out[c] = out[c].astype("object").where(out[c].notna(), None)
    return pa.Table.from_pandas(out, schema=SCHEMA, preserve_index=False)


def _csv_members(zf, month, out_dir):
    """Yield readable CSV file objects for `month`, descending into nested zips."""
    for name in sorted(zf.namelist()):
        base = os.path.basename(name)
        if name.startswith("__MACOSX") or not base:
            continue
        if base.endswith(".zip") and month in base:
            nested = out_dir / base
            with zf.open(name) as src, open(nested, "wb") as dst:
                dst.write(src.read())
            with zipfile.ZipFile(nested) as inner:
                yield from _csv_members(inner, month, out_dir)
            nested.unlink()
        elif base.endswith(".csv") and (month in base or month[:4] not in base):
            with zf.open(name) as f:
                yield base, f


def zip_to_parquet(zip_path, month, out_path, chunksize=250_000):
    """Stream every CSV for `month` in the zip into one Parquet file. Returns row count."""
    rows = 0
    tmp = Path(str(out_path) + ".part")
    with zipfile.ZipFile(zip_path) as zf, pq.ParquetWriter(tmp, SCHEMA, compression="zstd") as writer:
        for name, f in _csv_members(zf, month, Path(out_path).parent):
            for chunk in pd.read_csv(io.TextIOWrapper(f, encoding="utf-8", errors="replace"),
                                     dtype=str, chunksize=chunksize, keep_default_na=False, na_values=[""]):
                table = normalize(chunk, month)
                writer.write_table(table)
                rows += table.num_rows
            log.info("  %s done, %d rows so far", name, rows)
    tmp.rename(out_path)
    return rows


def extract_month(month="latest", region="nyc", data_dir=DATA_DIR):
    """Step 9: find, download and convert one month. Returns (month, parquet_path)."""
    session = requests.Session()
    prefix = "JC-" if region == "jc" else ""
    keys = list_keys(session, prefix)
    if month == "latest":
        month = latest_month(keys, region)
    key = pick_key(keys, month, region)
    zip_path = download(session, key, Path(data_dir))
    out = Path(data_dir) / f"trips_{region}_{month}.parquet"
    if out.exists():
        log.info("parquet already built: %s", out)
        return month, out
    n = zip_to_parquet(zip_path, month, out)
    log.info("wrote %s (%d rows)", out, n)
    return month, out


# ---------------------------------------------------------------- load

def load_month(parquet_path, month, dsn=None):
    """Step 10: replace one month in raw.trips, atomically. Returns rows inserted.

    delete month + insert month happen in ONE transaction, so readers never
    see a half loaded month, and rerunning the same month is always safe.
    """
    import psycopg

    from db import DSN

    cols = ", ".join(COLUMNS)
    with psycopg.connect(dsn or DSN) as conn, conn.cursor() as cur:
        cur.execute("create temp table trips_stage (like raw.trips including defaults) on commit drop")
        with cur.copy(f"copy trips_stage ({cols}) from stdin with (format csv)") as copy:
            pf = pq.ParquetFile(parquet_path)
            for batch in pf.iter_batches(batch_size=200_000):
                buf = io.BytesIO()
                pacsv.write_csv(pa.Table.from_batches([batch]), buf,
                                pacsv.WriteOptions(include_header=False))
                copy.write(buf.getvalue())
        cur.execute("delete from raw.trips where source_month = %s", (month,))
        deleted = cur.rowcount
        cur.execute(f"insert into raw.trips ({cols}) select {cols} from trips_stage "
                    "where ride_id is not null on conflict (ride_id) do nothing")
        inserted = cur.rowcount
    log.info("month %s: replaced %d rows with %d rows", month, deleted, inserted)
    return inserted


def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)-7s %(message)s", datefmt="%H:%M:%S")
    ap = argparse.ArgumentParser(description="Load one month of Citi Bike trips")
    ap.add_argument("--month", default="latest", help="YYYYMM or 'latest'")
    ap.add_argument("--region", default="nyc", choices=["nyc", "jc"])
    ap.add_argument("--step", default="all", choices=["all", "extract", "load"])
    args = ap.parse_args()

    if args.step == "load":
        if args.month == "latest":
            ap.error("--step load needs an explicit --month")
        path = DATA_DIR / f"trips_{args.region}_{args.month}.parquet"
        load_month(path, args.month)
        return
    month, path = extract_month(args.month, args.region)
    if args.step == "all":
        load_month(path, month)


if __name__ == "__main__":
    main()
