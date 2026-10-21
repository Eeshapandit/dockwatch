"""Export the dbt marts into docs/data.json for the public project page.

The page (docs/index.html) is static: it can't query Postgres. So after
`dbt build`, this script snapshots the answers into one small JSON file.

Run:  python export_page_data.py
"""
import json
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path

import psycopg

from db import DSN

OUT = Path(__file__).parent / "docs" / "data.json"
FLOWS_PER_HOUR = 150     # busiest routes kept per hour
NET_PER_HOUR = 400       # stations with the biggest gain or loss kept per hour


def rows(cur, sql, *params):
    cur.execute(sql, params)
    cols = [c.name for c in cur.description]
    return [dict(zip(cols, r)) for r in cur.fetchall()]


def table_exists(cur, name):
    cur.execute("select to_regclass(%s) is not null", (name,))
    return cur.fetchone()[0]


def num(v, nd=None):
    if isinstance(v, Decimal):
        v = float(v)
    return round(v, nd) if (nd is not None and v is not None) else v


def main():
    data = {"generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds")}
    stations, index = [], {}

    def sid(name, lat, lng):
        key = (name, round(lat, 4), round(lng, 4))
        if key not in index:
            index[key] = len(stations)
            stations.append([name, round(lat, 5), round(lng, 5)])
        return index[key]

    with psycopg.connect(DSN) as conn, conn.cursor() as cur:
        stats = {}
        if table_exists(cur, "raw.station_information"):
            cur.execute("select count(*) from raw.station_information")
            stats["stations_tracked"] = cur.fetchone()[0]
        if table_exists(cur, "raw.station_status"):
            cur.execute("select count(*), min(last_reported), max(last_reported) from raw.station_status")
            n, lo, hi = cur.fetchone()
            stats.update(status_rows=n, live_from=lo and lo.isoformat(), live_to=hi and hi.isoformat())

        if table_exists(cur, "marts.station_latest"):
            latest = rows(cur, "select station_name, capacity, lat, lon, is_empty, is_full from marts.station_latest")
            stats["empty_now"] = sum(r["is_empty"] for r in latest)
            stats["full_now"] = sum(r["is_full"] for r in latest)
            data["empty_now"] = [[r["station_name"], r["capacity"], num(r["lat"], 5), num(r["lon"], 5)]
                                 for r in latest if r["is_empty"] and r["lat"] is not None]

        if table_exists(cur, "marts.mart_empty_leaderboard"):
            data["leaderboard"] = [
                {"name": r["station_name"], "capacity": r["capacity"],
                 "minutes_empty": num(r["minutes_empty"], 0), "share_empty": num(r["share_empty"], 3)}
                for r in rows(cur, """select * from marts.mart_empty_leaderboard
                                      where station_name is not null
                                      order by minutes_empty desc, station_name limit 15""")]

        if table_exists(cur, "marts.mart_hourly_trips"):
            cur.execute("select count(*), min(source_month), max(source_month), count(distinct source_month) from raw.trips")
            n, m_lo, m_hi, months = cur.fetchone()
            stats.update(trips_loaded=n, trip_months=[m_lo, m_hi] if n else [])
            data["hourly"] = [
                {"hour": r["hour"],
                 "weekday": num(r["weekday_trips"] / r["weekday_days"], 0) if r["weekday_days"] else 0,
                 "weekend": num(r["weekend_trips"] / r["weekend_days"], 0) if r["weekend_days"] else 0}
                for r in rows(cur, "select * from marts.mart_hourly_trips order by hour")]

        if table_exists(cur, "marts.mart_station_net_flow"):
            net = {}
            for r in rows(cur, """
                    select * from (
                        select *, row_number() over (partition by hour order by abs(net) desc) as rk
                        from marts.mart_station_net_flow where lat is not null
                    ) x where rk <= %s order by hour, rk""", NET_PER_HOUR):
                net.setdefault(str(r["hour"]), []).append(
                    [sid(r["station_name"], r["lat"], r["lng"]), r["net"], r["departures"], r["arrivals"]])
            data["net"] = net

        if table_exists(cur, "marts.mart_trip_flows"):
            flows = {}
            for r in rows(cur, """
                    select * from (
                        select *, row_number() over (partition by hour order by trips desc, start_name) as rk
                        from marts.mart_trip_flows where start_lat is not null and end_lat is not null
                    ) x where rk <= %s order by hour, rk""", FLOWS_PER_HOUR):
                a = sid(r["start_name"], r["start_lat"], r["start_lng"])
                b = sid(r["end_name"], r["end_lat"], r["end_lng"])
                flows.setdefault(str(r["hour"]), []).append([a, b, r["trips"]])
            data["flows"] = flows

    data["stats"] = stats
    data["stations"] = stations
    OUT.parent.mkdir(exist_ok=True)
    OUT.write_text(json.dumps(data, separators=(",", ":")))
    print(f"wrote {OUT} ({OUT.stat().st_size / 1024:.0f} KB): {len(stations)} stations, "
          f"{sum(len(v) for v in data.get('flows', {}).values())} routes")


if __name__ == "__main__":
    main()
