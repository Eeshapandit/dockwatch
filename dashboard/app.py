"""Step 13: the dashboard. Reads ONLY dbt marts, never raw tables.

Run:  make dashboard   then open port 8501
"""
import os

import altair as alt
import pandas as pd
import psycopg
import pydeck as pdk
import streamlit as st

DSN = os.getenv("PG_DSN", "postgresql://dockwatch:dockwatch@localhost:5432/dockwatch")
NYC = pdk.ViewState(latitude=40.735, longitude=-73.95, zoom=10.6, pitch=35)
RED, GREEN, ORANGE = [226, 74, 51], [46, 160, 95], [240, 150, 40]

st.set_page_config(page_title="dockwatch", page_icon="🚲", layout="wide")


@st.cache_data(ttl=60)
def q(sql, *params):
    with psycopg.connect(DSN) as conn:
        return pd.read_sql(sql, conn, params=params or None)


def exists(name):
    return bool(q("select to_regclass(%s) is not null as ok", name)["ok"][0])


st.title("🚲 dockwatch")
st.caption("Live NYC bike share availability and trip flows. Built with Python, Redpanda, Postgres, Airflow and dbt.")

if not exists("marts.station_latest"):
    st.warning("No dbt models yet. Run `make dbt` first.")
    st.stop()

# ------------------------------------------------------------------ right now
latest = q("select * from marts.station_latest")
c1, c2, c3, c4 = st.columns(4)
c1.metric("Stations reporting", f"{len(latest):,}")
c2.metric("Empty right now", f"{int(latest.is_empty.sum()):,}")
c3.metric("Full right now", f"{int(latest.is_full.sum()):,}")
c4.metric("Last update (UTC)", str(latest.last_reported.max())[:16])

left, right = st.columns([3, 2])
with left:
    st.subheader("Where you can't get a bike right now")
    pts = latest.dropna(subset=["lat", "lon"]).copy()
    pts["color"] = pts.apply(lambda r: RED if r.is_empty else (ORANGE if r.is_full else [120, 140, 130]), axis=1)
    pts["radius"] = pts.apply(lambda r: 70 if (r.is_empty or r.is_full) else 30, axis=1)
    st.pydeck_chart(pdk.Deck(
        map_style="dark", initial_view_state=NYC,
        layers=[pdk.Layer("ScatterplotLayer", pts, get_position="[lon, lat]", get_fill_color="color",
                          get_radius="radius", opacity=0.8, pickable=True)],
        tooltip={"text": "{station_name}\nbikes: {num_bikes_available}  docks: {num_docks_available}"}))
    st.caption("Red = empty, orange = full, grey = has bikes and docks.")
with right:
    st.subheader("Empty the longest, last 24h of data")
    if exists("marts.mart_empty_leaderboard"):
        lb = q("""select station_name as station, capacity,
                         round(minutes_empty) as minutes_empty,
                         round(100 * share_empty) as pct_empty
                  from marts.mart_empty_leaderboard
                  where station_name is not null
                  order by minutes_empty desc limit 15""")
        st.dataframe(lb, hide_index=True, width="stretch")

# ------------------------------------------------------------------ trip flows
if exists("marts.mart_trip_flows"):
    st.divider()
    st.subheader("How bikes move on a weekday")
    hour = st.slider("Hour of day", 0, 23, 8, format="%d:00")
    flows = q("""select * from marts.mart_trip_flows where hour = %s
                 and start_lat is not null and end_lat is not null
                 order by trips desc limit 250""", hour)
    net = q("""select * from marts.mart_station_net_flow where hour = %s and lat is not null""", hour)
    net["color"] = net.net.apply(lambda n: GREEN if n > 0 else RED)
    net["radius"] = net.net.abs().clip(upper=60) * 6 + 20
    st.pydeck_chart(pdk.Deck(
        map_style="dark", initial_view_state=NYC,
        layers=[
            pdk.Layer("ArcLayer", flows, get_source_position="[start_lng, start_lat]",
                      get_target_position="[end_lng, end_lat]", get_source_color=ORANGE,
                      get_target_color=[255, 220, 120], get_width="trips / 8", width_min_pixels=1, pickable=True),
            pdk.Layer("ScatterplotLayer", net, get_position="[lng, lat]", get_fill_color="color",
                      get_radius="radius", opacity=0.55, pickable=True),
        ],
        tooltip={"text": "{start_name} → {end_name}: {trips} trips"}))
    st.caption("Orange arcs: the busiest routes starting this hour. Red stations lose bikes, green stations gain them.")

    hourly = q("select * from marts.mart_hourly_trips order by hour")
    hourly["Weekday"] = hourly.weekday_trips / hourly.weekday_days.where(hourly.weekday_days > 0)
    hourly["Weekend"] = hourly.weekend_trips / hourly.weekend_days.where(hourly.weekend_days > 0)
    long = hourly.melt("hour", ["Weekday", "Weekend"], var_name="day", value_name="trips per day")
    st.altair_chart(alt.Chart(long).mark_line(point=True).encode(
        x=alt.X("hour:O", title="Hour started"), y="trips per day:Q", color="day:N"), width="stretch")

# ------------------------------------------------------------------ one station
if exists("marts.fct_station_hourly_availability"):
    st.divider()
    st.subheader("One station, hour by hour")
    names = latest.dropna(subset=["station_name"]).sort_values("station_name")
    pick = st.selectbox("Station", names.station_name.tolist())
    sid = names.loc[names.station_name == pick, "station_id"].iloc[0]
    hist = q("""select hour_local, minutes_empty, minutes_full from marts.fct_station_hourly_availability
                where station_id = %s order by hour_local""", sid)
    if hist.empty:
        st.info("No history for this station yet.")
    else:
        h = hist.melt("hour_local", ["minutes_empty", "minutes_full"], var_name="state", value_name="minutes")
        st.altair_chart(alt.Chart(h).mark_bar().encode(
            x=alt.X("hour_local:T", title="Hour (New York)"), y="minutes:Q",
            color=alt.Color("state:N", scale=alt.Scale(range=["#e24a33", "#f09628"]))), width="stretch")
