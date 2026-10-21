-- Weekday rides by hour: does a station GAIN or LOSE bikes?
-- net < 0: more rides start here than end here, so it drains (red on the map).
-- net > 0: it fills up (green on the map).
with departures as (
    select start_station_id as trip_station_id, start_hour as hour, count(*) as departures
    from {{ ref('stg_trips') }} where is_weekday group by 1, 2
),
arrivals as (
    select end_station_id as trip_station_id, extract(hour from ended_at)::int as hour, count(*) as arrivals
    from {{ ref('stg_trips') }} where is_weekday group by 1, 2
)
select
    coalesce(d.trip_station_id, a.trip_station_id)     as trip_station_id,
    coalesce(d.hour, a.hour)                           as hour,
    s.station_name,
    s.lat,
    s.lng,
    coalesce(d.departures, 0)                          as departures,
    coalesce(a.arrivals, 0)                            as arrivals,
    coalesce(a.arrivals, 0) - coalesce(d.departures, 0) as net
from departures d
full outer join arrivals a
  on a.trip_station_id = d.trip_station_id and a.hour = d.hour
join {{ ref('int_trip_stations') }} s
  on s.trip_station_id = coalesce(d.trip_station_id, a.trip_station_id)
