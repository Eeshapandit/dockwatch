-- Trip files use their own station ids (they match GBFS short_name).
-- One row per trip station id with a name and a location, taken as the
-- MEDIAN of every ride's coordinates (robust to GPS noise on e-bikes).
with ends as (
    select start_station_id as trip_station_id, start_station_name as station_name, start_lat as lat, start_lng as lng
    from {{ ref('stg_trips') }}
    union all
    select end_station_id, end_station_name, end_lat, end_lng
    from {{ ref('stg_trips') }}
)
select
    trip_station_id,
    mode() within group (order by station_name)               as station_name,
    percentile_cont(0.5) within group (order by lat)          as lat,
    percentile_cont(0.5) within group (order by lng)          as lng,
    count(*)                                                  as ride_ends
from ends
group by trip_station_id
