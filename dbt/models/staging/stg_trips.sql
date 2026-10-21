-- Rides between two stations, with obviously broken rows removed:
-- no station (dockless e-bike ends), negative or absurd durations.
select
    ride_id,
    rideable_type,
    member_casual,
    started_at,
    ended_at,
    round(extract(epoch from ended_at - started_at) / 60.0, 1)  as duration_min,
    extract(hour from started_at)::int                          as start_hour,
    extract(isodow from started_at) < 6                         as is_weekday,
    start_station_id,
    start_station_name,
    end_station_id,
    end_station_name,
    start_lat, start_lng, end_lat, end_lng,
    source_month
from {{ source('raw', 'trips') }}
where start_station_id is not null
  and end_station_id is not null
  and ended_at > started_at + interval '1 minute'
  and ended_at < started_at + interval '3 hours'
