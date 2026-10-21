-- The most travelled station to station routes on weekdays, by start hour.
select
    t.start_hour                    as hour,
    t.start_station_id,
    t.end_station_id,
    s1.station_name                 as start_name,
    s2.station_name                 as end_name,
    s1.lat as start_lat, s1.lng as start_lng,
    s2.lat as end_lat,   s2.lng as end_lng,
    count(*)                        as trips,
    round(avg(t.duration_min), 1)   as avg_duration_min
from {{ ref('stg_trips') }} t
join {{ ref('int_trip_stations') }} s1 on s1.trip_station_id = t.start_station_id
join {{ ref('int_trip_stations') }} s2 on s2.trip_station_id = t.end_station_id
where t.is_weekday
  and t.start_station_id <> t.end_station_id
group by 1, 2, 3, 4, 5, 6, 7, 8, 9
