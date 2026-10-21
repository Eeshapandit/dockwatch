-- The current state of every station, with its name. "What's empty right now?"
select distinct on (s.station_id)
    s.station_id,
    i.station_name,
    i.capacity,
    i.lat,
    i.lon,
    s.num_bikes_available,
    s.num_docks_available,
    (s.num_bikes_available = 0 and s.is_renting)   as is_empty,
    (s.num_docks_available = 0 and s.is_returning) as is_full,
    s.last_reported
from {{ ref('stg_station_status') }} s
left join {{ ref('stg_station_information') }} i using (station_id)
order by s.station_id, s.last_reported desc
