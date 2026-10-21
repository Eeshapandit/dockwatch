select
    station_id,
    name          as station_name,
    short_name,                      -- the id used in trip files
    lat,
    lon,
    capacity,
    region_id
from {{ source('raw', 'station_information') }}
