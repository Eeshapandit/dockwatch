-- Step 5: the station DIMENSION table.
--
-- GRAIN: one row per station. Not per minute: names and locations
--        rarely change, so we keep only the latest version.
-- KEY:   station_id alone (unlike the status FACT table, it never repeats).
-- LOAD:  upsert. New station -> insert. Changed station -> update in place.

create schema if not exists raw;

create table if not exists raw.station_information (
    station_id   text primary key,
    name         text not null,          -- "W 52 St & 11 Ave"
    short_name   text,                   -- Citi Bike's own code, like "6926.01"
    lat          double precision,       -- latitude, for maps later
    lon          double precision,       -- longitude
    capacity     int,                    -- total docks at the station
    region_id    text,                   -- NYC vs Jersey City etc.
    first_seen   timestamptz not null default now(),  -- when WE first loaded it
    updated_at   timestamptz not null default now()   -- when WE last changed it
);
