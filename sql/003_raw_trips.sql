-- Steps 9 + 10: raw trip history, loaded one month at a time.
--
-- GRAIN: one row per ride.
-- KEY:   ride_id. Files from before 2021 have no ride id, so trips.py
--        builds a stable one from the row's contents.
-- RERUN: loading a month first deletes that source_month, then inserts it
--        again, inside one transaction. Rerun any month any time.

create schema if not exists raw;

create table if not exists raw.trips (
    ride_id             text primary key,
    rideable_type       text,
    started_at          timestamp,          -- local NYC time, as published
    ended_at            timestamp,
    start_station_id    text,               -- matches GBFS short_name, not station_id
    start_station_name  text,
    end_station_id      text,
    end_station_name    text,
    start_lat           double precision,
    start_lng           double precision,
    end_lat             double precision,
    end_lng             double precision,
    member_casual       text,               -- member or casual
    source_month        text not null,      -- '202509', the file it came from
    loaded_at           timestamptz not null default now()
);

create index if not exists ix_trips_source_month on raw.trips (source_month);
create index if not exists ix_trips_started_at on raw.trips (started_at);
