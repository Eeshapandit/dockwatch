-- Step 2: raw table for live station status.
--
-- GRAIN: one row = one station at one moment it reported.
-- KEY:   (station_id, last_reported). station_id alone repeats every poll,
--        but a station can only report once at a given instant.
--        Bonus: re-inserting an unchanged station hits this key and is
--        skipped, which makes our loader idempotent (Step 5).

create schema if not exists raw;

create table if not exists raw.station_status (
    station_id            text        not null,  -- UUID from source, text is safest
    num_bikes_available   int         not null,  -- includes e-bikes
    num_ebikes_available  int         not null,
    num_bikes_disabled    int         not null,
    num_docks_available   int         not null,
    num_docks_disabled    int         not null,
    is_installed          boolean     not null,  -- source sends 0/1, we convert
    is_renting            boolean     not null,
    is_returning          boolean     not null,
    last_reported         timestamptz not null,  -- source sends epoch seconds (UTC)
    loaded_at             timestamptz not null default now(),  -- when WE saved it
    primary key (station_id, last_reported)
);
