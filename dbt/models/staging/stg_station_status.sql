-- Clean live status. Uninstalled stations are already filtered by the loader;
-- the date filter is a second safety net against placeholder timestamps.
select
    station_id,
    num_bikes_available,
    num_ebikes_available,
    num_bikes_disabled,
    num_docks_available,
    num_docks_disabled,
    is_renting,
    is_returning,
    last_reported
from {{ source('raw', 'station_status') }}
where is_installed
  and last_reported >= timestamptz '2020-01-01'
