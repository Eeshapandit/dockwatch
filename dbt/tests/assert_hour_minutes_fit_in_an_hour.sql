-- A station can't be empty for more than 60 minutes of one hour.
-- dbt treats any returned row as a failure.
select *
from {{ ref('fct_station_hourly_availability') }}
where minutes_empty > 60.01 or minutes_full > 60.01 or minutes_observed > 60.01
