-- The 1970 placeholder timestamps from uninstalled stations must never reach staging.
select * from {{ ref('stg_station_status') }} where last_reported < timestamptz '2020-01-01'
