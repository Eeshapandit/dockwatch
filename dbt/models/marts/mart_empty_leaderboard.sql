-- Which stations spent the most time empty over the last 24 hours of data?
with window_hours as (
    select *
    from {{ ref('fct_station_hourly_availability') }}
    where hour_local >= (select max(hour_local) from {{ ref('fct_station_hourly_availability') }}) - interval '23 hours'
)
select
    w.station_id,
    i.station_name,
    i.capacity,
    i.lat,
    i.lon,
    sum(w.minutes_observed)                                              as minutes_observed,
    sum(w.minutes_empty)                                                 as minutes_empty,
    sum(w.minutes_full)                                                  as minutes_full,
    round(sum(w.minutes_empty) / nullif(sum(w.minutes_observed), 0), 3) as share_empty
from window_hours w
left join {{ ref('stg_station_information') }} i using (station_id)
group by 1, 2, 3, 4, 5
