-- Minutes empty / full per station per local (New York) hour.
-- A spell from 8:40 to 10:15 is split into 8:00 (20 min), 9:00 (60), 10:00 (15).
with spells as (
    select
        *,
        spell_start at time zone 'America/New_York' as local_start,
        spell_end   at time zone 'America/New_York' as local_end
    from {{ ref('int_station_spells') }}
),
split as (
    select
        s.station_id,
        h.hour_local,
        s.is_empty,
        s.is_full,
        extract(epoch from least(s.local_end, h.hour_local + interval '1 hour')
                         - greatest(s.local_start, h.hour_local)) / 60.0 as minutes
    from spells s
    cross join lateral generate_series(
        date_trunc('hour', s.local_start), s.local_end, interval '1 hour'
    ) as h(hour_local)
)
select
    station_id || '|' || to_char(hour_local, 'YYYY-MM-DD HH24') as station_hour_key,
    station_id,
    hour_local,
    round(sum(minutes)::numeric, 1)                              as minutes_observed,
    round(coalesce(sum(minutes) filter (where is_empty), 0)::numeric, 1) as minutes_empty,
    round(coalesce(sum(minutes) filter (where is_full), 0)::numeric, 1)  as minutes_full
from split
where minutes > 0
group by station_id, hour_local
