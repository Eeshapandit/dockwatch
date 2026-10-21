-- Same question as empty_minutes.sql, now with NAMES.
-- The only new part is the JOIN at the bottom: fact table + dimension table.

with timeline as (
    select
        station_id,
        last_reported,
        num_bikes_available,
        is_renting,
        lead(last_reported) over (
            partition by station_id
            order by last_reported
        ) as next_reported
    from raw.station_status
),
empty_spells as (
    select
        station_id,
        greatest(last_reported, now() - interval '30 minutes') as spell_start,
        coalesce(next_reported, now()) as spell_end
    from timeline
    where num_bikes_available = 0
      and is_renting
      and coalesce(next_reported, now()) > now() - interval '30 minutes'
),
per_station as (
    select
        station_id,
        round(sum(extract(epoch from spell_end - spell_start)) / 60.0, 1) as minutes_empty
    from empty_spells
    group by station_id
)
select
    i.name,
    i.capacity,
    p.minutes_empty
from per_station p
left join raw.station_information i      -- LEFT join: keep the station even if
       on i.station_id = p.station_id    -- its name is missing (shows as blank)
order by p.minutes_empty desc, i.name
limit 15;
