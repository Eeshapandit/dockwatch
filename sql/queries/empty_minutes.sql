-- Which stations were EMPTY the longest in the last 30 minutes?
--
-- Each row only says "at time T, station had N bikes". To get a DURATION
-- we need the NEXT snapshot's time: the state lasted from this report
-- until the next one. LEAD() is a window function that peeks at the next
-- row within each station's own timeline (partition by station_id).

with timeline as (
    select
        station_id,
        last_reported,
        num_bikes_available,
        is_renting,
        lead(last_reported) over (
            partition by station_id   -- each station is its own timeline
            order by last_reported    -- in time order
        ) as next_reported            -- null for the latest row: still in that state
    from raw.station_status
),
empty_spells as (
    select
        station_id,
        -- clamp to the 30 minute window so old states don't inflate totals
        greatest(last_reported, now() - interval '30 minutes') as spell_start,
        coalesce(next_reported, now()) as spell_end
    from timeline
    where num_bikes_available = 0
      and is_renting
      and coalesce(next_reported, now()) > now() - interval '30 minutes'
)
select
    station_id,
    round(sum(extract(epoch from spell_end - spell_start)) / 60.0, 1) as minutes_empty
from empty_spells
group by station_id
order by minutes_empty desc
limit 10;
