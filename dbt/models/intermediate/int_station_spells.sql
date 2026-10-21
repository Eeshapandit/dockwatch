-- Turn snapshots into SPELLS: "station X was in this state from A until B".
-- Because we only store real changes (Step 6), a state lasts until the
-- station's next stored row. LEAD() peeks at that next row.
--
-- Open spells (the latest row) end where the pipeline last saw ANY data,
-- and no spell may last more than 12 hours, so a pipeline outage can't
-- invent a 3 day empty spell.
with ordered as (
    select
        *,
        lead(last_reported) over (partition by station_id order by last_reported) as next_reported
    from {{ ref('stg_station_status') }}
),
bounds as (
    select max(last_reported) as seen_until from {{ ref('stg_station_status') }}
)
select
    o.station_id,
    o.last_reported as spell_start,
    least(coalesce(o.next_reported, b.seen_until), o.last_reported + interval '12 hours') as spell_end,
    o.num_bikes_available,
    o.num_docks_available,
    (o.num_bikes_available = 0 and o.is_renting)   as is_empty,
    (o.num_docks_available = 0 and o.is_returning) as is_full
from ordered o
cross join bounds b
where least(coalesce(o.next_reported, b.seen_until), o.last_reported + interval '12 hours') > o.last_reported
