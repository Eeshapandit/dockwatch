-- How many rides start in each hour, weekdays vs weekends. The commute shape.
select
    start_hour as hour,
    count(*) filter (where is_weekday)      as weekday_trips,
    count(*) filter (where not is_weekday)  as weekend_trips,
    count(distinct started_at::date) filter (where is_weekday)     as weekday_days,
    count(distinct started_at::date) filter (where not is_weekday) as weekend_days
from {{ ref('stg_trips') }}
group by start_hour
