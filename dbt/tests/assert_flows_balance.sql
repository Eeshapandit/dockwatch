-- Every weekday ride is one departure and one arrival, so across all
-- stations and hours, departures and arrivals must add up to the same total.
select sum(departures) as d, sum(arrivals) as a
from {{ ref('mart_station_net_flow') }}
having sum(departures) <> sum(arrivals)
