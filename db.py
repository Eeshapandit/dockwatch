"""Shared database settings and SQL, used by the streaming and batch code.

Inside Docker the database host is "postgres" (the compose service name);
on your Codespace terminal it is "localhost". PG_DSN lets one file serve both.
"""
import os

DSN = os.getenv("PG_DSN", "postgresql://dockwatch:dockwatch@localhost:5432/dockwatch")

STATUS_INSERT_SQL = """
insert into raw.station_status (
    station_id, num_bikes_available, num_ebikes_available, num_bikes_disabled,
    num_docks_available, num_docks_disabled, is_installed, is_renting,
    is_returning, last_reported
) values (
    %(station_id)s, %(num_bikes_available)s, %(num_ebikes_available)s, %(num_bikes_disabled)s,
    %(num_docks_available)s, %(num_docks_disabled)s, %(is_installed)s, %(is_renting)s,
    %(is_returning)s, %(last_reported)s
)
on conflict (station_id, last_reported) do nothing
"""

INFO_UPSERT_SQL = """
insert into raw.station_information
    (station_id, name, short_name, lat, lon, capacity, region_id)
values
    (%(station_id)s, %(name)s, %(short_name)s, %(lat)s, %(lon)s, %(capacity)s, %(region_id)s)
on conflict (station_id) do update set
    name = excluded.name, short_name = excluded.short_name, lat = excluded.lat,
    lon = excluded.lon, capacity = excluded.capacity, region_id = excluded.region_id,
    updated_at = now()
where (raw.station_information.name, raw.station_information.lat,
       raw.station_information.lon, raw.station_information.capacity)
   is distinct from (excluded.name, excluded.lat, excluded.lon, excluded.capacity)
"""
