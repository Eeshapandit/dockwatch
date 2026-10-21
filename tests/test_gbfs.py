from gbfs import changed_rows, clean_info_feed, clean_status_feed, remember, state_of


def station(sid, bikes=3, docks=5, installed=1, reported=1792595980):
    return {"station_id": sid, "num_bikes_available": bikes, "num_docks_available": docks,
            "is_installed": installed, "is_renting": 1, "is_returning": 1, "last_reported": reported,
            "legacy_id": "x", "eightd_has_available_keys": False}


def feed(*stations):
    return {"data": {"stations": list(stations)}}


def test_clean_drops_uninstalled_and_converts_types():
    rows = clean_status_feed(feed(station("a"), station("b", installed=0, reported=86400)))
    assert [r["station_id"] for r in rows] == ["a"]
    r = rows[0]
    assert r["is_renting"] is True
    assert r["last_reported"].year == 2026 and r["last_reported"].tzinfo is not None
    assert "legacy_id" not in r and r["num_ebikes_available"] == 0


def test_heartbeat_is_not_a_change():
    last = {}
    first = clean_status_feed(feed(station("a", reported=100_000_000)))
    assert len(changed_rows(first, last)) == 1
    remember(first, last)
    heartbeat = clean_status_feed(feed(station("a", reported=100_000_060)))  # same counts, new time
    assert changed_rows(heartbeat, last) == []


def test_real_change_is_kept_and_state_only_updates_on_remember():
    last = {}
    remember(clean_status_feed(feed(station("a", bikes=3))), last)
    took_a_bike = clean_status_feed(feed(station("a", bikes=2, reported=1792596040)))
    assert len(changed_rows(took_a_bike, last)) == 1
    assert last["a"] == state_of(clean_status_feed(feed(station("a", bikes=3)))[0])  # unchanged until remember


def test_info_feed():
    (r,) = clean_info_feed({"data": {"stations": [
        {"station_id": "s1", "name": "W 52 St & 11 Ave", "short_name": "6926.01", "lat": 40.76, "lon": -73.99, "capacity": 39}]}})
    assert r["short_name"] == "6926.01" and r["capacity"] == 39
