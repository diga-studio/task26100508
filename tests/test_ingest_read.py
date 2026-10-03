"""Ingest/read semantics: dedupe, late arrivals, ranges, re-submission."""

from __future__ import annotations

from telemetry_store import Reading

from conftest import DAY, Reference, make_reading

NOW = 500 * DAY


def test_same_miner_same_ts_different_ids(store, rng):
    readings = [make_reading(rng, f"id-{i}", "m1", NOW - 100) for i in range(3)]
    assert store.ingest(readings, NOW) == 3
    got = store.read_range("m1", 0, NOW + 1, NOW)
    assert {r.reading_id for r in got} == {"id-0", "id-1", "id-2"}


def test_duplicate_ingest_hot_and_cold(store, rng):
    hot = make_reading(rng, "hot-1", "m1", NOW - 10 * DAY)
    cold = make_reading(rng, "cold-1", "m1", NOW - 60 * DAY)
    assert store.ingest([hot, cold], NOW) == 2
    # Duplicate while both are still hot.
    assert store.ingest([hot, cold], NOW) == 0
    store.maintain(NOW)  # cold-1 moves to the archive layer
    # Duplicate again: one is hot, one is archived. Neither is re-stored.
    assert store.ingest([hot, cold], NOW) == 0
    got = store.read_range("m1", 0, NOW + 1, NOW)
    assert got == sorted([hot, cold], key=lambda r: (r.ts, r.reading_id))


def test_read_range_is_left_closed_right_open(store, rng):
    readings = [make_reading(rng, f"t{i}", "m1", NOW - i * 1000) for i in range(5)]
    store.ingest(readings, NOW)
    start = readings[3].ts
    end = readings[1].ts
    got = store.read_range("m1", start, end, NOW)
    assert [r.reading_id for r in got] == ["t3", "t2"]


def test_expired_readings_never_returned(store, rng):
    old = make_reading(rng, "old", "m1", NOW - 300 * DAY)
    store.ingest([old], NOW)
    later = NOW + 100 * DAY  # old is now 400 days old -> expired
    assert store.read_range("m1", 0, later + 1, later) == []


def test_late_reading_into_archived_range(store, rng):
    ref = Reference()
    base = [
        make_reading(rng, f"base-{i}", "m1", NOW - (40 + i) * DAY) for i in range(10)
    ]
    store.ingest(base, NOW)
    ref.ingest(base)
    store.maintain(NOW)
    assert store._conn.execute("SELECT COUNT(*) FROM readings").fetchone()[0] == 0

    # Late reading lands inside the already-archived time range.
    late = make_reading(rng, "late-1", "m1", NOW - 45 * DAY)
    assert store.ingest([late], NOW) == 1
    ref.ingest([late])

    # Visible immediately, before any maintenance.
    got = store.read_range("m1", 0, NOW + 1, NOW)
    assert got == ref.expect("m1", 0, NOW + 1, NOW)
    assert late in got

    # Late reading does not clobber its archived neighbours.
    neighbours = {r.reading_id for r in got}
    assert {"base-4", "base-5"} <= neighbours

    # After the next maintenance it joins the cold layer; results unchanged.
    store.maintain(NOW)
    assert store._conn.execute("SELECT COUNT(*) FROM readings").fetchone()[0] == 0
    got2 = store.read_range("m1", 0, NOW + 1, NOW)
    assert got2 == got


def test_late_reading_merges_into_single_segment_per_miner(store, rng):
    first = [make_reading(rng, f"a-{i}", "m1", NOW - (40 + i) * DAY) for i in range(5)]
    store.ingest(first, NOW)
    store.maintain(NOW)
    second = [make_reading(rng, f"b-{i}", "m1", NOW - (42 + i) * DAY) for i in range(3)]
    store.ingest(second, NOW)
    store.maintain(NOW)
    segs = store._conn.execute(
        "SELECT COUNT(*) FROM segments WHERE miner_id = 'm1'"
    ).fetchone()[0]
    assert segs == 1  # overlapping ranges merged, no duplicate copies
    cold = store._conn.execute("SELECT COUNT(*) FROM cold_ids").fetchone()[0]
    assert cold == 8


def test_ingest_empty_batch(store):
    assert store.ingest([], NOW) == 0


def test_out_of_order_ingest(store, rng):
    ref = Reference()
    for batch_no in range(5):
        batch = [
            make_reading(rng, f"o-{batch_no}-{i}", "m1", NOW - rng.randint(1, 300) * DAY)
            for i in range(20)
        ]
        store.ingest(batch, NOW)
        ref.ingest(batch)
        if batch_no % 2 == 0:
            store.maintain(NOW)
        got = store.read_range("m1", 0, NOW + 1, NOW)
        assert got == ref.expect("m1", 0, NOW + 1, NOW)
