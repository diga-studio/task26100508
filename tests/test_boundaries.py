"""Boundary tests: 30-day hot/cold edge and 365-day expiry edge, +/-1 second."""

from __future__ import annotations

from telemetry_store import (
    EXPIRE_AGE_SECONDS,
    HOT_MAX_AGE_SECONDS,
    Reading,
    TelemetryStore,
)

from conftest import DAY, Reference, make_reading

NOW = 500 * DAY


def _hot_count(store: TelemetryStore) -> int:
    return store._conn.execute("SELECT COUNT(*) FROM readings").fetchone()[0]


def _cold_count(store: TelemetryStore) -> int:
    return store._conn.execute("SELECT COUNT(*) FROM cold_ids").fetchone()[0]


def test_hot_cold_boundary_plus_minus_one_second(store, rng):
    """age < 30d stays hot; age >= 30d (exactly, and +1s) becomes cold."""
    readings = [
        make_reading(rng, "hot-edge+1s", "m1", NOW - HOT_MAX_AGE_SECONDS + 1),
        make_reading(rng, "cold-edge", "m1", NOW - HOT_MAX_AGE_SECONDS),
        make_reading(rng, "cold-edge-1s", "m1", NOW - HOT_MAX_AGE_SECONDS - 1),
    ]
    assert store.ingest(readings, NOW) == 3
    store.maintain(NOW)

    assert _hot_count(store) == 1
    assert _cold_count(store) == 2
    hot_ids = {row[0] for row in store._conn.execute("SELECT id FROM readings")}
    assert hot_ids == {"hot-edge+1s"}

    got = store.read_range("m1", 0, NOW + 1, NOW)
    assert got == sorted(readings, key=lambda r: (r.ts, r.reading_id))


def test_expiry_boundary_plus_minus_one_second(store, rng):
    """age < 365d survives; age >= 365d (exactly, and +1s) is purged."""
    t0 = NOW
    readings = [
        make_reading(rng, "live-edge", "m1", t0 - 100 * DAY),
        make_reading(rng, "expire-edge", "m1", t0 - 100 * DAY - 1),
        make_reading(rng, "expire-edge-1s", "m1", t0 - 100 * DAY - 2),
    ]
    assert store.ingest(readings, t0) == 3
    store.maintain(t0)  # all three are cold -> archived
    assert _cold_count(store) == 3

    # Advance time so the three readings straddle the 365-day expiry line.
    now1 = readings[0].ts + EXPIRE_AGE_SECONDS - 1  # readings[0] age = 365d-1s
    report = store.maintain(now1)
    assert report.expired_readings == 2
    got = store.read_range("m1", 0, now1 + 1, now1)
    assert [r.reading_id for r in got] == ["live-edge"]

    # One more second and the last reading expires too.
    report = store.maintain(now1 + 1)
    assert report.expired_readings == 1
    assert store.read_range("m1", 0, now1 + 2, now1 + 1) == []


def test_read_results_identical_across_maintenance(store, rng):
    """Reference comparison: reads before/after maintain must match exactly."""
    ref = Reference()
    miners = [f"miner-{i}" for i in range(20)]
    for batch_no in range(10):
        batch = [
            make_reading(
                rng,
                f"r-{batch_no}-{i}",
                rng.choice(miners),
                NOW - rng.randint(0, 400 * DAY),
            )
            for i in range(200)
        ]
        # Drop readings that would already be expired at ingest time.
        batch = [r for r in batch if r.ts > NOW - EXPIRE_AGE_SECONDS]
        store.ingest(batch, NOW)
        ref.ingest(batch)

    for miner in miners:
        start = NOW - rng.randint(0, 400) * DAY
        end = start + rng.randint(1, 90) * DAY
        before = store.read_range(miner, start, end, NOW)
        assert before == ref.expect(miner, start, end, NOW)

    store.maintain(NOW)

    for miner in miners:
        start = NOW - rng.randint(0, 400) * DAY
        end = start + rng.randint(1, 90) * DAY
        after = store.read_range(miner, start, end, NOW)
        assert after == ref.expect(miner, start, end, NOW)


def test_expiry_cutting_through_archived_segment(store, rng, tmp_path):
    """Expiry boundary inside an archived segment keeps still-valid readings."""
    t0 = NOW
    # One reading per day, 40 consecutive days -> archived into one segment.
    readings = [
        make_reading(rng, f"d{i}", "m1", t0 - (50 + i) * DAY) for i in range(40)
    ]
    store.ingest(readings, t0)
    store.maintain(t0)
    assert _cold_count(store) == 40
    segs = store._conn.execute("SELECT COUNT(*) FROM segments").fetchone()[0]
    assert segs == 1

    # Advance so the expiry line lands in the middle of the segment.
    now1 = readings[19].ts + EXPIRE_AGE_SECONDS  # readings[19] exactly expires
    store.maintain(now1)

    ref = Reference()
    ref.ingest(readings)
    got = store.read_range("m1", 0, now1 + 1, now1)
    assert got == ref.expect("m1", 0, now1 + 1, now1)
    assert len(got) == 19  # d19 is exactly 365 days old -> expired
    assert [r.reading_id for r in got] == [f"d{i}" for i in reversed(range(19))]
    assert _hot_count(store) == 0
    assert _cold_count(store) == 19
