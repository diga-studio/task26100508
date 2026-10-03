"""Scale test: 1M readings / 2000 miners, maintain peak memory <= 128 MiB."""

from __future__ import annotations

import gc
import resource
import sys

from telemetry_store import Reading, TelemetryStore

DAY = 86400
NOW = 500 * DAY
N_READINGS = 1_000_000
N_MINERS = 2000
BATCH = 50_000
MAX_EXTRA_BYTES = 128 * 1024 * 1024


def _reading(i: int) -> Reading:
    return Reading(
        reading_id=f"r{i}",
        miner_id=f"miner-{i % N_MINERS}",
        ts=NOW - (i % 340) * DAY - (i * 37) % DAY,
        hashrate=90.0 + (i % 2500) / 100.0,
        power=2800.0 + (i % 7000) / 10.0,
        temperature=55.0 + (i % 3000) / 100.0,
    )


def _rss_bytes() -> int:
    usage = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    # macOS reports bytes, Linux reports kilobytes.
    return usage if sys.platform == "darwin" else usage * 1024


def test_maintain_memory_at_one_million_readings(tmp_path):
    store = TelemetryStore.open(tmp_path / "db")
    for start in range(0, N_READINGS, BATCH):
        store.ingest((_reading(i) for i in range(start, start + BATCH)), NOW)

    gc.collect()
    baseline = _rss_bytes()
    report = store.maintain(NOW)
    peak_extra = _rss_bytes() - baseline

    assert report.archived_readings > 800_000  # most readings are cold
    assert peak_extra <= MAX_EXTRA_BYTES, f"maintain used {peak_extra / 2**20:.1f} MiB extra"

    # A second maintain far in the future exercises mass expiry + rewrites.
    gc.collect()
    baseline2 = _rss_bytes()
    report2 = store.maintain(NOW + 200 * DAY)
    peak_extra2 = _rss_bytes() - baseline2
    assert report2.expired_readings > 300_000
    assert peak_extra2 <= MAX_EXTRA_BYTES, f"expiry used {peak_extra2 / 2**20:.1f} MiB extra"

    # Spot-check correctness for a few miners against regenerated input.
    now2 = NOW + 200 * DAY
    for miner_no in (0, 7, 1234, 1999):
        miner = f"miner-{miner_no}"
        expected = sorted(
            (
                _reading(i)
                for i in range(miner_no, N_READINGS, N_MINERS)
                if _reading(i).ts > now2 - 365 * DAY
            ),
            key=lambda r: (r.ts, r.reading_id),
        )
        got = store.read_range(miner, 0, now2 + 1, now2)
        assert got == expected
    store.close()
