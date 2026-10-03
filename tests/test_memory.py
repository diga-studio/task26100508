"""100 万条读数规模下维护操作的峰值内存验证（额外峰值 <= 128 MiB）。"""
from __future__ import annotations

import gc
import os
import resource

import pytest

from telemetry_archive import DAY_SECONDS, Reading, TelemetryArchive

TOTAL = 1_000_000
MINERS = 2_000
BATCH = 50_000
NOW = 500 * DAY_SECONDS
MEMORY_BUDGET_BYTES = 128 * 1024 * 1024


def peak_rss() -> int:
    return resource.getrusage(resource.RUSAGE_SELF).ru_maxrss


def gen_batch(start: int, n: int):
    out = []
    for i in range(start, start + n):
        miner = f"miner-{i % MINERS}"
        # 均匀分布在最近一年内，保证既有热读数也有大量冷读数
        ts = NOW - (i % 364) * DAY_SECONDS - (i % 1400)
        out.append(
            Reading(
                id=f"r-{i}",
                miner_id=miner,
                ts=ts,
                hashrate=100.0 + (i % 50),
                power=3000.0 + (i % 100),
                temperature=60.0 + (i % 30),
            )
        )
    return out


@pytest.mark.slow
def test_maintain_memory_one_million(archive_path):
    with TelemetryArchive(archive_path) as ta:
        for start in range(0, TOTAL, BATCH):
            ta.ingest(gen_batch(start, min(BATCH, TOTAL - start)), NOW)
        gc.collect()
        before = peak_rss()
        ta.maintain(NOW)
        after = peak_rss()
        extra = after - before
        assert extra < MEMORY_BUDGET_BYTES, (
            f"维护峰值内存超出预算: {extra / 1024 / 1024:.1f} MiB"
        )
        # 抽查若干矿机，确认数据完整可读
        for miner in ("miner-0", "miner-7", "miner-1999"):
            got = ta.query(miner, 0, NOW + 1, NOW)
            expected = TOTAL // MINERS
            assert len(got) == expected
            assert all(r.miner_id == miner for r in got)
    # 维护后热区不应保留冷读数的完整副本
    import sqlite3

    conn = sqlite3.connect(os.path.join(archive_path, "hot.sqlite3"))
    try:
        hot_count = conn.execute("SELECT COUNT(*) FROM hot_readings").fetchone()[0]
        cold_count = conn.execute("SELECT COUNT(*) FROM cold_ids").fetchone()[0]
    finally:
        conn.close()
    assert hot_count + cold_count == TOTAL
    assert cold_count > 0
    # 热区只剩年龄不足 30 天的读数
    assert hot_count <= TOTAL * 31 // 364 + MINERS
