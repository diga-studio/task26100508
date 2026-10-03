"""30 天与 365 天边界前后 1 秒的测试，维护前后与参考结果逐条比较。"""
from __future__ import annotations

import os
import sqlite3

from telemetry_archive import (
    DAY_SECONDS,
    HOT_AGE_SECONDS,
    RETENTION_SECONDS,
    Reading,
    TelemetryArchive,
)

from conftest import Reference

NOW = 500 * DAY_SECONDS


def make(miner: str, ts: int, n: int, prefix: str) -> list[Reading]:
    return [
        Reading(
            id=f"{prefix}-{miner}-{i}",
            miner_id=miner,
            ts=ts,
            hashrate=100.0 + i,
            power=3000.0 + i,
            temperature=60.0 + i,
        )
        for i in range(n)
    ]


def hot_ids(path: str) -> set[str]:
    conn = sqlite3.connect(os.path.join(path, "hot.sqlite3"))
    try:
        return {r[0] for r in conn.execute("SELECT id FROM hot_readings")}
    finally:
        conn.close()


def cold_files(path: str) -> list[str]:
    out = []
    cold = os.path.join(path, "cold")
    for root, _dirs, files in os.walk(cold):
        out.extend(os.path.join(root, f) for f in files)
    return sorted(out)


def test_hot_cold_boundary(archive_path):
    """年龄 30 天减 1 秒保持热，恰 30 天与 30 天加 1 秒转冷。"""
    ref = Reference()
    readings = (
        make("m1", NOW - HOT_AGE_SECONDS + 1, 2, "warm")
        + make("m1", NOW - HOT_AGE_SECONDS, 2, "edge")
        + make("m1", NOW - HOT_AGE_SECONDS - 1, 2, "cold")
    )
    ref.ingest(readings)
    with TelemetryArchive(archive_path) as ta:
        ta.ingest(readings, NOW)
        before = ta.query("m1", 0, NOW + 1, NOW)
        assert before == ref.query("m1", 0, NOW + 1, NOW)
        ta.maintain(NOW)
        after = ta.query("m1", 0, NOW + 1, NOW)
    assert after == before == ref.query("m1", 0, NOW + 1, NOW)
    remaining_hot = hot_ids(archive_path)
    assert {r.id for r in readings if r.ts > NOW - HOT_AGE_SECONDS} == remaining_hot
    assert cold_files(archive_path), "冷读数应已进入归档文件"


def test_retention_boundary(archive_path):
    """年龄 365 天减 1 秒保留，恰 365 天过期删除。"""
    ref = Reference()
    kept = make("m1", NOW - RETENTION_SECONDS + 1, 2, "kept")
    dropped = make("m1", NOW - RETENTION_SECONDS, 2, "dropped")
    ref.ingest(kept + dropped)
    ingest_now = NOW - 2  # 接收时所有读数均未过期
    with TelemetryArchive(archive_path) as ta:
        ta.ingest(kept + dropped, ingest_now)
        ta.maintain(ingest_now)
        assert ta.query("m1", 0, NOW + 1, NOW) == ref.query("m1", 0, NOW + 1, NOW)
        ta.maintain(NOW)
        got = ta.query("m1", 0, NOW + 1, NOW)
    assert got == ref.query("m1", 0, NOW + 1, NOW)
    assert {r.id for r in kept} == {r.id for r in got}
    # 与保留读数同段时文件仍存在，但过期载荷必须已清除
    from telemetry_archive.coldstore import read_segment

    archived_ids = set()
    for f in cold_files(archive_path):
        archived_ids.update(r.id for r in read_segment(f))
    assert archived_ids == {r.id for r in kept}


def test_fully_expired_segment_file_removed(archive_path):
    """整段过期后，归档文件本身必须被清除。"""
    day_start = 100 * DAY_SECONDS
    now = day_start + RETENTION_SECONDS + DAY_SECONDS
    old = make("m1", day_start, 3, "old")
    with TelemetryArchive(archive_path) as ta:
        ta.ingest(old, day_start + RETENTION_SECONDS - 1)
        ta.maintain(day_start + RETENTION_SECONDS - 1)
        assert cold_files(archive_path)
        ta.maintain(now)
        assert ta.query("m1", 0, now + 1, now) == []
    assert cold_files(archive_path) == []


def test_expiry_boundary_cuts_through_segment(archive_path):
    """过期边界穿过已归档时间段时，仍有效的读数必须保留。"""
    day_start = (NOW - RETENTION_SECONDS) // DAY_SECONDS * DAY_SECONDS
    expire_cutoff = day_start + DAY_SECONDS // 2
    now = expire_cutoff + RETENTION_SECONDS
    old = make("m1", day_start, 3, "old")
    fresh = make("m1", expire_cutoff + 1, 3, "fresh")
    ref = Reference()
    ref.ingest(old + fresh)
    ingest_now = day_start + RETENTION_SECONDS - 1  # 接收时 old 尚未过期
    with TelemetryArchive(archive_path) as ta:
        ta.ingest(old + fresh, ingest_now)
        ta.maintain(ingest_now)  # 两段读数进入同一归档段
        assert len(cold_files(archive_path)) == 1
        ta.maintain(now)  # 过期边界穿过该段
        got = ta.query("m1", 0, now + 1, now)
    assert got == ref.query("m1", 0, now + 1, now)
    assert {r.id for r in got} == {r.id for r in fresh}
    files = cold_files(archive_path)
    assert len(files) == 1, "段重写后不应积累废弃文件"


def test_maintain_is_idempotent(archive_path):
    """重复维护结果不变。"""
    ref = Reference()
    readings = make("m1", NOW - 40 * DAY_SECONDS, 20, "r") + make(
        "m1", NOW - 10 * DAY_SECONDS, 20, "h"
    )
    ref.ingest(readings)
    with TelemetryArchive(archive_path) as ta:
        ta.ingest(readings, NOW)
        ta.maintain(NOW)
        first = ta.query("m1", 0, NOW + 1, NOW)
        ta.maintain(NOW)
        ta.maintain(NOW)
        again = ta.query("m1", 0, NOW + 1, NOW)
    assert first == again == ref.query("m1", 0, NOW + 1, NOW)
