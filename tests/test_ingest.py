"""接收、重复提交、迟到读数与整批原子性测试。"""
from __future__ import annotations

import pytest

from telemetry_archive import DAY_SECONDS, Reading, TelemetryArchive

from conftest import Reference

NOW = 500 * DAY_SECONDS


def make(miner: str, ts: int, n: int, prefix: str) -> list[Reading]:
    return [
        Reading(
            id=f"{prefix}-{miner}-{i}",
            miner_id=miner,
            ts=ts + i,
            hashrate=100.0 + i,
            power=3000.0 + i,
            temperature=60.0 + i,
        )
        for i in range(n)
    ]


def test_duplicate_ingest_hot_and_cold(archive_path):
    """同一 ID 重投：无论原读数在热区还是冷区，都只保留一条。"""
    ref = Reference()
    cold_rs = make("m1", NOW - 40 * DAY_SECONDS, 5, "c")
    hot_rs = make("m1", NOW - 100, 5, "h")
    ref.ingest(cold_rs + hot_rs)
    with TelemetryArchive(archive_path) as ta:
        ta.ingest(cold_rs + hot_rs, NOW)
        ta.ingest(cold_rs + hot_rs, NOW)  # 全部重投（热区副本）
        ta.maintain(NOW)  # cold_rs 进入冷区
        ta.ingest(cold_rs + hot_rs, NOW)  # 再次重投（冷热混合）
        ta.maintain(NOW)
        got = ta.query("m1", 0, NOW + 1, NOW)
    assert got == ref.query("m1", 0, NOW + 1, NOW)
    assert len(got) == 10


def test_duplicate_within_single_batch(archive_path):
    """同一批内含重复 ID 也只保留一条。"""
    rs = make("m1", NOW - 10, 5, "d")
    with TelemetryArchive(archive_path) as ta:
        ta.ingest(rs + rs, NOW)
        got = ta.query("m1", 0, NOW + 1, NOW)
    assert len(got) == 5


def test_late_reading_into_archived_range(archive_path):
    """向已归档时间段补入迟到读数：立即可查，维护后进入冷层，不覆盖同时间段其他读数。"""
    ref = Reference()
    old = make("m1", NOW - 40 * DAY_SECONDS, 10, "old")
    ref.ingest(old)
    with TelemetryArchive(archive_path) as ta:
        ta.ingest(old, NOW)
        ta.maintain(NOW)  # old 进入归档
        late = make("m1", NOW - 40 * DAY_SECONDS + 3, 5, "late")
        ref.ingest(late)
        ta.ingest(late, NOW)
        # 接收成功后立即可查
        assert ta.query("m1", 0, NOW + 1, NOW) == ref.query("m1", 0, NOW + 1, NOW)
        ta.maintain(NOW)  # 维护后迟到读数进入正确存储层
        got = ta.query("m1", 0, NOW + 1, NOW)
    assert got == ref.query("m1", 0, NOW + 1, NOW)
    assert len(got) == 15  # 原有读数未被覆盖


def test_ingest_rejects_future_and_expired(archive_path):
    with TelemetryArchive(archive_path) as ta:
        with pytest.raises(ValueError):
            ta.ingest(make("m1", NOW + 1, 1, "f"), NOW)
        with pytest.raises(ValueError):
            ta.ingest(make("m1", NOW - 365 * DAY_SECONDS, 1, "e"), NOW)


def test_ingest_batch_atomic_on_crash(archive_path):
    """提交前被终止：整批不生效；无中断时整批生效。"""
    rs = make("m1", NOW - 100, 50, "a")

    def boom(point: str) -> None:
        if point == "ingest_before_commit":
            raise RuntimeError("simulated kill")

    ta = TelemetryArchive(archive_path, fault_hook=boom)
    with pytest.raises(RuntimeError):
        ta.ingest(rs, NOW)
    ta.close()
    with TelemetryArchive(archive_path) as ta2:
        assert ta2.query("m1", 0, NOW + 1, NOW) == []
        ta2.ingest(rs, NOW)
        assert ta2.query("m1", 0, NOW + 1, NOW) == sorted(
            rs, key=lambda r: (r.ts, r.id)
        )


def test_out_of_order_ingest(archive_path):
    """乱序接收不影响读取正确性。"""
    ref = Reference()
    batches = [
        make("m1", NOW - 5 * DAY_SECONDS, 10, "b1"),
        make("m1", NOW - 60 * DAY_SECONDS, 10, "b2"),
        make("m1", NOW - 20 * DAY_SECONDS, 10, "b3"),
    ]
    with TelemetryArchive(archive_path) as ta:
        for b in batches:
            ref.ingest(b)
            ta.ingest(b, NOW)
        ta.maintain(NOW)
        for b in batches:  # 全部重投
            ta.ingest(b, NOW)
        ta.maintain(NOW)
        got = ta.query("m1", 0, NOW + 1, NOW)
    assert got == ref.query("m1", 0, NOW + 1, NOW)
