"""归档、过期清理途中强制终止后重开与连续重试的测试。"""
from __future__ import annotations

import os
import sqlite3
import subprocess
import sys

import pytest

from telemetry_archive import DAY_SECONDS, Reading, TelemetryArchive

from conftest import Reference

NOW0 = 500 * DAY_SECONDS
NOW1 = NOW0 + 10 * DAY_SECONDS

HOOK_POINTS = [
    "expire_hot_committed",
    "cold_file_unlinked_pending",
    "segment_written",
    "segment_committed",
    "archive_hot_done",
]


def make(miner: str, ts: int, n: int, prefix: str) -> list[Reading]:
    return [
        Reading(
            id=f"{prefix}-{miner}-{i}",
            miner_id=miner,
            ts=ts + i * 60,
            hashrate=100.0 + i,
            power=3000.0 + i,
            temperature=60.0 + i,
        )
        for i in range(n)
    ]


def build_store(path: str, ref: Reference) -> None:
    """构造包含热、冷、将过期读数的库，并在 NOW0 做一次成功维护。"""
    batches = [
        make("m1", NOW0 - 364 * DAY_SECONDS, 8, "soon-expired"),
        make("m1", NOW0 - 40 * DAY_SECONDS, 20, "cold-a"),
        make("m2", NOW0 - 50 * DAY_SECONDS, 20, "cold-b"),
        make("m1", NOW0 - 35 * DAY_SECONDS, 20, "cold-c"),
        make("m1", NOW0 - 25 * DAY_SECONDS, 20, "aging-a"),
        make("m2", NOW0 - 28 * DAY_SECONDS, 20, "aging-b"),
        make("m2", NOW0 - 2 * DAY_SECONDS, 10, "hot"),
    ]
    with TelemetryArchive(path) as ta:
        for b in batches:
            ref.ingest(b)
            ta.ingest(b, NOW0)
        ta.maintain(NOW0)
        assert ta.query("m1", 0, NOW1 + 1, NOW1) == ref.query(
            "m1", 0, NOW1 + 1, NOW1
        )
    # NOW1 时刻补入落入已归档时间段的迟到读数，触发段合并路径
    late = make("m1", NOW0 - 40 * DAY_SECONDS + 3000, 5, "late")
    ref.ingest(late)
    with TelemetryArchive(path) as ta:
        ta.ingest(late, NOW1)


def cold_files_on_disk(path: str) -> list[str]:
    out = []
    cold = os.path.join(path, "cold")
    if not os.path.isdir(cold):
        return out
    for root, _dirs, files in os.walk(cold):
        out.extend(os.path.join(root, f) for f in files)
    return sorted(out)


def manifest_files(path: str) -> list[str]:
    conn = sqlite3.connect(os.path.join(path, "hot.sqlite3"))
    try:
        return sorted(r[0] for r in conn.execute("SELECT path FROM archive_files"))
    finally:
        conn.close()


def tmp_files(path: str) -> list[str]:
    tmp = os.path.join(path, "tmp")
    return os.listdir(tmp) if os.path.isdir(tmp) else []


def assert_consistent(path: str, ref: Reference, now: int) -> None:
    """已确认读数不丢不重，磁盘与清单一致，无废弃文件。"""
    with TelemetryArchive(path) as ta:
        for miner in ("m1", "m2"):
            assert ta.query(miner, 0, now + 1, now) == ref.query(
                miner, 0, now + 1, now
            )
    assert cold_files_on_disk(path) == manifest_files(path)
    assert tmp_files(path) == []


@pytest.mark.parametrize("point", HOOK_POINTS)
def test_crash_at_each_point_then_recover(archive_path, point):
    """维护在各故障点中断：重开后数据不丢不重，重试成功，废弃文件被清除。"""
    ref = Reference()
    build_store(archive_path, ref)
    crashed_once = False

    def hook(p: str) -> None:
        nonlocal crashed_once
        if p == point and not crashed_once:
            crashed_once = True
            raise RuntimeError("simulated kill")

    ta = TelemetryArchive(archive_path, fault_hook=hook)
    try:
        ta.maintain(NOW1)
    except RuntimeError:
        pass
    ta.close()
    assert crashed_once, f"故障点 {point} 未被触发"
    # 重开即可读，且结果与参考一致
    assert_consistent(archive_path, ref, NOW1)
    # 重试维护成功，结果仍一致，无废弃文件
    with TelemetryArchive(archive_path) as ta2:
        ta2.maintain(NOW1)
    assert_consistent(archive_path, ref, NOW1)


def test_repeated_crashes_do_not_accumulate_waste(archive_path):
    """连续中断、重开、重试多轮后，不积累废弃副本。"""
    ref = Reference()
    build_store(archive_path, ref)
    rounds = 6
    for i in range(rounds):
        state = {"count": 0}

        def hook(p: str) -> None:
            if p == "segment_written":
                state["count"] += 1
                if state["count"] > i % 3:
                    raise RuntimeError("simulated kill")

        ta = TelemetryArchive(archive_path, fault_hook=hook)
        try:
            ta.maintain(NOW1)
        except RuntimeError:
            pass
        ta.close()
        assert_consistent(archive_path, ref, NOW1)
    with TelemetryArchive(archive_path) as ta:
        ta.maintain(NOW1)
    assert_consistent(archive_path, ref, NOW1)


@pytest.mark.parametrize("point", HOOK_POINTS)
def test_hard_kill_subprocess(archive_path, point):
    """子进程在维护途中被 os._exit 强杀：重开后数据完整，重试成功。"""
    ref = Reference()
    build_store(archive_path, ref)
    worker = os.path.join(os.path.dirname(__file__), "crash_worker.py")
    proc = subprocess.run(
        [sys.executable, worker, archive_path, point, str(NOW1)],
        capture_output=True,
    )
    assert proc.returncode != 0, f"故障点 {point} 未触发强杀"
    assert_consistent(archive_path, ref, NOW1)
    with TelemetryArchive(archive_path) as ta:
        ta.maintain(NOW1)
    assert_consistent(archive_path, ref, NOW1)
