"""矿场遥测冷热归档库主入口。"""
from __future__ import annotations

import os
import shutil
import sqlite3
from urllib.parse import quote
from typing import Callable, Iterable, List, Optional

from . import coldstore, db
from .models import Reading

DAY_SECONDS = 86400
HOT_AGE_SECONDS = 30 * DAY_SECONDS
RETENTION_SECONDS = 365 * DAY_SECONDS

_INGEST_CHUNK = 10000


class TelemetryArchive:
    """热数据存 SQLite，冷数据存本地压缩归档文件的遥测库。

    所有调用串行执行；进程在任意位置被强制终止后，
    重新打开即可继续读取已确认接收且未过期的读数。
    """

    def __init__(
        self,
        path: str,
        fault_hook: Optional[Callable[[str], None]] = None,
    ) -> None:
        self._root = path
        self._cold_dir = os.path.join(path, "cold")
        self._tmp_dir = os.path.join(path, "tmp")
        os.makedirs(self._cold_dir, exist_ok=True)
        os.makedirs(self._tmp_dir, exist_ok=True)
        self._conn = db.connect(os.path.join(path, "hot.sqlite3"))
        self._fault_hook = fault_hook
        self._recover()

    # ---- 内部工具 ----

    def _hook(self, point: str) -> None:
        if self._fault_hook is not None:
            self._fault_hook(point)

    def _miner_dir(self, miner_id: str) -> str:
        return os.path.join(self._cold_dir, quote(miner_id, safe=""))

    def _recover(self) -> None:
        """重开时清理中断遗留产物：临时文件与不在清单中的孤儿段文件。"""
        for name in os.listdir(self._tmp_dir):
            os.unlink(os.path.join(self._tmp_dir, name))
        known = {
            row[0]
            for row in self._conn.execute("SELECT path FROM archive_files")
        }
        for miner_dir in os.listdir(self._cold_dir):
            miner_path = os.path.join(self._cold_dir, miner_dir)
            if not os.path.isdir(miner_path):
                continue
            for name in os.listdir(miner_path):
                full = os.path.join(miner_path, name)
                if full not in known:
                    os.unlink(full)
            if not os.listdir(miner_path):
                os.rmdir(miner_path)

    # ---- 接收 ----

    def ingest(self, readings: Iterable[Reading], now: int) -> None:
        """批量接收读数；整批要么全部生效，要么全部不生效。"""
        expire_cutoff = now - RETENTION_SECONDS
        batch: dict[str, Reading] = {}
        for r in readings:
            if r.ts > now:
                raise ValueError(f"未来读数: {r.id}")
            if r.ts <= expire_cutoff:
                raise ValueError(f"已过期读数: {r.id}")
            batch.setdefault(r.id, r)
        if not batch:
            return
        ids = list(batch)
        existing: set[str] = set()
        for i in range(0, len(ids), _INGEST_CHUNK):
            chunk = ids[i : i + _INGEST_CHUNK]
            marks = ",".join("?" * len(chunk))
            existing.update(
                row[0]
                for row in self._conn.execute(
                    f"SELECT id FROM hot_readings WHERE id IN ({marks})", chunk
                )
            )
            existing.update(
                row[0]
                for row in self._conn.execute(
                    f"SELECT id FROM cold_ids WHERE id IN ({marks})", chunk
                )
            )
        fresh = [r for rid, r in batch.items() if rid not in existing]
        if not fresh:
            return
        try:
            self._conn.execute("BEGIN")
            for i in range(0, len(fresh), _INGEST_CHUNK):
                self._conn.executemany(
                    "INSERT INTO hot_readings (id, miner_id, ts, hashrate, power,"
                    " temperature) VALUES (?, ?, ?, ?, ?, ?)",
                    [
                        (r.id, r.miner_id, r.ts, r.hashrate, r.power, r.temperature)
                    for r in fresh[i : i + _INGEST_CHUNK]
                    ],
                )
            self._hook("ingest_before_commit")
            self._conn.commit()
        except BaseException:
            self._conn.rollback()
            raise

    # ---- 读取 ----

    def query(self, miner_id: str, start: int, end: int, now: int) -> List[Reading]:
        """按矿机与时间区间 [start, end) 读取未过期读数，每个 ID 出现一次。"""
        cutoff = now - RETENTION_SECONDS
        lo = max(start, cutoff + 1)
        if end <= lo:
            return []
        merged: dict[str, Reading] = {}
        for row in self._conn.execute(
            "SELECT id, ts, hashrate, power, temperature FROM hot_readings"
            " WHERE miner_id = ? AND ts >= ? AND ts < ?",
            (miner_id, lo, end),
        ):
            rid, ts, hashrate, power, temperature = row
            merged[rid] = Reading(rid, miner_id, ts, hashrate, power, temperature)
        miner_dir = self._miner_dir(miner_id)
        for (rel_path,) in self._conn.execute(
            "SELECT path FROM archive_files WHERE miner_id = ?"
            " AND day_start < ? AND day_start + ? > ?",
            (miner_id, end, DAY_SECONDS, lo),
        ):
            for r in coldstore.read_segment(rel_path):
                if lo <= r.ts < end and r.id not in merged:
                    merged[r.id] = r
        return sorted(merged.values(), key=lambda r: (r.ts, r.id))

    # ---- 归档清理 ----

    def maintain(self, now: int) -> None:
        """执行归档清理：热转冷、过期删除、清理中断遗留产物。"""
        expire_cutoff = now - RETENTION_SECONDS
        hot_cutoff = now - HOT_AGE_SECONDS
        self._expire_hot(expire_cutoff)
        self._expire_cold(expire_cutoff)
        self._archive_hot(hot_cutoff)
        self._hook("maintain.done")

    def _expire_hot(self, expire_cutoff: int) -> None:
        self._conn.execute("BEGIN")
        try:
            self._conn.execute(
                "DELETE FROM hot_readings WHERE ts <= ?", (expire_cutoff,)
            )
            self._conn.commit()
        except BaseException:
            self._conn.rollback()
            raise
        self._hook("expire_hot_committed")

    def _remove_cold_file(self, path: str) -> None:
        """清单与 ID 索引先提交，再删文件；中断后由重开恢复清理孤儿文件。"""
        self._conn.execute("BEGIN")
        try:
            self._conn.execute("DELETE FROM archive_files WHERE path = ?", (path,))
            self._conn.execute("DELETE FROM cold_ids WHERE file_path = ?", (path,))
            self._conn.commit()
        except BaseException:
            self._conn.rollback()
            raise
        self._hook("cold_file_unlinked_pending")
        try:
            os.unlink(path)
        except FileNotFoundError:
            pass

    def _expire_cold(self, expire_cutoff: int) -> None:
        rows = self._conn.execute(
            "SELECT path, miner_id, day_start FROM archive_files"
            " WHERE day_start <= ?",
            (expire_cutoff,),
        ).fetchall()
        for path, miner_id, day_start in rows:
            day_end = day_start + DAY_SECONDS
            if day_end - 1 <= expire_cutoff:
                self._remove_cold_file(path)
            elif day_start <= expire_cutoff:
                survivors = [
                    r for r in coldstore.read_segment(path) if r.ts > expire_cutoff
                ]
                if not survivors:
                    self._remove_cold_file(path)
                else:
                    self._rewrite_segment(miner_id, day_start, path, survivors)
        self._hook("expire_cold_done")

    def _rewrite_segment(
        self, miner_id: str, day_start: int, old_path: str, readings: List[Reading]
    ) -> None:
        old_name = os.path.basename(old_path)
        old_gen = int(old_name.split(".")[1])
        new_path = os.path.join(
            self._miner_dir(miner_id), coldstore.segment_name(day_start, old_gen + 1)
        )
        coldstore.write_segment(self._tmp_dir, new_path, readings)
        self._hook("segment_written")
        self._conn.execute("BEGIN")
        try:
            self._conn.execute(
                "INSERT INTO archive_files (path, miner_id, day_start)"
                " VALUES (?, ?, ?)",
                (new_path, miner_id, day_start),
            )
            self._conn.execute(
                "DELETE FROM archive_files WHERE path = ?", (old_path,)
            )
            self._conn.execute(
                "UPDATE cold_ids SET file_path = ? WHERE file_path = ?",
                (new_path, old_path),
            )
            self._conn.commit()
        except BaseException:
            self._conn.rollback()
            raise
        self._hook("segment_committed")
        try:
            os.unlink(old_path)
        except FileNotFoundError:
            pass

    def _archive_hot(self, hot_cutoff: int) -> None:
        miners = self._conn.execute(
            "SELECT DISTINCT miner_id FROM hot_readings WHERE ts <= ?",
            (hot_cutoff,),
        ).fetchall()
        for (miner_id,) in miners:
            days = self._conn.execute(
                "SELECT DISTINCT ts / ? FROM hot_readings"
                " WHERE miner_id = ? AND ts <= ?",
                (DAY_SECONDS, miner_id, hot_cutoff),
            ).fetchall()
            for (day,) in days:
                day_start = day * DAY_SECONDS
                self._archive_group(miner_id, day_start, hot_cutoff)
        self._hook("archive_hot_done")

    def _archive_group(
        self, miner_id: str, day_start: int, hot_cutoff: int
    ) -> None:
            rows = self._conn.execute(
                "SELECT id, ts, hashrate, power, temperature FROM hot_readings"
                " WHERE miner_id = ? AND ts >= ? AND ts < ? AND ts <= ?",
                (miner_id, day_start, day_start + DAY_SECONDS, hot_cutoff),
            ).fetchall()
            if not rows:
                return
            new_readings = [
                Reading(rid, miner_id, ts, hr, pw, tp)
                for rid, ts, hr, pw, tp in rows
            ]
            existing = self._conn.execute(
                "SELECT path FROM archive_files WHERE miner_id = ? AND day_start = ?",
                (miner_id, day_start),
            ).fetchone()
            if existing is not None:
                merged: dict[str, Reading] = {
                    r.id: r for r in coldstore.read_segment(existing[0])
                }
                for r in new_readings:
                    merged.setdefault(r.id, r)
                self._rewrite_and_absorb(
                    miner_id, day_start, existing[0], list(merged.values()),
                    [r.id for r in new_readings],
                )
            else:
                self._create_segment(miner_id, day_start, new_readings)

    def _create_segment(
        self, miner_id: str, day_start: int, readings: List[Reading]
    ) -> None:
        path = os.path.join(
            self._miner_dir(miner_id), coldstore.segment_name(day_start, 0)
        )
        coldstore.write_segment(self._tmp_dir, path, readings)
        self._hook("segment_written")
        self._conn.execute("BEGIN")
        try:
            self._conn.execute(
                "INSERT INTO archive_files (path, miner_id, day_start)"
                " VALUES (?, ?, ?)",
                (path, miner_id, day_start),
            )
            self._conn.executemany(
                "INSERT INTO cold_ids (id, file_path) VALUES (?, ?)",
                [(r.id, path) for r in readings],
            )
            self._conn.executemany(
                "DELETE FROM hot_readings WHERE id = ?",
                [(r.id,) for r in readings],
            )
            self._conn.commit()
        except BaseException:
            self._conn.rollback()
            raise
        self._hook("segment_committed")

    def _rewrite_and_absorb(
        self,
        miner_id: str,
        day_start: int,
        old_path: str,
        merged: List[Reading],
        new_ids: List[str],
    ) -> None:
        old_name = os.path.basename(old_path)
        old_gen = int(old_name.split(".")[1])
        new_path = os.path.join(
            self._miner_dir(miner_id), coldstore.segment_name(day_start, old_gen + 1)
        )
        coldstore.write_segment(self._tmp_dir, new_path, merged)
        self._hook("segment_written")
        self._conn.execute("BEGIN")
        try:
            self._conn.execute(
                "INSERT INTO archive_files (path, miner_id, day_start)"
                " VALUES (?, ?, ?)",
                (new_path, miner_id, day_start),
            )
            self._conn.execute(
                "DELETE FROM archive_files WHERE path = ?", (old_path,)
            )
            self._conn.execute(
                "UPDATE cold_ids SET file_path = ? WHERE file_path = ?",
                (new_path, old_path),
            )
            self._conn.executemany(
                "INSERT INTO cold_ids (id, file_path) VALUES (?, ?)",
                [(rid, new_path) for rid in new_ids],
            )
            self._conn.executemany(
                "DELETE FROM hot_readings WHERE id = ?",
                [(rid,) for rid in new_ids],
            )
            self._conn.commit()
        except BaseException:
            self._conn.rollback()
            raise
        self._hook("segment_committed")
        try:
            os.unlink(old_path)
        except FileNotFoundError:
            pass

    def close(self) -> None:
        self._conn.close()

    def __enter__(self) -> "TelemetryArchive":
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()
