"""TelemetryStore: hot readings in SQLite, cold readings in archive segments.

Layout under the store root directory:
    hot.sqlite3   - hot readings + management info (segments registry, cold IDs)
    segments/     - compressed cold archive segment files

Crash safety: every mutating step is either a single SQLite transaction or an
atomic file rename. Artifacts left behind by a kill mid-maintenance (temp
files, unreferenced segment files, segment rows whose file deletion did not
finish) are reconciled by the next successful maintain() run.
"""

from __future__ import annotations

import os
import sqlite3
from pathlib import Path
from typing import Iterable

from . import segments as seg
from .models import (
    EXPIRE_AGE_SECONDS,
    HOT_MAX_AGE_SECONDS,
    MaintenanceReport,
    Reading,
    SegmentInfo,
)

_SCHEMA = """
CREATE TABLE IF NOT EXISTS meta (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS readings (
    id TEXT PRIMARY KEY,
    miner_id TEXT NOT NULL,
    ts INTEGER NOT NULL,
    hashrate REAL NOT NULL,
    power REAL NOT NULL,
    temperature REAL NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_readings_miner_ts ON readings(miner_id, ts);
CREATE TABLE IF NOT EXISTS segments (
    id INTEGER PRIMARY KEY,
    miner_id TEXT NOT NULL,
    start_ts INTEGER NOT NULL,
    end_ts INTEGER NOT NULL,
    filename TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_segments_miner ON segments(miner_id, start_ts, end_ts);
CREATE TABLE IF NOT EXISTS cold_ids (
    id TEXT PRIMARY KEY,
    segment_id INTEGER NOT NULL REFERENCES segments(id) ON DELETE CASCADE
);
"""

_ID_CHUNK = 5000


class TelemetryStore:
    """Hot/cold telemetry archive. All calls are serialized by the caller."""

    def __init__(self, root: str | Path) -> None:
        self._root = Path(root)
        self._seg_dir = self._root / "segments"
        self._seg_dir.mkdir(parents=True, exist_ok=True)
        self._conn = sqlite3.connect(self._root / "hot.sqlite3")
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA synchronous=FULL")
        self._conn.execute("PRAGMA foreign_keys=ON")
        self._conn.executescript(_SCHEMA)
        self._conn.commit()

    @classmethod
    def open(cls, root: str | Path) -> "TelemetryStore":
        """Open (creating if needed) a store rooted at the given directory."""
        return cls(root)

    # ------------------------------------------------------------------ meta

    def _get_meta(self, key: str, default: int) -> int:
        row = self._conn.execute("SELECT value FROM meta WHERE key = ?", (key,)).fetchone()
        return int(row[0]) if row else default

    def _set_meta(self, key: str, value: int) -> None:
        self._conn.execute(
            "INSERT INTO meta(key, value) VALUES(?, ?) "
            "ON CONFLICT(key) DO UPDATE SET value = excluded.value",
            (key, str(value)),
        )

    def _touch_now(self, now: int) -> None:
        if now > self._get_meta("last_now", -1):
            self._set_meta("last_now", now)

    # ---------------------------------------------------------------- ingest

    def ingest(self, readings: Iterable[Reading], now: int) -> int:
        """Atomically persist a batch of readings; returns new-count."""
        batch = list(readings)
        if not batch:
            return 0
        known = self._cold_id_filter([r.reading_id for r in batch])
        with self._conn:  # single transaction: all-or-nothing for the batch
            before = self._conn.total_changes
            self._conn.executemany(
                "INSERT OR IGNORE INTO readings(id, miner_id, ts, hashrate, power, temperature) "
                "VALUES (?, ?, ?, ?, ?, ?)",
                [
                    (r.reading_id, r.miner_id, r.ts, r.hashrate, r.power, r.temperature)
                    for r in batch
                    if r.reading_id not in known
                ],
            )
            inserted = self._conn.total_changes - before
            self._touch_now(now)
        return inserted

    def _cold_id_filter(self, ids: list[str]) -> set[str]:
        """Return the subset of ids already present in the cold layer."""
        found: set[str] = set()
        for i in range(0, len(ids), _ID_CHUNK):
            chunk = ids[i : i + _ID_CHUNK]
            marks = ",".join("?" * len(chunk))
            rows = self._conn.execute(f"SELECT id FROM cold_ids WHERE id IN ({marks})", chunk)
            found.update(row[0] for row in rows)
        return found

    # ------------------------------------------------------------------ read

    def read_range(self, miner_id: str, start: int, end: int, now: int) -> list[Reading]:
        """Return live readings for a miner in [start, end), one per ID."""
        lo = max(start, now - EXPIRE_AGE_SECONDS + 1)
        if lo >= end:
            return []
        merged: dict[str, Reading] = {}
        rows = self._conn.execute(
            "SELECT id, miner_id, ts, hashrate, power, temperature FROM readings "
            "WHERE miner_id = ? AND ts >= ? AND ts < ?",
            (miner_id, lo, end),
        )
        for rid, mid, ts, hashrate, power, temperature in rows:
            merged[rid] = Reading(rid, mid, ts, hashrate, power, temperature)
        for info in self._segments_for(miner_id, lo, end):
            for reading in seg.read_segment(self._seg_dir / info.filename):
                if lo <= reading.ts < end and reading.reading_id not in merged:
                    merged[reading.reading_id] = reading
        return sorted(merged.values(), key=lambda r: (r.ts, r.reading_id))

    def _segments_for(self, miner_id: str, lo: int, hi: int) -> list[SegmentInfo]:
        rows = self._conn.execute(
            "SELECT id, miner_id, start_ts, end_ts, filename FROM segments "
            "WHERE miner_id = ? AND start_ts < ? AND end_ts > ? ORDER BY start_ts",
            (miner_id, hi, lo),
        )
        return [SegmentInfo(*row) for row in rows]

    # --------------------------------------------------------------- maintain

    def maintain(self, now: int) -> MaintenanceReport:
        """Archive cold readings, purge expired data, sweep orphan files."""
        hot_cutoff = now - HOT_MAX_AGE_SECONDS
        expire_cutoff = now - EXPIRE_AGE_SECONDS
        report = MaintenanceReport()

        expired_hot = self._expire_hot(expire_cutoff, now)
        archived, segments_created = self._archive_cold(hot_cutoff, expire_cutoff)
        expired_cold, segments_removed = self._expire_segments(expire_cutoff)
        orphans = self._sweep_orphans()

        return MaintenanceReport(
            archived_readings=archived,
            expired_readings=expired_hot + expired_cold,
            segments_created=segments_created,
            segments_removed=segments_removed,
            orphan_files_removed=orphans,
        )

    def _expire_hot(self, expire_cutoff: int, now: int) -> int:
        with self._conn:
            before = self._conn.total_changes
            self._conn.execute("DELETE FROM readings WHERE ts <= ?", (expire_cutoff,))
            deleted = self._conn.total_changes - before
            self._touch_now(now)
        return deleted

    def _archive_cold(self, hot_cutoff: int, expire_cutoff: int) -> tuple[int, int]:
        total_archived = 0
        segments_created = 0
        miners = [
            row[0]
            for row in self._conn.execute(
                "SELECT DISTINCT miner_id FROM readings WHERE ts <= ? AND ts > ?",
                (hot_cutoff, expire_cutoff),
            )
        ]
        for miner_id in miners:
            archived, created = self._archive_miner(miner_id, hot_cutoff, expire_cutoff)
            total_archived += archived
            segments_created += created
        return total_archived, segments_created

    def _archive_miner(self, miner_id: str, hot_cutoff: int, expire_cutoff: int) -> tuple[int, int]:
        rows = self._conn.execute(
            "SELECT id, miner_id, ts, hashrate, power, temperature FROM readings "
            "WHERE miner_id = ? AND ts <= ? AND ts > ? ORDER BY ts, id",
            (miner_id, hot_cutoff, expire_cutoff),
        ).fetchall()
        if not rows:
            return 0, 0
        candidates = [Reading(*row) for row in rows]
        new_lo = candidates[0].ts
        new_hi = candidates[-1].ts + 1

        overlapping = self._segments_for(miner_id, new_lo, new_hi)
        merged: dict[str, Reading] = {}
        for info in overlapping:
            for reading in seg.read_segment(self._seg_dir / info.filename):
                merged.setdefault(reading.reading_id, reading)
        for reading in candidates:
            merged[reading.reading_id] = reading
        ordered = sorted(merged.values(), key=lambda r: (r.ts, r.reading_id))

        segment_id = self._get_meta("next_segment_id", 1)
        start_ts = ordered[0].ts
        end_ts = ordered[-1].ts + 1
        filename = seg.segment_filename(segment_id, miner_id, start_ts, end_ts)
        seg.write_segment(self._seg_dir, filename, ordered)

        old_segment_ids = [info.segment_id for info in overlapping]
        old_files = [info.filename for info in overlapping]
        with self._conn:
            self._set_meta("next_segment_id", segment_id + 1)
            if old_segment_ids:
                marks = ",".join("?" * len(old_segment_ids))
                self._conn.execute(f"DELETE FROM segments WHERE id IN ({marks})", old_segment_ids)
            self._conn.execute(
                "INSERT INTO segments(id, miner_id, start_ts, end_ts, filename) "
                "VALUES (?, ?, ?, ?, ?)",
                (segment_id, miner_id, start_ts, end_ts, filename),
            )
            self._conn.executemany(
                "INSERT INTO cold_ids(id, segment_id) VALUES (?, ?)",
                [(r.reading_id, segment_id) for r in ordered],
            )
            self._conn.executemany(
                "DELETE FROM readings WHERE id = ?", [(r.reading_id,) for r in candidates]
            )
        for old in old_files:
            seg.remove_file(self._seg_dir, old)
        return len(candidates), 1

    def _expire_segments(self, expire_cutoff: int) -> tuple[int, int]:
        expired_readings = 0
        segments_removed = 0
        rows = self._conn.execute(
            "SELECT id, miner_id, start_ts, end_ts, filename FROM segments "
            "WHERE start_ts <= ? ORDER BY id",
            (expire_cutoff,),
        ).fetchall()
        for segment_id, miner_id, start_ts, end_ts, filename in rows:
            if end_ts <= expire_cutoff + 1:
                count = self._drop_segment(segment_id, filename)
                expired_readings += count
                segments_removed += 1
            else:
                kept, dropped = self._rewrite_segment(
                    segment_id, miner_id, filename, expire_cutoff
                )
                expired_readings += dropped
                segments_removed += 1 if dropped else 0
        return expired_readings, segments_removed

    def _drop_segment(self, segment_id: int, filename: str) -> int:
        row = self._conn.execute(
            "SELECT COUNT(*) FROM cold_ids WHERE segment_id = ?", (segment_id,)
        ).fetchone()
        with self._conn:
            self._conn.execute("DELETE FROM segments WHERE id = ?", (segment_id,))
        seg.remove_file(self._seg_dir, filename)
        return row[0]

    def _rewrite_segment(
        self, segment_id: int, miner_id: str, filename: str, expire_cutoff: int
    ) -> tuple[int, int]:
        survivors = [
            r for r in seg.read_segment(self._seg_dir / filename) if r.ts > expire_cutoff
        ]
        total = self._conn.execute(
            "SELECT COUNT(*) FROM cold_ids WHERE segment_id = ?", (segment_id,)
        ).fetchone()[0]
        dropped = total - len(survivors)
        if dropped == 0:
            return total, 0
        new_id = self._get_meta("next_segment_id", 1)
        start_ts = survivors[0].ts
        end_ts = survivors[-1].ts + 1
        new_filename = seg.segment_filename(new_id, miner_id, start_ts, end_ts)
        seg.write_segment(self._seg_dir, new_filename, survivors)
        with self._conn:
            self._set_meta("next_segment_id", new_id + 1)
            self._conn.execute("DELETE FROM segments WHERE id = ?", (segment_id,))
            self._conn.execute(
                "INSERT INTO segments(id, miner_id, start_ts, end_ts, filename) "
                "VALUES (?, ?, ?, ?, ?)",
                (new_id, miner_id, start_ts, end_ts, new_filename),
            )
            self._conn.executemany(
                "INSERT INTO cold_ids(id, segment_id) VALUES (?, ?)",
                [(r.reading_id, new_id) for r in survivors],
            )
        seg.remove_file(self._seg_dir, filename)
        return len(survivors), dropped

    def _sweep_orphans(self) -> int:
        expected = {row[0] for row in self._conn.execute("SELECT filename FROM segments")}
        removed = 0
        if self._seg_dir.is_dir():
            for name in os.listdir(self._seg_dir):
                if name.endswith(seg.TMP_SUFFIX) or (
                    name.endswith(seg.SEGMENT_SUFFIX) and name not in expected
                ):
                    seg.remove_file(self._seg_dir, name)
                    removed += 1
        return removed

    # ------------------------------------------------------------------ misc

    def close(self) -> None:
        self._conn.close()

    def __enter__(self) -> "TelemetryStore":
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()
