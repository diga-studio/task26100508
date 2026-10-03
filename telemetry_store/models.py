"""Data model for telemetry readings."""

from __future__ import annotations

from dataclasses import dataclass

SECONDS_PER_DAY = 86400
HOT_MAX_AGE_DAYS = 30
EXPIRE_AGE_DAYS = 365
HOT_MAX_AGE_SECONDS = HOT_MAX_AGE_DAYS * SECONDS_PER_DAY
EXPIRE_AGE_SECONDS = EXPIRE_AGE_DAYS * SECONDS_PER_DAY


@dataclass(frozen=True)
class Reading:
    """A single telemetry sample from a miner."""

    reading_id: str
    miner_id: str
    ts: int
    hashrate: float
    power: float
    temperature: float


@dataclass(frozen=True)
class SegmentInfo:
    """Metadata for one cold-storage archive segment file."""

    segment_id: int
    miner_id: str
    start_ts: int
    end_ts: int
    filename: str


@dataclass(frozen=True)
class MaintenanceReport:
    """Summary of a single maintenance run."""

    archived_readings: int = 0
    expired_readings: int = 0
    segments_created: int = 0
    segments_removed: int = 0
    orphan_files_removed: int = 0
