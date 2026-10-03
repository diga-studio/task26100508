"""Shared helpers: deterministic reading generator + reference model."""

from __future__ import annotations

import random

import pytest

from telemetry_store import EXPIRE_AGE_SECONDS, Reading, TelemetryStore

DAY = 86400


def make_reading(rng: random.Random, reading_id: str, miner_id: str, ts: int) -> Reading:
    return Reading(
        reading_id=reading_id,
        miner_id=miner_id,
        ts=ts,
        hashrate=round(rng.uniform(90.0, 115.0), 6),
        power=round(rng.uniform(2800.0, 3500.0), 6),
        temperature=round(rng.uniform(55.0, 85.0), 6),
    )


class Reference:
    """In-memory reference model: first write per ID wins, expiry by age."""

    def __init__(self) -> None:
        self._by_id: dict[str, Reading] = {}

    def ingest(self, readings: list[Reading]) -> None:
        for reading in readings:
            self._by_id.setdefault(reading.reading_id, reading)

    def expect(self, miner_id: str, start: int, end: int, now: int) -> list[Reading]:
        lo = max(start, now - EXPIRE_AGE_SECONDS + 1)
        if lo >= end:
            return []
        got = [
            r
            for r in self._by_id.values()
            if r.miner_id == miner_id and lo <= r.ts < end
        ]
        return sorted(got, key=lambda r: (r.ts, r.reading_id))

    def live_ids(self, now: int) -> set[str]:
        lo = now - EXPIRE_AGE_SECONDS + 1
        return {r.reading_id for r in self._by_id.values() if r.ts >= lo}


@pytest.fixture()
def store(tmp_path):
    with TelemetryStore.open(tmp_path / "db") as s:
        yield s


@pytest.fixture()
def rng() -> random.Random:
    return random.Random(20261005)
