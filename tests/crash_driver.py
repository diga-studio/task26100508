"""Subprocess driver for crash-recovery tests.

Modes:
  setup        create the store and ingest the confirmed dataset (batches)
  maintain     run one maintain() pass (parent may SIGKILL this mid-flight)
  ingest-crash ingest one big batch in a single call (parent may SIGKILL)
  verify       reopen, check confirmed data, check archive-dir hygiene
"""

from __future__ import annotations

import argparse
import os
import random
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from telemetry_store import EXPIRE_AGE_SECONDS, Reading, TelemetryStore  # noqa: E402

DAY = 86400
NOW = 500 * DAY
SEED = 91011
N_MINERS = 50
BATCHES = 20
PER_BATCH = 2000  # confirmed dataset: 20 batches x 2000 = 40k readings
CRASH_BATCH = 200_000  # single ingest call used by ingest-crash mode


def confirmed_dataset() -> list[Reading]:
    rng = random.Random(SEED)
    out = []
    for b in range(BATCHES):
        for i in range(PER_BATCH):
            miner = f"miner-{rng.randrange(N_MINERS)}"
            ts = NOW - rng.randint(1, 340) * DAY - rng.randint(0, DAY - 1)
            out.append(
                Reading(
                    reading_id=f"c-{b}-{i}",
                    miner_id=miner,
                    ts=ts,
                    hashrate=round(rng.uniform(90, 115), 6),
                    power=round(rng.uniform(2800, 3500), 6),
                    temperature=round(rng.uniform(55, 85), 6),
                )
            )
    return out


def crash_batch() -> list[Reading]:
    rng = random.Random(SEED + 1)
    return [
        Reading(
            reading_id=f"x-{i}",
            miner_id=f"miner-{rng.randrange(N_MINERS)}",
            ts=NOW - rng.randint(1, 20) * DAY,
            hashrate=100.0,
            power=3000.0,
            temperature=60.0,
        )
        for i in range(CRASH_BATCH)
    ]


def cmd_setup(db: str) -> None:
    data = confirmed_dataset()
    with TelemetryStore.open(db) as store:
        for b in range(BATCHES):
            chunk = data[b * PER_BATCH : (b + 1) * PER_BATCH]
            store.ingest(chunk, NOW)
    print("SETUP DONE", flush=True)


def cmd_maintain(db: str, now: int) -> None:
    with TelemetryStore.open(db) as store:
        report = store.maintain(now)
    print(f"MAINTAIN DONE {report}", flush=True)


def cmd_ingest_crash(db: str) -> None:
    with TelemetryStore.open(db) as store:
        store.ingest(crash_batch(), NOW)
    print("INGEST DONE", flush=True)


def cmd_verify(db: str, now: int, allow_crash_batch: bool) -> None:
    cutoff = now - EXPIRE_AGE_SECONDS
    live = [r for r in confirmed_dataset() if r.ts > cutoff]
    ref: dict[str, Reading] = {r.reading_id: r for r in live}
    crash_ids = {r.reading_id for r in crash_batch()}
    miners = [f"miner-{i}" for i in range(N_MINERS)]
    with TelemetryStore.open(db) as store:
        # 1) exact per-miner comparison against the reference model
        crash_present = 0
        for miner in miners:
            got = store.read_range(miner, 0, now + 1, now)
            ids = [r.reading_id for r in got]
            assert len(ids) == len(set(ids)), f"duplicate ids for {miner}"
            expected = sorted(
                (r for r in live if r.miner_id == miner), key=lambda r: (r.ts, r.reading_id)
            )
            extra = [r for r in got if r.reading_id in crash_ids]
            crash_present += len(extra)
            assert [r for r in got if r.reading_id not in crash_ids] == expected, miner
            for r in extra:
                assert r.reading_id in crash_ids
        # 2) crash batch is all-or-nothing
        assert crash_present in (0, len(crash_ids)), f"partial batch: {crash_present}"
        if not allow_crash_batch:
            assert crash_present == 0
        # 3) a full maintain now succeeds and keeps results identical
        before = {m: store.read_range(m, 0, now + 1, now) for m in miners}
        store.maintain(now)
        for m, expected in before.items():
            assert store.read_range(m, 0, now + 1, now) == expected
        # 4) archive directory hygiene: no temp files, no unreferenced files
        seg_dir = Path(db) / "segments"
        registered = {row[0] for row in store._conn.execute("SELECT filename FROM segments")}
        on_disk = set(os.listdir(seg_dir)) if seg_dir.is_dir() else set()
        assert not any(name.endswith(".tmp") for name in on_disk), "temp files left"
        assert on_disk == registered, f"orphans: {on_disk - registered}"
    print("VERIFY OK", flush=True)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--db", required=True)
    parser.add_argument("--mode", required=True, choices=["setup", "maintain", "ingest-crash", "verify"])
    parser.add_argument("--now", type=int, default=NOW)
    parser.add_argument("--allow-crash-batch", action="store_true")
    args = parser.parse_args()
    if args.mode == "setup":
        cmd_setup(args.db)
    elif args.mode == "maintain":
        cmd_maintain(args.db, args.now)
    elif args.mode == "ingest-crash":
        cmd_ingest_crash(args.db)
    else:
        cmd_verify(args.db, args.now, args.allow_crash_batch)


if __name__ == "__main__":
    main()
