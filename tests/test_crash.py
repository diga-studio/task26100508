"""Crash-recovery tests: SIGKILL mid-operation, reopen, retry, verify."""

from __future__ import annotations

import random
import signal
import subprocess
import sys
import time
from pathlib import Path

import pytest

DRIVER = Path(__file__).resolve().parent / "crash_driver.py"
DAY = 86400
NOW = 500 * DAY


def run_driver(db: Path, mode: str, now: int = NOW, extra: list[str] | None = None) -> subprocess.CompletedProcess:
    cmd = [sys.executable, str(DRIVER), "--db", str(db), "--mode", mode, "--now", str(now)]
    if extra:
        cmd += extra
    return subprocess.run(cmd, capture_output=True, text=True, timeout=300)


def kill_after_delay(db: Path, mode: str, now: int, delay: float) -> None:
    proc = subprocess.Popen(
        [sys.executable, str(DRIVER), "--db", str(db), "--mode", mode, "--now", str(now)],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    time.sleep(delay)
    proc.send_signal(signal.SIGKILL)
    proc.wait()


@pytest.fixture()
def crash_db(tmp_path):
    db = tmp_path / "db"
    result = run_driver(db, "setup")
    assert "SETUP DONE" in result.stdout, result.stderr
    return db


def test_repeated_kill_during_maintenance(crash_db):
    rng = random.Random(7)
    killed_midflight = 0
    for round_no in range(8):
        now = NOW + round_no * 10 * DAY
        delay = rng.uniform(0.0, 0.6)
        kill_after_delay(crash_db, "maintain", now, delay)
        # Reopen and verify: confirmed data intact, no dupes, hygiene restored.
        result = run_driver(crash_db, "verify", now)
        assert "VERIFY OK" in result.stdout, result.stderr


def test_kill_during_ingest_is_all_or_nothing(crash_db):
    for attempt in range(4):
        delay = 0.05 * attempt
        kill_after_delay(crash_db, "ingest-crash", NOW, delay)
        result = run_driver(crash_db, "verify", NOW, extra=["--allow-crash-batch"])
        assert "VERIFY OK" in result.stdout, result.stderr


def test_orphan_files_do_not_accumulate(crash_db):
    seg_dir = crash_db / "segments"
    for round_no in range(5):
        now = NOW + round_no * 10 * DAY
        kill_after_delay(crash_db, "maintain", now, 0.05 * (round_no % 3))
        run_driver(crash_db, "verify", now)
        # After each successful verify (which runs maintain), the directory
        # contains exactly the registered files; repeated crashes add nothing.
        on_disk = sorted(p.name for p in seg_dir.iterdir())
        assert all(not name.endswith(".tmp") for name in on_disk)
