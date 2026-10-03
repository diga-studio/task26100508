"""Cold-storage archive segment files (lossless compressed, crash-safe).

Format: gzip-compressed UTF-8 text, one reading per line, tab-separated:
    reading_id \t miner_id \t ts \t hashrate \t power \t temperature
Floats are stored with repr() so they round-trip exactly (lossless).

Writes are crash-safe: data goes to a .tmp file, is fsynced, then
atomically renamed into place, and the directory is fsynced.
"""

from __future__ import annotations

import gzip
import os
from pathlib import Path
from typing import Iterator

from .models import Reading

SEGMENT_SUFFIX = ".seg.gz"
TMP_SUFFIX = ".tmp"
_HEADER = b"#telemetry-segment-v1"


def segment_filename(segment_id: int, miner_id: str, start_ts: int, end_ts: int) -> str:
    """Build the canonical file name for a segment."""
    safe_miner = miner_id.replace("/", "_").replace("\x00", "_")
    return f"seg-{segment_id:012d}-{safe_miner}-{start_ts}-{end_ts}{SEGMENT_SUFFIX}"


def _encode(reading: Reading) -> bytes:
    return (
        f"{reading.reading_id}\t{reading.miner_id}\t{reading.ts}\t"
        f"{reading.hashrate!r}\t{reading.power!r}\t{reading.temperature!r}\n"
    ).encode("utf-8")


def _decode(line: bytes) -> Reading:
    reading_id, miner_id, ts, hashrate, power, temperature = line.decode("utf-8").rstrip("\n").split("\t")
    return Reading(
        reading_id=reading_id,
        miner_id=miner_id,
        ts=int(ts),
        hashrate=float(hashrate),
        power=float(power),
        temperature=float(temperature),
    )


def _fsync_dir(directory: Path) -> None:
    fd = os.open(directory, os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def write_segment(directory: Path, filename: str, readings: list[Reading]) -> None:
    """Write a segment file atomically (temp file + fsync + rename)."""
    tmp_path = directory / (filename + TMP_SUFFIX)
    final_path = directory / filename
    with open(tmp_path, "wb") as raw:
        with gzip.GzipFile(filename="", mode="wb", fileobj=raw, mtime=0) as gz:
            gz.write(_HEADER + b"\n")
            for reading in readings:
                gz.write(_encode(reading))
        raw.flush()
        os.fsync(raw.fileno())
    os.replace(tmp_path, final_path)
    _fsync_dir(directory)


def read_segment(path: Path) -> Iterator[Reading]:
    """Stream readings out of a segment file."""
    with gzip.open(path, "rb") as gz:
        header = gz.readline()
        if header.rstrip(b"\n") != _HEADER:
            raise ValueError(f"bad segment header in {path}")
        for line in gz:
            line = line.rstrip(b"\n")
            if line:
                yield _decode(line)


def list_segment_files(directory: Path) -> list[str]:
    """List committed segment file names present in the directory."""
    if not directory.is_dir():
        return []
    return sorted(name for name in os.listdir(directory) if name.endswith(SEGMENT_SUFFIX))


def remove_file(directory: Path, filename: str) -> None:
    """Best-effort removal of a file inside the segment directory."""
    try:
        os.unlink(directory / filename)
    except FileNotFoundError:
        pass
