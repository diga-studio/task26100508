"""冷归档段文件的读写（无损压缩，写临时文件后原子改名）。"""
from __future__ import annotations

import json
import os
import uuid
import zlib
from typing import Iterable, List

from .models import Reading

SEGMENT_SUFFIX = ".zlib"


def segment_name(day_start: int, generation: int) -> str:
    """段文件名：按天对齐的起始秒 + 代数（重写时递增避免覆盖未提交副本）。"""
    return f"{day_start}.{generation}{SEGMENT_SUFFIX}"


def write_segment(tmp_dir: str, final_path: str, readings: Iterable[Reading]) -> None:
    """把读数写入压缩段文件。

    先写入临时目录中的临时文件并 fsync，再原子改名到最终路径，
    最后 fsync 目录项；进程在任意点被终止都不会留下半截的最终文件。
    """
    os.makedirs(tmp_dir, exist_ok=True)
    os.makedirs(os.path.dirname(final_path), exist_ok=True)
    tmp_path = os.path.join(tmp_dir, f"seg-{uuid.uuid4().hex}.tmp")
    compressor = zlib.compressobj(level=9)
    with open(tmp_path, "wb") as fh:
        for r in readings:
            line = json.dumps(
                [r.id, r.miner_id, r.ts, r.hashrate, r.power, r.temperature],
                separators=(",", ":"),
            ).encode("utf-8") + b"\n"
            fh.write(compressor.compress(line))
        fh.write(compressor.flush())
        fh.flush()
        os.fsync(fh.fileno())
    os.replace(tmp_path, final_path)
    dir_fd = os.open(os.path.dirname(final_path), os.O_RDONLY)
    try:
        os.fsync(dir_fd)
    finally:
        os.close(dir_fd)


def read_segment(path: str) -> List[Reading]:
    """读取一个段文件中的全部读数。"""
    with open(path, "rb") as fh:
        raw = zlib.decompress(fh.read())
    out: List[Reading] = []
    for line in raw.split(b"\n"):
        if not line:
            continue
        rid, miner_id, ts, hashrate, power, temperature = json.loads(line)
        out.append(
            Reading(
                id=rid,
                miner_id=miner_id,
                ts=int(ts),
                hashrate=float(hashrate),
                power=float(power),
                temperature=float(temperature),
            )
        )
    return out
