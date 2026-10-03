"""测试共享工具：按输入记录计算参考结果。"""
from __future__ import annotations

import os
import sys
from typing import Dict, List

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from telemetry_archive import RETENTION_SECONDS, Reading  # noqa: E402


class Reference:
    """按输入记录计算的参考模型：每个 ID 只保留一条。"""

    def __init__(self) -> None:
        self.records: Dict[str, Reading] = {}

    def ingest(self, readings: List[Reading]) -> None:
        for r in readings:
            self.records.setdefault(r.id, r)

    def query(self, miner_id: str, start: int, end: int, now: int) -> List[Reading]:
        cutoff = now - RETENTION_SECONDS
        out = [
            r
            for r in self.records.values()
            if r.miner_id == miner_id and start <= r.ts < end and r.ts > cutoff
        ]
        return sorted(out, key=lambda r: (r.ts, r.id))


@pytest.fixture()
def archive_path(tmp_path):
    return str(tmp_path / "store")
