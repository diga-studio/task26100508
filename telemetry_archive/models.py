"""数据模型定义。"""
from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class Reading:
    """一条矿机遥测读数。

    id: 全局唯一采样 ID；同一 ID 重投时内容保证相同。
    miner_id: 矿机 ID。
    ts: UTC 采样时间（Unix 秒，整数）。
    hashrate: 算力。
    power: 功耗。
    temperature: 温度。
    """

    id: str
    miner_id: str
    ts: int
    hashrate: float
    power: float
    temperature: float
