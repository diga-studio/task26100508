"""SQLite 存储层：热读数表、冷读数 ID 索引、归档文件清单。"""
from __future__ import annotations

import sqlite3

SCHEMA = """
CREATE TABLE IF NOT EXISTS hot_readings (
    id TEXT PRIMARY KEY,
    miner_id TEXT NOT NULL,
    ts INTEGER NOT NULL,
    hashrate REAL NOT NULL,
    power REAL NOT NULL,
    temperature REAL NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_hot_miner_ts ON hot_readings (miner_id, ts);

CREATE TABLE IF NOT EXISTS cold_ids (
    id TEXT PRIMARY KEY,
    file_path TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS archive_files (
    path TEXT PRIMARY KEY,
    miner_id TEXT NOT NULL,
    day_start INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_archive_miner_day ON archive_files (miner_id, day_start);
"""


def connect(db_path: str) -> sqlite3.Connection:
    """打开（必要时创建）数据库并确保表结构存在。"""
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=FULL")
    conn.execute("PRAGMA foreign_keys=ON")
    conn.executescript(SCHEMA)
    conn.commit()
    return conn
