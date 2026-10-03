# 矿场遥测冷热归档库

纯 Python 实现，无外部服务依赖：热读数与管理信息存 SQLite，冷读数存本地 gzip 无损压缩归档文件。

## 概述

- 热读数与管理信息存 SQLite（hot.sqlite3），冷读数存本地压缩归档段文件（segments/ 目录）。
- 年龄 < 30 天为热读数；30 天 <= 年龄 < 365 天为冷读数；年龄 >= 365 天自动过期清除（一天固定 86400 秒）。
- 所有调用由调用方串行发起，并在每次调用时提供不倒退的 UTC 当前时间（Unix 秒）。
- 崩溃安全：接收是单事务（整批全生效或全不生效）；归档段先写临时文件、fsync、再原子改名；进程被强制终止后重开即可读，下一次成功维护自动清理中断遗留产物。

## 使用

    from telemetry_store import TelemetryStore, Reading

    with TelemetryStore.open("/path/to/store") as store:
        # 批量接收（同一 ID 重投自动去重，无论原读数在热区还是冷区）
        n = store.ingest([
            Reading(reading_id="abc", miner_id="miner-1", ts=1_700_000_000,
                    hashrate=112.5, power=3250.0, temperature=67.5),
        ], now=1_700_000_100)

        # 按矿机与时间区间读取 [start, end)，每个 ID 一条，按 (ts, id) 排序
        readings = store.read_range("miner-1", start=0, end=1_700_000_100, now=1_700_000_100)

        # 归档清理：热转冷、过期清除、孤儿文件清扫
        report = store.maintain(now=1_700_000_100)

## 存储布局

    <store>/
      hot.sqlite3      # 热读数 + 段注册表 + 冷区 ID 索引（WAL, synchronous=FULL）
      segments/        # seg-<id>-<miner>-<start>-<end>.seg.gz 压缩归档段

归档段为 gzip 文本，每行一条读数（制表符分隔，浮点用 repr 保证无损往返）。

## 测试

    python3 -m pytest                 # 全部测试（含 100 万条规模内存验证，约 4 分钟）
    python3 -m pytest --ignore=tests/test_memory.py   # 快速套件（约 20 秒）

覆盖：30 天 / 365 天边界前后 1 秒；维护前后与参考模型逐条比对；迟到读数写入已归档时间段与重复提交；归档 / 过期清理途中 SIGKILL 后重开与连续重试（不丢不重、废弃文件清除）；100 万条读数下维护峰值内存 <= 128 MiB。
