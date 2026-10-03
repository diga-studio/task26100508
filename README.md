# 矿场遥测冷热归档库

由上层直接调用的 Python 库（无接口层）。热读数与管理信息存 SQLite，
冷读数存本地无损压缩（zlib）归档文件；旧读数自动压缩归档、到期自动删除，
进程在任意位置被强制终止后重开即可继续工作，历史曲线不缺点、不重复计数。

## 存储布局

```
<path>/
├── hot.sqlite3      # 热读数 + 冷读数 ID 索引 + 归档文件清单（WAL 模式）
├── cold/<矿机ID>/<天起始秒>.<代数>.zlib   # 冷段文件（按矿机、按天）
└── tmp/             # 段文件临时写入区，重开时自动清空
```

## 使用

```python
from telemetry_archive import TelemetryArchive, Reading

with TelemetryArchive("/var/lib/telemetry") as ta:
    # 批量接收（整批原子生效；同一 ID 重投自动去重）
    ta.ingest([
        Reading(id="sample-1", miner_id="miner-01", ts=1_700_000_000,
                hashrate=112.5, power=3250.0, temperature=67.5),
    ], now=1_700_000_100)

    # 按矿机与时间区间 [start, end) 读取，自动合并冷热两层、按 ID 去重
    rows = ta.query("miner-01", start=0, end=1_700_000_100, now=1_700_000_100)

    # 归档清理：热转冷、过期删除、清理中断遗留产物
    ta.maintain(now=1_700_000_100)
```

约定：

- `now` 由调用方提供，为不倒退的 UTC Unix 秒；一天固定 86400 秒。
- 年龄不足 30 天为热读数；达到 30 天但不足 365 天为冷读数；达到 365 天即过期。
- 接收时拒绝未来读数与已过期读数（`ValueError`）。
- 允许乱序与迟到读数（包括落入已归档时间段），接收成功后立即可查，
  下次成功 `maintain` 后进入正确存储层。
- 所有调用需串行执行；`TelemetryArchive(path, fault_hook=...)` 的
  `fault_hook` 仅供测试注入故障使用。

## 崩溃安全

- 接收：整批在一个 SQLite 事务中提交，要么全部生效要么全部不生效。
- 归档：段文件先写临时文件并 fsync，再原子改名；清单、ID 索引与热区删除
  在同一事务中提交；旧文件在提交后删除。
- 重开：构造 `TelemetryArchive` 时自动清理临时文件与不在清单中的孤儿段文件，
  无需人工修复；重复中断、重开、重试不会积累废弃副本。

## 测试

```bash
python3 -m pytest                # 全部测试（含 100 万条规模内存验证，约 3 分钟）
python3 -m pytest -m "not slow"  # 跳过大规模内存测试（秒级）
```

覆盖：30 天 / 365 天边界前后 1 秒；维护前后与按输入记录计算的参考结果逐条比较；
向已归档时间段补入迟到读数与重复提交；归档与过期清理途中强制终止
（进程内故障注入 + 子进程 `os._exit` 强杀）后重开与连续重试；
100 万条读数、2000 台矿机规模下维护峰值内存不超过 128 MiB。
