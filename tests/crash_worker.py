"""在子进程中执行 maintain 并在指定故障点 os._exit，模拟 kill -9。"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from telemetry_archive import TelemetryArchive  # noqa: E402


def main() -> None:
    path, point, now = sys.argv[1], sys.argv[2], int(sys.argv[3])

    def hook(p: str) -> None:
        if p == point:
            os._exit(9)

    ta = TelemetryArchive(path, fault_hook=hook)
    ta.maintain(now)
    ta.close()


if __name__ == "__main__":
    main()
