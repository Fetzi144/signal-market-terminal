from __future__ import annotations

import argparse
import json
import os
import socket
import subprocess
from datetime import datetime, timezone
from pathlib import Path
from shutil import disk_usage
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen


SIZE_UNITS = {
    "b": 1,
    "kb": 1000,
    "mb": 1000**2,
    "gb": 1000**3,
    "tb": 1000**4,
    "kib": 1024,
    "mib": 1024**2,
    "gib": 1024**3,
    "tib": 1024**4,
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Capture a single SMT server resource snapshot as JSONL."
    )
    parser.add_argument(
        "--output-dir",
        default="/var/log/smt-monitor",
        help="Directory where daily JSONL files should be stored.",
    )
    parser.add_argument(
        "--health-url",
        default="http://127.0.0.1/api/v1/health",
        help="Local health endpoint to sample.",
    )
    parser.add_argument(
        "--retention-days",
        type=int,
        default=14,
        help="Delete JSONL files older than this many days.",
    )
    return parser.parse_args()


def read_meminfo() -> dict[str, int]:
    values: dict[str, int] = {}
    try:
        with open("/proc/meminfo", "r", encoding="utf-8") as handle:
            for line in handle:
                key, raw_value = line.split(":", 1)
                number = raw_value.strip().split()[0]
                values[key] = int(number) * 1024
    except FileNotFoundError:
        return values
    return values


def parse_size(value: str) -> int | None:
    raw = value.strip()
    if raw in {"", "0", "0B"}:
        return 0
    number_chars: list[str] = []
    unit_chars: list[str] = []
    for char in raw:
        if char.isdigit() or char == ".":
            number_chars.append(char)
        else:
            unit_chars.append(char)
    if not number_chars:
        return None
    unit = "".join(unit_chars).strip().lower()
    multiplier = SIZE_UNITS.get(unit, 1)
    return int(float("".join(number_chars)) * multiplier)


def parse_percent(value: str) -> float | None:
    raw = value.strip().rstrip("%")
    if not raw:
        return None
    try:
        return float(raw)
    except ValueError:
        return None


def run_command(command: list[str]) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        command,
        check=False,
        capture_output=True,
        text=True,
        encoding="utf-8",
    )


def collect_docker_stats() -> dict[str, Any]:
    command = [
        "docker",
        "stats",
        "--no-stream",
        "--format",
        "{{.Name}}\t{{.CPUPerc}}\t{{.MemUsage}}\t{{.NetIO}}\t{{.BlockIO}}",
    ]
    result = run_command(command)
    if result.returncode != 0:
        return {
            "error": {
                "returncode": result.returncode,
                "stderr": result.stderr.strip(),
            },
            "containers": [],
        }

    containers: list[dict[str, Any]] = []
    for line in result.stdout.splitlines():
        if not line.strip():
            continue
        parts = line.split("\t")
        if len(parts) != 5:
            continue
        mem_usage, mem_limit = [item.strip() for item in parts[2].split("/", 1)]
        net_rx, net_tx = [item.strip() for item in parts[3].split("/", 1)]
        block_read, block_write = [item.strip() for item in parts[4].split("/", 1)]
        containers.append(
            {
                "name": parts[0].strip(),
                "cpu_percent": parse_percent(parts[1]),
                "memory": {
                    "usage_human": mem_usage,
                    "limit_human": mem_limit,
                    "usage_bytes": parse_size(mem_usage),
                    "limit_bytes": parse_size(mem_limit),
                },
                "network": {
                    "rx_human": net_rx,
                    "tx_human": net_tx,
                    "rx_bytes": parse_size(net_rx),
                    "tx_bytes": parse_size(net_tx),
                },
                "block_io": {
                    "read_human": block_read,
                    "write_human": block_write,
                    "read_bytes": parse_size(block_read),
                    "write_bytes": parse_size(block_write),
                },
            }
        )
    containers.sort(key=lambda item: item["name"])
    return {"containers": containers}


def collect_health(health_url: str) -> dict[str, Any]:
    request = Request(
        health_url,
        headers={"User-Agent": "smt-monitor/1.0", "Accept": "application/json"},
    )
    try:
        with urlopen(request, timeout=10) as response:
            payload = json.loads(response.read().decode("utf-8"))
    except HTTPError as exc:
        return {"error": f"http_{exc.code}"}
    except URLError as exc:
        return {"error": str(exc.reason)}
    except Exception as exc:  # pragma: no cover - defensive
        return {"error": str(exc)}

    ingestion = {
        item["run_type"]: {
            "last_status": item.get("last_status"),
            "last_run": item.get("last_run"),
            "markets_processed": item.get("markets_processed"),
        }
        for item in payload.get("ingestion", [])
    }
    scheduler_lease = payload.get("scheduler_lease", {})
    return {
        "status": payload.get("status"),
        "active_markets": payload.get("active_markets"),
        "total_signals": payload.get("total_signals"),
        "recent_alerts_24h": payload.get("recent_alerts_24h"),
        "ingestion": ingestion,
        "scheduler_lease": {
            "heartbeat_freshness_seconds": scheduler_lease.get(
                "heartbeat_freshness_seconds"
            ),
            "expires_in_seconds": scheduler_lease.get("expires_in_seconds"),
        },
    }


def collect_system() -> dict[str, Any]:
    meminfo = read_meminfo()
    root_disk = disk_usage("/")
    try:
        load_1, load_5, load_15 = os.getloadavg()
    except (AttributeError, OSError):
        load_1 = load_5 = load_15 = None
    try:
        with open("/proc/uptime", "r", encoding="utf-8") as handle:
            uptime_seconds = float(handle.read().split()[0])
    except FileNotFoundError:
        uptime_seconds = None
    return {
        "hostname": socket.gethostname(),
        "uptime_seconds": uptime_seconds,
        "load_average": {"1m": load_1, "5m": load_5, "15m": load_15},
        "memory": {
            "total_bytes": meminfo.get("MemTotal"),
            "available_bytes": meminfo.get("MemAvailable"),
            "free_bytes": meminfo.get("MemFree"),
            "cached_bytes": meminfo.get("Cached"),
            "buffers_bytes": meminfo.get("Buffers"),
            "swap_total_bytes": meminfo.get("SwapTotal"),
            "swap_free_bytes": meminfo.get("SwapFree"),
        },
        "disk_root": {
            "total_bytes": root_disk.total,
            "used_bytes": root_disk.used,
            "free_bytes": root_disk.free,
        },
    }


def prune_old_logs(output_dir: Path, retention_days: int) -> None:
    if retention_days <= 0:
        return
    cutoff = datetime.now(timezone.utc).timestamp() - (retention_days * 86400)
    for path in output_dir.glob("resource-*.jsonl"):
        try:
            if path.stat().st_mtime < cutoff:
                path.unlink()
        except FileNotFoundError:
            continue


def write_snapshot(output_dir: Path, snapshot: dict[str, Any]) -> Path:
    output_dir.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now(timezone.utc)
    path = output_dir / f"resource-{stamp.date().isoformat()}.jsonl"
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(snapshot, separators=(",", ":")) + "\n")
    return path


def main() -> int:
    args = parse_args()
    output_dir = Path(args.output_dir)
    snapshot = {
        "timestamp_utc": datetime.now(timezone.utc).isoformat(),
        "system": collect_system(),
        "docker": collect_docker_stats(),
        "health": collect_health(args.health_url),
    }
    prune_old_logs(output_dir, args.retention_days)
    path = write_snapshot(output_dir, snapshot)
    print(path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
