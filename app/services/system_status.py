from __future__ import annotations

import os
import subprocess
import time

_CPU_SNAPSHOT = None


def _read_cpu_times() -> tuple[int, int]:
    with open("/proc/stat", "r", encoding="utf-8") as f:
        line = f.readline().strip()
    parts = [int(x) for x in line.split()[1:]]
    idle = parts[3] + (parts[4] if len(parts) > 4 else 0)
    total = sum(parts)
    return idle, total


def cpu_usage_percent() -> float | None:
    global _CPU_SNAPSHOT
    now = time.time()
    idle, total = _read_cpu_times()
    if _CPU_SNAPSHOT is None:
        _CPU_SNAPSHOT = (now, idle, total)
        return None
    _, prev_idle, prev_total = _CPU_SNAPSHOT
    _CPU_SNAPSHOT = (now, idle, total)
    delta_total = total - prev_total
    delta_idle = idle - prev_idle
    if delta_total <= 0:
        return None
    return round(100.0 * (1.0 - (delta_idle / delta_total)), 1)


def memory_status() -> dict:
    meminfo = {}
    with open("/proc/meminfo", "r", encoding="utf-8") as f:
        for line in f:
            key, value = line.split(":", 1)
            meminfo[key] = int(value.strip().split()[0])
    total_mb = meminfo.get("MemTotal", 0) // 1024
    available_mb = meminfo.get("MemAvailable", 0) // 1024
    used_mb = max(total_mb - available_mb, 0)
    return {
        "total_mb": total_mb,
        "used_mb": used_mb,
        "available_mb": available_mb,
        "used_percent": round((used_mb / total_mb) * 100.0, 1) if total_mb else None,
    }


def gpu_status() -> list[dict]:
    try:
        cmd = [
            "nvidia-smi",
            "--query-gpu=index,name,utilization.gpu,memory.used,memory.total,temperature.gpu",
            "--format=csv,noheader,nounits",
        ]
        out = subprocess.check_output(cmd, text=True, stderr=subprocess.DEVNULL).strip()
    except Exception:
        return []
    gpus = []
    for line in out.splitlines():
        parts = [p.strip() for p in line.split(",")]
        if len(parts) != 6:
            continue
        gpus.append({
            "index": int(parts[0]),
            "name": parts[1],
            "utilization_gpu": float(parts[2]),
            "memory_used_mb": int(parts[3]),
            "memory_total_mb": int(parts[4]),
            "temperature_c": float(parts[5]),
        })
    return gpus


def system_status() -> dict:
    return {
        "cpu_percent": cpu_usage_percent(),
        "cpu_count": os.cpu_count(),
        "loadavg": tuple(round(v, 2) for v in os.getloadavg()),
        "memory": memory_status(),
        "gpus": gpu_status(),
    }
