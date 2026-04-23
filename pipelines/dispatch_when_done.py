#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import sqlite3
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app.services.processing_jobs import dispatch_next_job


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--db-path", type=Path, required=True)
    parser.add_argument("--watch-job-id", type=int, required=True)
    parser.add_argument("--poll-seconds", type=int, default=30)
    args = parser.parse_args()

    os.chdir(str(ROOT))

    while True:
        conn = sqlite3.connect(args.db_path)
        conn.row_factory = sqlite3.Row
        row = conn.execute("SELECT status FROM processing_jobs WHERE id = ?", (args.watch_job_id,)).fetchone()
        conn.close()

        status = row["status"] if row else None
        if status in {"completed", "failed", "cancelled"}:
            print(
                json.dumps(
                    {
                        "watch_job_id": args.watch_job_id,
                        "final_status": status,
                        "dispatch": dispatch_next_job(),
                    },
                    ensure_ascii=False,
                    default=str,
                )
            )
            return 0

        time.sleep(max(args.poll_seconds, 5))


if __name__ == "__main__":
    raise SystemExit(main())
