"""Audited database-authoritative redrive for terminal Activity work jobs."""

from __future__ import annotations

import argparse
import sys

from activity_repository import ActivityRepository


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--job-id", required=True)
    parser.add_argument("--actor", required=True)
    parser.add_argument("--reason", required=True)
    args = parser.parse_args()
    repo = ActivityRepository("ACTIVITY_DATABASE_URL")
    if not repo.durable:
        parser.error("ACTIVITY_DATABASE_URL is required")
    try:
        event_id = repo.redrive_failed_work_job(
            args.job_id, args.actor, args.reason
        )
    except Exception as exc:
        print(
            f"Activity work redrive failed: {type(exc).__name__}",
            file=sys.stderr,
        )
        return 1
    print(
        f"redrive requested: job_id={args.job_id} outbox_event_id={event_id}; "
        "the Activity outbox relay publishes the audited retry"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
