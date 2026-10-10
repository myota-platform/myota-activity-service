"""Audited, idempotent redrive of an Activity notification poison event."""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
import uuid
from typing import Any

import psycopg
from nats.aio.client import Client as NATS

from event_consumer import ACTIVITY_NOTIFICATION_SUBJECTS


def _request(
    database_url: str, event_id: str, actor: str, reason: str
) -> tuple[str, str, dict[str, Any]]:
    with psycopg.connect(database_url) as connection:
        row = connection.execute(
            "SELECT event_type, subject, payload FROM dead_letter_event "
            "WHERE event_id=%s AND stream_name='MYOTA_EVENTS' "
            "AND resolved_at IS NULL FOR UPDATE",
            (event_id,),
        ).fetchone()
        if row is None:
            raise RuntimeError(
                "unresolved Activity notification dead letter not found"
            )
        event_type, subject, payload = row
        if subject not in ACTIVITY_NOTIFICATION_SUBJECTS:
            raise RuntimeError(
                "dead-letter subject is not registered for this consumer"
            )
        if not isinstance(payload, dict) or payload.get("eventId") != event_id:
            raise RuntimeError(
                "poison envelope has no valid eventId; reconstruct from the owning source database"
            )
        if event_type != subject.removeprefix("myota.events."):
            raise RuntimeError(
                "dead-letter subject and event type do not match"
            )

        pending = connection.execute(
            "SELECT id FROM activity_notification_redrive "
            "WHERE event_id=%s AND status='PENDING' ORDER BY requested_at DESC LIMIT 1",
            (event_id,),
        ).fetchone()
        if pending:
            return str(pending[0]), subject, payload
        redrive_id = str(uuid.uuid4())
        connection.execute(
            "INSERT INTO activity_notification_redrive"
            "(id,event_id,actor,reason,status) VALUES (%s,%s,%s,%s,'PENDING')",
            (redrive_id, event_id, actor, reason),
        )
    return redrive_id, subject, payload


async def redrive(
    database_url: str, event_id: str, actor: str, reason: str
) -> str:
    redrive_id, subject, payload = _request(
        database_url, event_id, actor, reason
    )
    nc = NATS()
    await nc.connect(os.environ.get("NATS_URL", "nats://nats:4222"))
    try:
        await nc.jetstream().publish(
            subject,
            json.dumps(payload, separators=(",", ":")).encode(),
            headers={
                "Nats-Msg-Id": f"activity-notification-redrive-{redrive_id}"
            },
        )
    finally:
        await nc.drain()
    with psycopg.connect(database_url) as connection:
        connection.execute(
            "UPDATE activity_notification_redrive SET status='PUBLISHED', "
            "published_at=now() WHERE id=%s AND status='PENDING'",
            (redrive_id,),
        )
    return redrive_id


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--event-id", required=True)
    parser.add_argument("--actor", required=True)
    parser.add_argument("--reason", required=True)
    args = parser.parse_args()
    database_url = os.environ.get("ACTIVITY_DATABASE_URL")
    if not database_url:
        raise RuntimeError("ACTIVITY_DATABASE_URL is required")
    redrive_id = asyncio.run(
        redrive(database_url, args.event_id, args.actor, args.reason)
    )
    print(f"published audited Activity notification redrive {redrive_id}")


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        print(f"Activity notification redrive failed: {exc}", file=sys.stderr)
        raise
