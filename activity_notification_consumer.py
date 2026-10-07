"""Translate cross-service review/security events into participant notices."""

from __future__ import annotations

import asyncio
import logging
import signal
import sys
from typing import Any

from activity_repository import ActivityRepository
from event_consumer import consume_forever


def recipient_and_kind(event: dict[str, Any]) -> tuple[str | None, str | None]:
    event_type = event.get("eventType", "")
    payload = event.get("payload") or {}
    if event_type.startswith(
        "geodata.entity.reviewed"
    ) or event_type.startswith("geodata.entity.status-changed"):
        recipient = (
            payload.get("proposerId")
            or (payload.get("review") or {}).get("proposerId")
            or (payload.get("provenance") or {}).get("proposerId")
        )
        return recipient, "GEODATA_PROPOSAL_DECISION"
    if event_type.startswith("identity."):
        recipient = (
            payload.get("accountId")
            or (payload.get("account") or {}).get("id")
            or event.get("aggregate", {}).get("id")
        )
        return recipient, "ACCOUNT_SECURITY_EVENT"
    return None, None


async def main() -> None:
    stop_event = asyncio.Event()
    loop = asyncio.get_running_loop()
    for signal_number in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(signal_number, stop_event.set)
    repo = ActivityRepository("ACTIVITY_DATABASE_URL")
    if not repo.durable:
        raise RuntimeError(
            "ACTIVITY_DATABASE_URL is required for the notification consumer"
        )

    async def handle(event: dict[str, Any]) -> None:
        recipient, kind = recipient_and_kind(event)
        if recipient and kind:
            repo.create_notification(
                str(recipient),
                kind,
                {
                    "eventType": event.get("eventType"),
                    "eventId": event.get("eventId"),
                    "payload": event.get("payload"),
                },
                f"event:{event.get('eventId')}",
            )

    await consume_forever(
        "activity-notifications",
        "myota.events.>",
        repo.dsn,
        handle,
        stop_event=stop_event,
    )


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        sys.exit(0)
