"""Small JetStream consumer primitive used by the notification worker."""

from __future__ import annotations

import asyncio
import json
import logging
import os
from collections.abc import Awaitable, Callable
from typing import Any

import psycopg
from nats.aio.client import Client as NATS
from nats.errors import TimeoutError as NatsTimeoutError
from nats.js.api import AckPolicy, ConsumerConfig
from nats.js.errors import FetchTimeoutError

LOG = logging.getLogger(__name__)


async def subscribe_notifications(js: Any, consumer: str, subject: str):
    """Bind replicas to a shared pull consumer without exclusive inboxes.

    A new broker durable avoids changing the immutable delivery mode of the
    legacy push consumer. Database deduplication keeps its stable identity.
    """
    durable = f"{consumer.replace('_', '-')}-pull-v1"
    return await js.pull_subscribe(
        subject,
        stream="MYOTA_EVENTS",
        durable=durable,
        config=ConsumerConfig(
            durable_name=durable,
            filter_subject=subject,
            ack_policy=AckPolicy.EXPLICIT,
            ack_wait=60,
            max_deliver=10,
            max_ack_pending=64,
        ),
    )


async def consume_forever(
    consumer: str,
    subject: str,
    database_url: str,
    handler: Callable[[dict[str, Any]], Awaitable[None]],
    supported_major: int = 1,
    stop_event: asyncio.Event | None = None,
) -> None:
    stop_event = stop_event or asyncio.Event()
    nc = NATS()
    await nc.connect(
        os.environ.get("NATS_URL", "nats://nats:4222"), name=consumer
    )
    try:
        sub = await subscribe_notifications(nc.jetstream(), consumer, subject)
        LOG.info("Notification pull consumer subscribed: %s", consumer)
        while not stop_event.is_set():
            try:
                messages = await sub.fetch(batch=1, timeout=1)
            except (FetchTimeoutError, NatsTimeoutError):
                continue
            for message in messages:
                event = json.loads(message.data)
                event_type = event.get("eventType", "")
                version = (
                    int(event_type.rsplit(".v", 1)[-1])
                    if ".v" in event_type
                    else 0
                )
                if version != supported_major:
                    await message.nak(delay=30)
                    continue
                try:
                    with psycopg.connect(database_url) as connection:
                        seen = connection.execute(
                            "SELECT 1 FROM consumer_processed_event WHERE consumer=%s AND event_id=%s",
                            (consumer, event["eventId"]),
                        ).fetchone()
                        if seen:
                            await message.ack()
                            continue
                        await handler(event)
                        connection.execute(
                            "INSERT INTO consumer_processed_event(consumer,event_id) VALUES (%s,%s) ON CONFLICT DO NOTHING",
                            (consumer, event["eventId"]),
                        )
                        connection.execute(
                            "INSERT INTO consumer_checkpoint(consumer,last_event_id) VALUES (%s,%s) ON CONFLICT (consumer) DO UPDATE SET last_event_id=EXCLUDED.last_event_id,updated_at=now()",
                            (consumer, event["eventId"]),
                        )
                    await message.ack()
                except Exception as exc:
                    with psycopg.connect(database_url) as connection:
                        connection.execute(
                            "INSERT INTO dead_letter_event(event_id,event_type,payload,attempts,error) VALUES (%s,%s,%s::jsonb,1,%s) ON CONFLICT DO NOTHING",
                            (
                                event["eventId"],
                                event_type,
                                json.dumps(event),
                                str(exc),
                            ),
                        )
                    await message.term()
    finally:
        await nc.drain()
