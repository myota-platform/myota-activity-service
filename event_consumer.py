"""Small JetStream consumer primitive used by the notification worker."""
from __future__ import annotations

import json
import os
from collections.abc import Awaitable, Callable
from typing import Any

import psycopg
from nats.aio.client import Client as NATS


async def consume_forever(consumer: str, subject: str, database_url: str,
                         handler: Callable[[dict[str, Any]], Awaitable[None]],
                         supported_major: int = 1) -> None:
    nc = NATS()
    await nc.connect(os.environ.get("NATS_URL", "nats://nats:4222"), name=consumer)
    js = nc.jetstream()
    sub = await js.subscribe(subject, durable=consumer.replace("_", "-"), manual_ack=True)
    try:
        async for message in sub.messages:
            event = json.loads(message.data)
            event_type = event.get("eventType", "")
            version = int(event_type.rsplit(".v", 1)[-1]) if ".v" in event_type else 0
            if version != supported_major:
                await message.nak()
                continue
            try:
                with psycopg.connect(database_url) as connection:
                    seen = connection.execute("SELECT 1 FROM consumer_processed_event WHERE consumer=%s AND event_id=%s", (consumer, event["eventId"])).fetchone()
                    if seen:
                        await message.ack()
                        continue
                    await handler(event)
                    connection.execute("INSERT INTO consumer_processed_event(consumer,event_id) VALUES (%s,%s) ON CONFLICT DO NOTHING", (consumer, event["eventId"]))
                    connection.execute("INSERT INTO consumer_checkpoint(consumer,last_event_id) VALUES (%s,%s) ON CONFLICT (consumer) DO UPDATE SET last_event_id=EXCLUDED.last_event_id,updated_at=now()", (consumer, event["eventId"]))
                await message.ack()
            except Exception as exc:
                with psycopg.connect(database_url) as connection:
                    connection.execute("INSERT INTO dead_letter_event(event_id,event_type,payload,attempts,error) VALUES (%s,%s,%s::jsonb,1,%s) ON CONFLICT DO NOTHING", (event["eventId"], event_type, json.dumps(event), str(exc)))
                await message.term()
    finally:
        await nc.drain()
