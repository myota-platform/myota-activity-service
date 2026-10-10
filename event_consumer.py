"""Small JetStream consumer primitive used by the notification worker."""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import os
import uuid
from collections.abc import Awaitable, Callable
from typing import Any

import psycopg
from psycopg.rows import dict_row
from nats.aio.client import Client as NATS
from nats.errors import TimeoutError as NatsTimeoutError
from nats.js.api import AckPolicy, ConsumerConfig, DeliverPolicy, ReplayPolicy
from nats.js.errors import FetchTimeoutError
from prometheus_client import Counter, Gauge

LOG = logging.getLogger(__name__)

# This tuple mirrors the Activity notification group in
# myota-contracts/contracts/event-registry.json. Keep it exact: the previous
# catch-all filter acknowledged unrelated facts and affected Interest retention.
ACTIVITY_NOTIFICATION_SUBJECTS = (
    "myota.events.geodata.entity.reviewed.v1",
    "myota.events.geodata.entity.status-changed.v1",
    "myota.events.identity.account.admin-updated.v1",
    "myota.events.identity.account.created.v1",
    "myota.events.identity.account.deactivated.v1",
    "myota.events.identity.bootstrap-admin.created.v1",
    "myota.events.identity.callsign.added.v1",
    "myota.events.identity.callsign.evidence-submitted.v1",
    "myota.events.identity.callsign.primary-changed.v1",
    "myota.events.identity.callsign.retired.v1",
    "myota.events.identity.callsign.verified.v1",
    "myota.events.identity.login.failed.v1",
    "myota.events.identity.login.succeeded.v1",
    "myota.events.identity.oidc.mapping.updated.v1",
    "myota.events.identity.recovery.completed.v1",
    "myota.events.identity.recovery.requested.v1",
    "myota.events.identity.role-definition.created.v1",
    "myota.events.identity.role-definition.updated.v1",
    "myota.events.identity.role.assigned.v1",
    "myota.events.identity.roles.replaced.v1",
    "myota.events.identity.service-token.issued.v1",
)

NOTIFICATION_DURABLE = "activity-notifications-v1"
MAX_DELIVERIES = 8
ACK_WAIT_SECONDS = 60
OUTCOMES = Counter(
    "myota_activity_notification_outcomes_total",
    "Activity notification deliveries by bounded outcome.",
    ("outcome",),
)
UNRESOLVED_DEAD_LETTERS = Gauge(
    "myota_activity_notification_unresolved_dead_letters",
    "Unresolved Activity notification poison events in PostgreSQL.",
)


def refresh_unresolved_dead_letters(database_url: str) -> None:
    with psycopg.connect(database_url) as connection:
        count = connection.execute(
            "SELECT count(*) FROM dead_letter_event "
            "WHERE stream_name='MYOTA_EVENTS' AND resolved_at IS NULL"
        ).fetchone()[0]
    UNRESOLVED_DEAD_LETTERS.set(count)


def _safe_diagnostic(event: dict[str, Any]) -> dict[str, Any]:
    """Redact common credential fields before writing a poison envelope to DB."""
    sensitive = (
        "password",
        "secret",
        "token",
        "authorization",
        "credential",
        "email",
        "address",
        "remoteaddr",
        "ipaddress",
        "phone",
    )

    def clean(value: Any) -> Any:
        if isinstance(value, dict):
            return {
                key: "[REDACTED]"
                if any(part in key.lower() for part in sensitive)
                else clean(item)
                for key, item in value.items()
            }
        if isinstance(value, list):
            return [clean(item) for item in value]
        return value

    return clean(event)


def _fallback_event_id(subject: str, sequence: int) -> str:
    """Create a stable diagnostic UUID for malformed envelopes without an ID."""
    digest = hashlib.sha256(f"{subject}:{sequence}".encode()).hexdigest()
    return str(uuid.UUID(digest[:32]))


async def subscribe_notifications(
    js: Any,
    consumer: str,
    subject: str,
    durable: str = NOTIFICATION_DURABLE,
    start_sequence: int | None = None,
):
    """Bind replicas to the registry-owned exact-filter pull consumer."""
    if consumer != "activity-notifications":
        raise ValueError(f"unregistered Activity event consumer: {consumer}")
    if durable != NOTIFICATION_DURABLE and not start_sequence:
        raise ValueError("replay durables require an explicit start sequence")
    config = ConsumerConfig(
        durable_name=durable,
        filter_subjects=list(ACTIVITY_NOTIFICATION_SUBJECTS),
        ack_policy=AckPolicy.EXPLICIT,
        deliver_policy=(
            DeliverPolicy.BY_START_SEQUENCE
            if start_sequence
            else DeliverPolicy.ALL
        ),
        replay_policy=ReplayPolicy.INSTANT,
        ack_wait=ACK_WAIT_SECONDS,
        max_deliver=MAX_DELIVERIES,
        max_ack_pending=64,
        max_waiting=32,
        backoff=[60, 120, 300, 300, 300, 300, 300, 300],
    )
    if start_sequence:
        config.opt_start_seq = start_sequence
    return await js.pull_subscribe(
        subject,
        stream="MYOTA_EVENTS",
        durable=durable,
        config=config,
    )


async def consume_forever(
    consumer: str,
    subject: str,
    database_url: str,
    handler: Callable[[dict[str, Any], Any], Awaitable[None]],
    supported_major: int = 1,
    stop_event: asyncio.Event | None = None,
    durable: str = NOTIFICATION_DURABLE,
    start_sequence: int | None = None,
    max_messages: int | None = None,
) -> None:
    stop_event = stop_event or asyncio.Event()
    nc = NATS()
    await nc.connect(
        os.environ.get("NATS_URL", "nats://nats:4222"), name=consumer
    )
    sub = None
    try:
        completed = 0

        def count_completed() -> None:
            nonlocal completed
            completed += 1
            if max_messages and completed >= max_messages:
                stop_event.set()

        sub = await subscribe_notifications(
            nc.jetstream(), consumer, subject, durable, start_sequence
        )
        LOG.info("Notification pull consumer subscribed: %s", durable)
        while not stop_event.is_set():
            try:
                messages = await sub.fetch(batch=1, timeout=1)
            except (FetchTimeoutError, NatsTimeoutError):
                continue
            for message in messages:
                metadata = message.metadata
                delivered = metadata.num_delivered if metadata else 1
                event: dict[str, Any] = {}
                event_type = "unknown"
                try:
                    decoded = json.loads(message.data)
                    if not isinstance(decoded, dict):
                        raise ValueError("invalid_envelope")
                    event = decoded
                    event_type = str(event.get("eventType") or "unknown")
                    event_id = event.get("eventId")
                    if not isinstance(event_id, str):
                        raise ValueError("missing_event_id")
                    try:
                        event_id = str(uuid.UUID(event_id))
                    except ValueError as invalid_id:
                        raise ValueError("invalid_event_id") from invalid_id
                    if event_type != message.subject.removeprefix(
                        "myota.events."
                    ):
                        raise ValueError("subject_event_type_mismatch")
                    version = (
                        int(event_type.rsplit(".v", 1)[-1])
                        if ".v" in event_type
                        else 0
                    )
                    if version != supported_major:
                        raise ValueError("unsupported_event_version")
                    with psycopg.connect(
                        database_url, row_factory=dict_row
                    ) as connection:
                        seen = connection.execute(
                            "SELECT 1 FROM consumer_processed_event WHERE consumer=%s AND event_id=%s",
                            (consumer, event_id),
                        ).fetchone()
                        if seen:
                            refresh_unresolved_dead_letters(database_url)
                            await message.ack()
                            OUTCOMES.labels("duplicate").inc()
                            count_completed()
                            continue
                        await handler(event, connection)
                        connection.execute(
                            "INSERT INTO consumer_processed_event(consumer,event_id) VALUES (%s,%s) ON CONFLICT DO NOTHING",
                            (consumer, event_id),
                        )
                        connection.execute(
                            "INSERT INTO consumer_checkpoint(consumer,last_event_id) VALUES (%s,%s) ON CONFLICT (consumer) DO UPDATE SET last_event_id=EXCLUDED.last_event_id,updated_at=now()",
                            (consumer, event_id),
                        )
                        connection.execute(
                            "UPDATE dead_letter_event SET resolved_at=now() WHERE event_id=%s AND stream_name='MYOTA_EVENTS' AND resolved_at IS NULL",
                            (event_id,),
                        )
                    refresh_unresolved_dead_letters(database_url)
                    await message.ack()
                    OUTCOMES.labels("processed").inc()
                    count_completed()
                except Exception as exc:
                    permanent = isinstance(exc, ValueError)
                    if not permanent and delivered < MAX_DELIVERIES:
                        delay = min(5 * (3 ** max(delivered - 1, 0)), 300)
                        LOG.warning(
                            "Activity notification attempt failed; event_id=%s "
                            "event_type=%s delivery=%s",
                            event.get("eventId", "unknown"),
                            event_type,
                            delivered,
                        )
                        await message.nak(delay=delay)
                        OUTCOMES.labels("retry").inc()
                        continue
                    sequence = metadata.sequence.stream if metadata else 0
                    try:
                        diagnostic_id = str(
                            uuid.UUID(str(event.get("eventId")))
                        )
                    except (ValueError, AttributeError):
                        diagnostic_id = _fallback_event_id(
                            message.subject, sequence
                        )
                    # If persistence fails, leave the message available for
                    # redelivery instead of terminating it without evidence.
                    with psycopg.connect(
                        database_url, row_factory=dict_row
                    ) as connection:
                        connection.execute(
                            "INSERT INTO dead_letter_event(event_id,event_type,payload,attempts,error,stream_name,stream_sequence,subject) VALUES (%s,%s,%s::jsonb,%s,%s,'MYOTA_EVENTS',%s,%s) ON CONFLICT (event_id) DO UPDATE SET attempts=GREATEST(dead_letter_event.attempts,EXCLUDED.attempts),error=EXCLUDED.error,stream_name=EXCLUDED.stream_name,stream_sequence=EXCLUDED.stream_sequence,subject=EXCLUDED.subject",
                            (
                                diagnostic_id,
                                event_type,
                                json.dumps(_safe_diagnostic(event)),
                                delivered,
                                "invalid_or_unsupported_envelope"
                                if permanent
                                else "delivery_attempts_exhausted",
                                metadata.sequence.stream if metadata else 0,
                                message.subject,
                            ),
                        )
                    refresh_unresolved_dead_letters(database_url)
                    LOG.error(
                        "Activity notification dead-lettered; event_id=%s "
                        "event_type=%s delivery=%s",
                        diagnostic_id,
                        event_type,
                        delivered,
                    )
                    # The redacted envelope and stream coordinates are now in
                    # the Activity database's reviewed replay workflow.
                    await message.term()
                    OUTCOMES.labels("dead_lettered").inc()
    finally:
        if sub is not None:
            await sub.unsubscribe()
        await nc.drain()
