"""Notification consumer rollout and acknowledgement regressions."""

import asyncio
import json
import os
import unittest
from unittest.mock import AsyncMock, MagicMock, patch

from nats.aio.client import Client as NATS
from nats.js.api import AckPolicy, DeliverPolicy, ReplayPolicy

from activity_notification_consumer import notice_payload
from event_consumer import (
    ACTIVITY_NOTIFICATION_SUBJECTS,
    NOTIFICATION_DURABLE,
    _safe_diagnostic,
    consume_forever,
    subscribe_notifications,
)


class NotificationConsumerTests(unittest.IsolatedAsyncioTestCase):
    def test_notice_projection_excludes_source_payload(self):
        self.assertEqual(
            notice_payload(
                {
                    "eventId": "event-1",
                    "eventType": "identity.login.failed.v1",
                    "payload": {"email": "person@example.test"},
                }
            ),
            {
                "eventId": "event-1",
                "eventType": "identity.login.failed.v1",
            },
        )

    async def test_duplicate_event_is_acknowledged_without_notification(self):
        stop = asyncio.Event()
        message = MagicMock()
        message.data = json.dumps(
            {
                "eventId": "11111111-1111-4111-8111-111111111111",
                "eventType": "identity.account.created.v1",
            }
        ).encode()
        message.subject = "myota.events.identity.account.created.v1"
        message.metadata.num_delivered = 1
        message.ack = AsyncMock(side_effect=stop.set)
        sub = MagicMock()
        sub.fetch = AsyncMock(return_value=[message])
        sub.unsubscribe = AsyncMock()
        client = MagicMock()
        client.connect = AsyncMock()
        client.drain = AsyncMock()
        client.jetstream.return_value.pull_subscribe = AsyncMock(
            return_value=sub
        )
        connection = MagicMock()
        connection.execute.return_value.fetchone.return_value = (1,)
        handler = AsyncMock()
        with (
            patch("event_consumer.NATS", return_value=client),
            patch("event_consumer.psycopg.connect") as connect,
            patch("event_consumer.refresh_unresolved_dead_letters"),
        ):
            connect.return_value.__enter__.return_value = connection
            await consume_forever(
                "activity-notifications",
                "myota.events.>",
                "unused-dsn",
                handler,
                stop_event=stop,
            )
        handler.assert_not_awaited()
        message.ack.assert_awaited_once()
        sub.unsubscribe.assert_awaited_once()
        client.drain.assert_awaited_once()
        kwargs = client.jetstream.return_value.pull_subscribe.call_args.kwargs
        self.assertEqual(kwargs["durable"], NOTIFICATION_DURABLE)
        self.assertEqual(kwargs["config"].ack_policy, AckPolicy.EXPLICIT)
        self.assertEqual(
            tuple(kwargs["config"].filter_subjects),
            ACTIVITY_NOTIFICATION_SUBJECTS,
        )
        self.assertEqual(kwargs["config"].deliver_policy, DeliverPolicy.ALL)
        self.assertEqual(kwargs["config"].replay_policy, ReplayPolicy.INSTANT)
        self.assertEqual(kwargs["config"].max_ack_pending, 64)
        self.assertEqual(kwargs["config"].max_deliver, 8)

    async def test_commit_then_lost_ack_redelivery_does_not_repeat_handler(
        self,
    ):
        stop = asyncio.Event()
        message = MagicMock()
        message.data = json.dumps(
            {
                "eventId": "22222222-2222-4222-8222-222222222222",
                "eventType": "identity.account.created.v1",
            }
        ).encode()
        message.subject = "myota.events.identity.account.created.v1"
        message.metadata.num_delivered = 1
        message.metadata.sequence.stream = 12
        ack_attempts = 0

        async def ack_with_lost_response():
            nonlocal ack_attempts
            ack_attempts += 1
            if ack_attempts == 1:
                raise RuntimeError("ack response lost")
            stop.set()

        message.ack = AsyncMock(side_effect=ack_with_lost_response)
        message.nak = AsyncMock()
        sub = MagicMock()
        sub.fetch = AsyncMock(side_effect=[[message], [message]])
        sub.unsubscribe = AsyncMock()
        client = MagicMock()
        client.connect = AsyncMock()
        client.drain = AsyncMock()
        client.jetstream.return_value.pull_subscribe = AsyncMock(
            return_value=sub
        )
        connection = MagicMock()
        connection.execute.return_value.fetchone.side_effect = [None, (1,)]
        handler = AsyncMock()

        with (
            patch("event_consumer.NATS", return_value=client),
            patch("event_consumer.psycopg.connect") as connect,
            patch("event_consumer.refresh_unresolved_dead_letters"),
        ):
            connect.return_value.__enter__.return_value = connection
            await consume_forever(
                "activity-notifications",
                "myota.events.>",
                "unused-dsn",
                handler,
                stop_event=stop,
            )

        handler.assert_awaited_once_with(json.loads(message.data), connection)
        message.nak.assert_awaited_once()
        self.assertEqual(message.ack.await_count, 2)

    def test_dead_letter_diagnostic_redacts_sensitive_fields(self):
        diagnostic = _safe_diagnostic(
            {
                "eventId": "id",
                "payload": {
                    "email": "person@example.test",
                    "serviceToken": "secret-value",
                    "accountId": "account-1",
                },
            }
        )
        self.assertEqual(diagnostic["payload"]["email"], "[REDACTED]")
        self.assertEqual(diagnostic["payload"]["serviceToken"], "[REDACTED]")
        self.assertEqual(diagnostic["payload"]["accountId"], "account-1")

    async def test_subscription_failure_still_drains_connection(self):
        client = MagicMock()
        client.connect = AsyncMock()
        client.drain = AsyncMock()
        client.jetstream.return_value.pull_subscribe = AsyncMock(
            side_effect=RuntimeError("broker unavailable")
        )
        with patch("event_consumer.NATS", return_value=client):
            with self.assertRaisesRegex(RuntimeError, "broker unavailable"):
                await consume_forever(
                    "activity-notifications", "myota.events.>", "", AsyncMock()
                )
        client.drain.assert_awaited_once()


@unittest.skipUnless(
    os.environ.get("NATS_TEST_URL"), "isolated JetStream is not configured"
)
class NotificationBrokerTests(unittest.IsolatedAsyncioTestCase):
    async def test_overlapping_replicas_and_restart_share_durable(self):
        clients = [NATS(), NATS()]
        try:
            for client in clients:
                await client.connect(os.environ["NATS_TEST_URL"])
            js = clients[0].jetstream()
            try:
                await js.delete_stream("MYOTA_EVENTS")
            except Exception:
                pass
            await js.add_stream(
                name="MYOTA_EVENTS", subjects=["myota.events.>"]
            )
            subscriptions = [
                await subscribe_notifications(
                    client.jetstream(),
                    "activity-notifications",
                    "myota.events.>",
                )
                for client in clients
            ]
            for number in range(2):
                await js.publish(
                    "myota.events.identity.account.created.v1",
                    str(number).encode(),
                )
            batches = await asyncio.gather(
                *(sub.fetch(1, timeout=3) for sub in subscriptions)
            )
            self.assertEqual(
                {message.data for batch in batches for message in batch},
                {b"0", b"1"},
            )
            for batch in batches:
                await batch[0].ack_sync()
            await clients[1].drain()
            clients[1] = NATS()
            await clients[1].connect(os.environ["NATS_TEST_URL"])
            restarted = await subscribe_notifications(
                clients[1].jetstream(),
                "activity-notifications",
                "myota.events.>",
            )
            await js.publish("myota.events.identity.account.created.v1", b"2")
            messages = await restarted.fetch(1, timeout=3)
            self.assertEqual(messages[0].data, b"2")
            await messages[0].ack_sync()
            info = await js.consumer_info("MYOTA_EVENTS", NOTIFICATION_DURABLE)
            self.assertEqual(info.num_ack_pending, 0)
            self.assertEqual(info.num_pending, 0)
        finally:
            for client in clients:
                if not client.is_closed:
                    await client.drain()
