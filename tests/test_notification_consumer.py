"""Notification consumer rollout and acknowledgement regressions."""

import asyncio
import json
import os
import unittest
from unittest.mock import AsyncMock, MagicMock, patch

from nats.aio.client import Client as NATS
from nats.js.api import AckPolicy

from event_consumer import consume_forever, subscribe_notifications


class NotificationConsumerTests(unittest.IsolatedAsyncioTestCase):
    async def test_duplicate_event_is_acknowledged_without_notification(self):
        stop = asyncio.Event()
        message = MagicMock()
        message.data = json.dumps(
            {"eventId": "seen-event", "eventType": "identity.changed.v1"}
        ).encode()
        message.ack = AsyncMock(side_effect=stop.set)
        sub = MagicMock()
        sub.fetch = AsyncMock(return_value=[message])
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
        client.drain.assert_awaited_once()
        kwargs = client.jetstream.return_value.pull_subscribe.call_args.kwargs
        self.assertEqual(kwargs["durable"], "activity-notifications-pull-v1")
        self.assertEqual(kwargs["config"].ack_policy, AckPolicy.EXPLICIT)
        self.assertEqual(kwargs["config"].max_ack_pending, 64)

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
            await js.add_stream(
                name="MYOTA_EVENTS", subjects=["myota.events.>"]
            )
            # A legacy push consumer may still exist during the rollout.
            legacy = await js.subscribe(
                "myota.events.>",
                durable="activity-notifications",
                manual_ack=True,
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
                    "myota.events.identity.changed.v1", str(number).encode()
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
            await legacy.unsubscribe()
            await clients[1].drain()
            clients[1] = NATS()
            await clients[1].connect(os.environ["NATS_TEST_URL"])
            restarted = await subscribe_notifications(
                clients[1].jetstream(),
                "activity-notifications",
                "myota.events.>",
            )
            await js.publish("myota.events.identity.changed.v1", b"2")
            messages = await restarted.fetch(1, timeout=3)
            self.assertEqual(messages[0].data, b"2")
            await messages[0].ack_sync()
            info = await js.consumer_info(
                "MYOTA_EVENTS", "activity-notifications-pull-v1"
            )
            self.assertEqual(info.num_ack_pending, 0)
            self.assertEqual(info.num_pending, 0)
        finally:
            for client in clients:
                if not client.is_closed:
                    await client.drain()
