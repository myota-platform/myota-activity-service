"""JetStream delivery and database-state handling for Activity work."""

import asyncio
import json
import unittest
from datetime import UTC, datetime
from unittest.mock import AsyncMock, MagicMock, patch
from uuid import UUID

from nats.js.api import AckPolicy, ConsumerConfig

from activity_worker import (
    CONSUMER_LIMITS,
    MAX_DELIVERIES,
    WORKERS,
    consume_kind,
    handle_message,
    validate_consumer_config,
)


JOB_ID = str(UUID("d4cb7610-6077-4b33-93d7-fefdd3c4a2a1"))
LEASE_TOKEN = str(UUID("a3108632-010b-40e3-a084-d7eac65039a0"))


def command(kind: str = "QSO_INGESTION", work_id: str = JOB_ID) -> dict:
    subject, _durable = WORKERS[kind]
    return {
        "subject": subject,
        "envelopeVersion": 1,
        "workId": work_id,
        "workType": subject.removeprefix("myota.work."),
        "createdAt": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
        "producer": "activity-service",
        "aggregate": {"type": "activity_job", "id": work_id},
        "payload": {"jobId": work_id},
    }


class ActivityWorkConsumerTests(unittest.IsolatedAsyncioTestCase):
    def message(self, envelope: dict, delivery: int = 1):
        message = MagicMock()
        body = dict(envelope)
        message.subject = body.pop("subject", WORKERS["QSO_INGESTION"][0])
        message.data = json.dumps(body).encode()
        message.metadata.num_delivered = delivery
        message.ack = AsyncMock()
        message.nak = AsyncMock()
        message.term = AsyncMock()
        message.in_progress = AsyncMock()
        return message

    async def test_commit_completion_precedes_ack(self):
        repo = MagicMock()
        repo.claim_work_job.return_value = {
            "id": JOB_ID,
            "kind": "QSO_INGESTION",
            "payload": {"records": []},
            "attempts": 1,
            "leaseToken": LEASE_TOKEN,
        }
        message = self.message(command())
        order = []
        repo.complete_job.side_effect = lambda _job_id, _lease: order.append(
            "complete"
        )
        message.ack.side_effect = lambda: order.append("ack")
        with patch("activity_worker.process"):
            await handle_message(repo, "QSO_INGESTION", message)
        self.assertEqual(order, ["complete", "ack"])
        repo.claim_work_job.assert_called_once_with(JOB_ID, 120)

    async def test_already_completed_job_is_acked_without_side_effect(self):
        repo = MagicMock()
        repo.claim_work_job.return_value = None
        repo.get_job.return_value = {
            "id": JOB_ID,
            "kind": "QSO_INGESTION",
            "status": "SUCCEEDED",
        }
        message = self.message(command())
        with patch("activity_worker.process") as process:
            await handle_message(repo, "QSO_INGESTION", message)
        process.assert_not_called()
        message.ack.assert_awaited_once()

    async def test_transient_failure_is_recorded_then_naked(self):
        repo = MagicMock()
        repo.claim_work_job.return_value = {
            "id": JOB_ID,
            "kind": "QSO_INGESTION",
            "payload": {"records": []},
            "attempts": 1,
            "leaseToken": LEASE_TOKEN,
        }
        message = self.message(command())
        with patch(
            "activity_worker.process", side_effect=RuntimeError("retry")
        ):
            await handle_message(repo, "QSO_INGESTION", message)
        repo.retry_work_job.assert_called_once_with(
            JOB_ID, LEASE_TOKEN, unittest.mock.ANY, 2
        )
        message.nak.assert_awaited_once_with(delay=2)
        message.ack.assert_not_awaited()

    async def test_retry_delay_does_not_consume_jetstream_delivery_attempts(
        self,
    ):
        repo = MagicMock()
        repo.claim_work_job.side_effect = [
            None,
            {
                "id": JOB_ID,
                "kind": "QSO_INGESTION",
                "payload": {"records": []},
                "attempts": 2,
                "leaseToken": LEASE_TOKEN,
            },
        ]
        repo.get_job.return_value = {
            "id": JOB_ID,
            "kind": "QSO_INGESTION",
            "status": "QUEUED",
            "retryAfterSeconds": 0.01,
        }
        message = self.message(command())
        with patch("activity_worker.process"):
            await handle_message(repo, "QSO_INGESTION", message)
        self.assertEqual(repo.claim_work_job.call_count, 2)
        message.in_progress.assert_awaited_once()
        message.nak.assert_not_awaited()
        message.ack.assert_awaited_once()

    async def test_exhausted_delivery_is_terminal_in_database_before_ack(self):
        repo = MagicMock()
        repo.claim_work_job.return_value = {
            "id": "job-1",
            "kind": "QSO_INGESTION",
            "payload": {"records": []},
            "attempts": 8,
            "leaseToken": LEASE_TOKEN,
        }
        message = self.message(command(), delivery=8)
        order = []
        repo.fail_work_job.side_effect = lambda *_args: order.append("failed")
        message.ack.side_effect = lambda: order.append("ack")
        with patch(
            "activity_worker.process", side_effect=RuntimeError("poison")
        ):
            await handle_message(repo, "QSO_INGESTION", message)
        self.assertEqual(order, ["failed", "ack"])
        repo.fail_work_job.assert_called_once_with(
            JOB_ID,
            LEASE_TOKEN,
            unittest.mock.ANY,
            WORKERS["QSO_INGESTION"][0],
            "activity.qso-ingestion.v1",
            8,
        )

    async def test_each_registered_kind_accepts_its_exact_contract_route(self):
        for kind, (subject, _durable) in WORKERS.items():
            with self.subTest(kind=kind):
                repo = MagicMock()
                repo.claim_work_job.return_value = {
                    "id": JOB_ID,
                    "kind": kind,
                    "payload": {},
                    "attempts": 1,
                    "leaseToken": LEASE_TOKEN,
                }
                message = self.message(command(kind))
                with patch("activity_worker.process"):
                    await handle_message(repo, kind, message)
                repo.claim_work_job.assert_called_once_with(JOB_ID, 120)
                message.ack.assert_awaited_once()
                self.assertEqual(message.subject, subject)

    def test_registered_durable_requires_explicit_pull_policy(self):
        kind = "PDF_RENDER"
        subject, durable = WORKERS[kind]
        ack_wait, max_pending, max_waiting = CONSUMER_LIMITS[kind]
        config = ConsumerConfig(
            filter_subject=subject,
            ack_policy=AckPolicy.EXPLICIT,
            ack_wait=ack_wait,
            max_deliver=MAX_DELIVERIES,
            max_ack_pending=max_pending,
            max_waiting=max_waiting,
        )
        validate_consumer_config(
            config,
            kind,
            subject,
            durable,
            ack_wait,
            max_pending,
            max_waiting,
        )
        config.filter_subject = "myota.work.activity.>"
        with self.assertRaisesRegex(RuntimeError, "does not match"):
            validate_consumer_config(
                config,
                kind,
                subject,
                durable,
                ack_wait,
                max_pending,
                max_waiting,
            )

    async def test_worker_binds_to_preprovisioned_durable_only(self):
        kind = "PDF_RENDER"
        subject, durable = WORKERS[kind]
        ack_wait, max_pending, max_waiting = CONSUMER_LIMITS[kind]
        info = MagicMock()
        info.config = ConsumerConfig(
            filter_subject=subject,
            ack_policy=AckPolicy.EXPLICIT,
            ack_wait=ack_wait,
            max_deliver=MAX_DELIVERIES,
            max_ack_pending=max_pending,
            max_waiting=max_waiting,
        )
        subscription = MagicMock()
        subscription.unsubscribe = AsyncMock()
        js = MagicMock()
        js.consumer_info = AsyncMock(return_value=info)
        js.pull_subscribe_bind = AsyncMock(return_value=subscription)
        nc = MagicMock()
        nc.jetstream.return_value = js
        stop = asyncio.Event()
        stop.set()

        await consume_kind(nc, MagicMock(), kind, stop)

        js.consumer_info.assert_awaited_once_with(
            "MYOTA_ACTIVITY_WORK", durable
        )
        js.pull_subscribe_bind.assert_awaited_once_with(
            stream="MYOTA_ACTIVITY_WORK", durable=durable
        )
        js.pull_subscribe.assert_not_called()
        subscription.unsubscribe.assert_awaited_once()

    async def test_invalid_envelope_is_recorded_and_terminated(self):
        repo = MagicMock()
        message = self.message({"subject": WORKERS["QSO_INGESTION"][0]})
        await handle_message(repo, "QSO_INGESTION", message)
        repo.record_work_dead_letter.assert_called_once()
        message.term.assert_awaited_once()
        message.ack.assert_not_awaited()
