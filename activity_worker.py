"""Bounded background worker for activity imports, awards, documents, and notices."""

from __future__ import annotations

import asyncio
import json
import logging
import os
import signal
import sys
from typing import Any
from uuid import UUID

from nats.aio.client import Client as NATS
from nats.js.api import AckPolicy

from activity_domain import normalize_qso, parse_adif, qso_deduplication_key
from activity_repository import ActivityRepository
from awards import AwardsHandler, evaluate_condition
from storage import ObjectStore


LOG = logging.getLogger("myota.activity.work")
WORKERS = {
    "QSO_INGESTION": (
        "myota.work.activity.qso-ingestion.v1",
        "activity-qso-ingestion-v1",
    ),
    "ADIF_IMPORT": (
        "myota.work.activity.adif-import.v1",
        "activity-adif-import-v1",
    ),
    "AWARD_RECALCULATE": (
        "myota.work.activity.award-recalculate.v1",
        "activity-award-recalculate-v1",
    ),
    "AWARD_EVALUATION": (
        "myota.work.activity.award-evaluation.v1",
        "activity-award-evaluation-v1",
    ),
    "PDF_RENDER": (
        "myota.work.activity.pdf-render.v1",
        "activity-pdf-render-v1",
    ),
    "STATISTICS_REBUILD": (
        "myota.work.activity.statistics-rebuild.v1",
        "activity-statistics-rebuild-v1",
    ),
}
# Keep these values aligned with the deploy-owned topology in
# myota-deploy/services/jetstream_topology.py. Pull delivery must bind to the
# pre-created durable; it must never auto-create a weaker default consumer.
CONSUMER_LIMITS = {
    "QSO_INGESTION": (120, 4, 4),
    "ADIF_IMPORT": (300, 1, 1),
    "AWARD_RECALCULATE": (120, 2, 2),
    "AWARD_EVALUATION": (120, 2, 2),
    "PDF_RENDER": (300, 1, 1),
    "STATISTICS_REBUILD": (300, 1, 1),
}
MAX_DELIVERIES = 8
LEASE_SECONDS = 120
LEGACY_RECONCILIATION_ENABLED = os.environ.get(
    "ACTIVITY_LEGACY_RECONCILIATION_ENABLED", "0"
).lower() in {"1", "true", "yes"}


def process_adif(repo: ActivityRepository, payload: dict[str, Any]) -> None:
    import_id = payload["importId"]
    record = repo.start_import(import_id)
    content = ObjectStore().get(record["bucket"], record["objectKey"])
    if content is None:
        repo.finish_import(
            import_id, 0, 0, 1, ["uploaded object is not available"], "FAILED"
        )
        repo.create_notification(
            record["activationId"],
            "ADIF_IMPORT_FAILED",
            {"importId": import_id, "reason": "object unavailable"},
            f"adif:{import_id}:failed",
        )
        return
    try:
        ObjectStore.scan_content(content, record["filename"])
        rows = parse_adif(content.decode("utf-8", errors="strict"))
    except Exception as exc:
        repo.finish_import(import_id, 0, 0, 1, [str(exc)], "FAILED")
        repo.create_notification(
            record["activationId"],
            "ADIF_IMPORT_FAILED",
            {"importId": import_id, "reason": str(exc)},
            f"adif:{import_id}:failed",
        )
        return
    activation = repo.get_activation(record["activationId"])
    normalized = []
    errors = []
    for index, row in enumerate(rows, start=1):
        try:
            row = normalize_qso(
                {**row, "source": "adif", "sourceImportId": import_id}
            )
            row["deduplicationKey"] = qso_deduplication_key(
                record["activationId"], row
            )
            normalized.append(row)
        except Exception as exc:
            errors.append(f"record {index}: {exc}")
    accepted = (
        repo.insert_qso_batch(record["activationId"], normalized)
        if normalized
        else []
    )
    result = repo.finish_import(
        import_id,
        len(rows),
        len(accepted),
        len(rows) - len(accepted) + len(errors),
        errors,
    )
    if errors or len(accepted) != len(rows):
        repo.create_notification(
            activation["operatorId"],
            "ADIF_IMPORT_COMPLETED_WITH_ERRORS",
            {"importId": import_id, "result": result},
            f"adif:{import_id}:completed",
        )
    else:
        repo.create_notification(
            activation["operatorId"],
            "ADIF_IMPORT_COMPLETED",
            {"importId": import_id, "result": result},
            f"adif:{import_id}:completed",
        )


def process_award_recalculation(
    repo: ActivityRepository, payload: dict[str, Any]
) -> None:
    programme = payload.get("programmeSlug")
    subjects = {str(value) for value in payload.get("subjectIds", []) if value}
    definitions = repo.list_collection("definitions")
    award_id = payload.get("awardId")
    if award_id:
        definitions = [
            award
            for award in definitions
            if str(award.get("id")) == str(award_id)
        ]
    for award in definitions:
        if award.get("programmeSlug") != programme or award.get(
            "status"
        ) not in {"PUBLISHED", "RETIRED"}:
            continue
        if payload.get("ruleVersion") is not None and int(
            award.get("version", 1)
        ) != int(payload["ruleVersion"]):
            continue
        award_subjects = sorted(
            subjects
            or set(
                repo.list_subject_ids(
                    programme, award.get("category", "HUNTER")
                )
            )
        )
        for subject_id in award_subjects:
            facts = repo.subject_facts(
                programme, subject_id, award.get("category", "HUNTER")
            )
            condition_met = evaluate_condition(award["condition"], facts)
            metric_field = {
                "QSO_COUNT": "qsoCount",
                "UNIQUE_CALLSIGNS": "uniqueCallsignCount",
                "UNIQUE_ENTITIES": "uniqueEntityCount",
                "ACTIVATION_COUNT": "activationCount",
            }.get(
                award.get("achievementMetric", "QSO_COUNT"),
                award.get("achievementMetric", "qsoCount"),
            )
            progress = float(facts.get(metric_field, 0))
            levels = [
                {
                    **level,
                    "eligible": condition_met
                    and progress >= float(level["threshold"]),
                }
                for level in award.get("levels", [])
            ]
            repo.save_progress(
                award,
                subject_id,
                award.get("category", "HUNTER"),
                facts,
                {
                    "conditionMet": condition_met,
                    "metric": award.get("achievementMetric", "QSO_COUNT"),
                    "progress": progress,
                    "levels": levels,
                    "ruleVersion": award.get("version", 1),
                },
            )
            if any(level.get("eligible") for level in levels):
                eligible_level_key = ",".join(
                    str(level["id"])
                    for level in levels
                    if level.get("eligible")
                )
                repo.create_notification(
                    subject_id,
                    "AWARD_QUALIFIED",
                    {
                        "awardId": award["id"],
                        "awardCode": award["code"],
                        "levels": levels,
                    },
                    f"award-qualified:{award['id']}:{award.get('version', 1)}:{subject_id}:{eligible_level_key}",
                )


def process_pdf(repo: ActivityRepository, payload: dict[str, Any]) -> None:
    issuance_id = payload["issuanceId"]
    AwardsHandler.render_issuance(
        None, {"issuanceId": issuance_id, "_body": {}}
    )


def process_statistics(
    repo: ActivityRepository, payload: dict[str, Any]
) -> None:
    repo.rebuild_statistics(payload.get("programmeSlug"))


def process_qso_ingestion(
    repo: ActivityRepository, payload: dict[str, Any]
) -> None:
    repo.get_activation(payload["activationId"])
    normalized = []
    for row in payload["records"]:
        record = normalize_qso(
            {**row, "source": payload.get("sourceFormat", "JSON")}
        )
        record["deduplicationKey"] = qso_deduplication_key(
            payload["activationId"], record
        )
        normalized.append(record)
    repo.insert_qso_batch(payload["activationId"], normalized)


def process_award_evaluation(
    repo: ActivityRepository, payload: dict[str, Any]
) -> None:
    award = repo.get_collection_record("definitions", payload["awardId"])
    subject_id = payload["subjectId"]
    facts = payload.get("facts") or repo.subject_facts(
        award["programmeSlug"], subject_id, award.get("category", "HUNTER")
    )
    condition_met = evaluate_condition(award["condition"], facts)
    metric_field = {
        "QSO_COUNT": "qsoCount",
        "UNIQUE_CALLSIGNS": "uniqueCallsignCount",
        "UNIQUE_ENTITIES": "uniqueEntityCount",
        "ACTIVATION_COUNT": "activationCount",
    }.get(
        award.get("achievementMetric", "QSO_COUNT"),
        award.get("achievementMetric", "qsoCount"),
    )
    progress = float(facts.get(metric_field, 0))
    levels = [
        {
            **level,
            "eligible": condition_met
            and progress >= float(level["threshold"]),
        }
        for level in award.get("levels", [])
    ]
    repo.save_progress(
        award,
        subject_id,
        award.get("category", "HUNTER"),
        facts,
        {
            "awardId": award["id"],
            "subjectId": subject_id,
            "conditionMet": condition_met,
            "metric": award.get("achievementMetric", "QSO_COUNT"),
            "progress": progress,
            "levels": levels,
            "ruleVersion": award.get("version", 1),
        },
    )
    if any(level.get("eligible") for level in levels):
        repo.create_notification(
            subject_id,
            "AWARD_QUALIFIED",
            {"awardId": award["id"], "levels": levels},
            f"award-qualified:{award['id']}:{award.get('version', 1)}:{subject_id}",
        )


def process(repo: ActivityRepository, job: dict[str, Any]) -> None:
    AwardsHandler.repository = repo
    handlers = {
        "ADIF_IMPORT": process_adif,
        "QSO_INGESTION": process_qso_ingestion,
        "AWARD_RECALCULATE": process_award_recalculation,
        "AWARD_EVALUATION": process_award_evaluation,
        "PDF_RENDER": process_pdf,
        "STATISTICS_REBUILD": process_statistics,
    }
    handler = handlers.get(job["kind"])
    if not handler:
        raise ValueError(f"unsupported activity job kind: {job['kind']}")
    handler(repo, job["payload"])


async def handle_message(
    repo: ActivityRepository, kind: str, message: Any
) -> None:
    subject, _durable = WORKERS[kind]
    delivery = int(getattr(message.metadata, "num_delivered", 1))
    lease_token: str | None = None
    try:
        envelope = json.loads(message.data)
        work_id = str(envelope["workId"])
        work_type = envelope["workType"]
        payload = envelope["payload"]
        required = {
            "envelopeVersion",
            "workId",
            "workType",
            "createdAt",
            "producer",
            "aggregate",
            "payload",
        }
        allowed = required | {"correlationId", "causationId"}
        if (
            envelope.get("envelopeVersion") != 1
            or work_type != subject.removeprefix("myota.work.")
            or not required.issubset(envelope)
            or not set(envelope).issubset(allowed)
            or envelope.get("producer") != "activity-service"
            or not str(envelope.get("createdAt", "")).endswith("Z")
            or str(UUID(work_id)) != work_id
            or not isinstance(payload, dict)
            or set(payload) != {"jobId"}
            or payload.get("jobId") != work_id
            or envelope.get("aggregate")
            != {"type": "activity_job", "id": work_id}
            or message.subject != subject
        ):
            raise ValueError("work contract mismatch")
    except Exception:
        await asyncio.to_thread(
            repo.record_work_dead_letter,
            str(getattr(message, "subject", "unknown")),
            None,
            None,
            delivery,
            "invalid_envelope",
        )
        await message.term()
        return

    try:
        claimed = await asyncio.to_thread(
            repo.claim_work_job, work_id, LEASE_SECONDS
        )
        while claimed is None:
            try:
                current = await asyncio.to_thread(repo.get_job, work_id)
            except KeyError:
                await asyncio.to_thread(
                    repo.record_work_dead_letter,
                    message.subject,
                    work_id,
                    work_type,
                    delivery,
                    "job_row_missing",
                )
                await message.term()
                return
            if current["kind"] != kind:
                await asyncio.to_thread(
                    repo.record_work_dead_letter,
                    message.subject,
                    work_id,
                    work_type,
                    delivery,
                    "job_kind_mismatch",
                )
                await message.term()
                return
            if current["status"] in {"SUCCEEDED", "FAILED"}:
                await message.ack()
                return
            wait_seconds = (
                current.get("retryAfterSeconds", 0)
                if current["status"] == "QUEUED"
                else current.get("leaseRemainingSeconds", 0)
            )
            try:
                await message.in_progress()
            except Exception:
                LOG.warning(
                    "could not extend ACK while waiting for Activity job lease",
                    extra={"kind": kind},
                )
            await asyncio.sleep(min(max(float(wait_seconds), 0.25), 30))
            claimed = await asyncio.to_thread(
                repo.claim_work_job, work_id, LEASE_SECONDS
            )

        task = asyncio.create_task(asyncio.to_thread(process, repo, claimed))
        lease_token = claimed["leaseToken"]
        while not task.done():
            done, _pending = await asyncio.wait({task}, timeout=30)
            if done:
                break
            await asyncio.to_thread(
                repo.heartbeat_work_job,
                work_id,
                lease_token,
                LEASE_SECONDS,
            )
            try:
                await message.in_progress()
            except Exception:
                # The job lease still prevents a second consumer from running
                # the handler while JetStream attempts redelivery.
                LOG.warning("work progress ACK failed", extra={"kind": kind})
        await task
        await asyncio.to_thread(repo.complete_job, work_id, lease_token)
        await message.ack()
    except Exception as exc:
        delay = min(300, 2 ** min(max(1, delivery), 8))
        try:
            if lease_token is None:
                await message.nak(delay=delay)
                return
            if delivery >= MAX_DELIVERIES:
                await asyncio.to_thread(
                    repo.fail_work_job,
                    work_id,
                    lease_token,
                    exc,
                    message.subject,
                    work_type,
                    delivery,
                )
                await message.ack()
            else:
                await asyncio.to_thread(
                    repo.retry_work_job,
                    work_id,
                    lease_token,
                    exc,
                    delay,
                )
                await message.nak(delay=delay)
        except Exception:
            # Leave the message unacknowledged if durable failure/retry state
            # could not be persisted; JetStream will redeliver it.
            LOG.exception(
                "could not persist work retry state", extra={"kind": kind}
            )


async def consume_kind(
    nc: NATS, repo: ActivityRepository, kind: str, stop_event: asyncio.Event
) -> None:
    subject, durable = WORKERS[kind]
    js = nc.jetstream()
    info = await js.consumer_info("MYOTA_ACTIVITY_WORK", durable)
    config = info.config
    ack_wait, max_ack_pending, max_waiting = CONSUMER_LIMITS[kind]
    validate_consumer_config(
        config,
        kind,
        subject,
        durable,
        ack_wait,
        max_ack_pending,
        max_waiting,
    )
    subscription = await js.pull_subscribe_bind(
        stream="MYOTA_ACTIVITY_WORK", durable=durable
    )
    try:
        while not stop_event.is_set():
            try:
                messages = await subscription.fetch(1, timeout=2)
            except Exception as exc:
                if "timeout" in str(exc).lower():
                    continue
                raise
            for message in messages:
                await handle_message(repo, kind, message)
    finally:
        await subscription.unsubscribe()


def validate_consumer_config(
    config: Any,
    kind: str,
    subject: str,
    durable: str,
    ack_wait: int,
    max_ack_pending: int,
    max_waiting: int,
) -> None:
    if (
        config.filter_subject != subject
        or config.ack_policy != AckPolicy.EXPLICIT
        or config.ack_wait != ack_wait
        or config.max_deliver != MAX_DELIVERIES
        or config.max_ack_pending != max_ack_pending
        or config.max_waiting != max_waiting
        or config.deliver_subject
    ):
        raise RuntimeError(
            f"Activity work durable {durable} does not match its registered pull policy ({kind})"
        )


async def reconcile_legacy_work(
    repo: ActivityRepository, stop_event: asyncio.Event
) -> None:
    """Translate only queued rows lacking an outbox command during rollout."""
    while not stop_event.is_set():
        try:
            repaired = await asyncio.to_thread(
                repo.reconcile_queued_work_outbox
            )
            if repaired:
                LOG.warning(
                    "repaired queued Activity work outbox rows",
                    extra={"count": repaired},
                )
        except Exception as exc:
            LOG.error(
                "Activity work outbox reconciliation failed",
                extra={"failure_class": type(exc).__name__},
            )
        try:
            await asyncio.wait_for(stop_event.wait(), timeout=30)
        except TimeoutError:
            pass


async def main() -> None:
    logging.basicConfig(level=os.environ.get("LOG_LEVEL", "INFO").upper())
    repo = ActivityRepository("ACTIVITY_DATABASE_URL")
    if not repo.durable:
        raise RuntimeError(
            "ACTIVITY_DATABASE_URL is required for the activity worker"
        )
    if LEGACY_RECONCILIATION_ENABLED:
        await asyncio.to_thread(repo.reconcile_queued_work_outbox)
    nc = NATS()
    await nc.connect(os.environ.get("NATS_URL", "nats://myota-nats:4222"))
    stop_event = asyncio.Event()
    loop = asyncio.get_running_loop()
    for signal_number in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(signal_number, stop_event.set)
    try:
        tasks = [consume_kind(nc, repo, kind, stop_event) for kind in WORKERS]
        if LEGACY_RECONCILIATION_ENABLED:
            tasks.append(reconcile_legacy_work(repo, stop_event))
        await asyncio.gather(*tasks)
    finally:
        await nc.drain()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        sys.exit(0)
