"""Bounded background worker for activity imports, awards, documents, and notices."""
from __future__ import annotations

import asyncio
import os
import sys
from typing import Any

from activity_domain import normalize_qso, parse_adif, qso_deduplication_key
from activity_repository import ActivityRepository
from awards import AwardsHandler, evaluate_condition
from storage import ObjectStore


def process_adif(repo: ActivityRepository, payload: dict[str, Any]) -> None:
    import_id = payload["importId"]
    record = repo.start_import(import_id)
    content = ObjectStore().get(record["bucket"], record["objectKey"])
    if content is None:
        repo.finish_import(import_id, 0, 0, 1, ["uploaded object is not available"], "FAILED")
        repo.create_notification(record["activationId"], "ADIF_IMPORT_FAILED", {"importId": import_id, "reason": "object unavailable"})
        return
    try:
        ObjectStore.scan_content(content, record["filename"])
        rows = parse_adif(content.decode("utf-8", errors="strict"))
    except Exception as exc:
        repo.finish_import(import_id, 0, 0, 1, [str(exc)], "FAILED")
        repo.create_notification(record["activationId"], "ADIF_IMPORT_FAILED", {"importId": import_id, "reason": str(exc)})
        return
    activation = repo.get_activation(record["activationId"])
    normalized = []
    errors = []
    for index, row in enumerate(rows, start=1):
        try:
            row = normalize_qso({**row, "source": "adif", "sourceImportId": import_id})
            row["deduplicationKey"] = qso_deduplication_key(record["activationId"], row)
            normalized.append(row)
        except Exception as exc:
            errors.append(f"record {index}: {exc}")
    accepted = repo.insert_qso_batch(record["activationId"], normalized) if normalized else []
    result = repo.finish_import(import_id, len(rows), len(accepted), len(rows) - len(accepted) + len(errors), errors)
    if errors or len(accepted) != len(rows):
        repo.create_notification(activation["operatorId"], "ADIF_IMPORT_COMPLETED_WITH_ERRORS", {"importId": import_id, "result": result})
    else:
        repo.create_notification(activation["operatorId"], "ADIF_IMPORT_COMPLETED", {"importId": import_id, "result": result})


def process_award_recalculation(repo: ActivityRepository, payload: dict[str, Any]) -> None:
    programme = payload.get("programmeSlug")
    subjects = {str(value) for value in payload.get("subjectIds", []) if value}
    for award in repo.list_collection("definitions"):
        if award.get("programmeSlug") != programme or award.get("status") not in {"PUBLISHED", "RETIRED"}:
            continue
        if not subjects:
            continue
        for subject_id in subjects:
            facts = repo.subject_facts(programme, subject_id, award.get("category", "HUNTER"))
            condition_met = evaluate_condition(award["condition"], facts)
            metric_field = {"QSO_COUNT": "qsoCount", "UNIQUE_CALLSIGNS": "uniqueCallsignCount", "UNIQUE_ENTITIES": "uniqueEntityCount", "ACTIVATION_COUNT": "activationCount"}.get(award.get("achievementMetric", "QSO_COUNT"), award.get("achievementMetric", "qsoCount"))
            progress = float(facts.get(metric_field, 0))
            levels = [{**level, "eligible": condition_met and progress >= float(level["threshold"])} for level in award.get("levels", [])]
            repo.save_progress(award, subject_id, award.get("category", "HUNTER"), facts, {"conditionMet": condition_met, "metric": award.get("achievementMetric", "QSO_COUNT"), "progress": progress, "levels": levels, "ruleVersion": award.get("version", 1)})
            if any(level.get("eligible") for level in levels):
                repo.create_notification(subject_id, "AWARD_QUALIFIED", {"awardId": award["id"], "awardCode": award["code"], "levels": levels})


def process_pdf(repo: ActivityRepository, payload: dict[str, Any]) -> None:
    issuance_id = payload["issuanceId"]
    AwardsHandler.render_issuance(None, {"issuanceId": issuance_id, "_body": {}})


def process_statistics(repo: ActivityRepository, payload: dict[str, Any]) -> None:
    repo.rebuild_statistics(payload.get("programmeSlug"))


def process_notification(repo: ActivityRepository, payload: dict[str, Any]) -> None:
    with repo.transaction() as connection:
        connection.execute("UPDATE activity_notification SET status='DELIVERED',delivered_at=now() WHERE id=%s AND status='QUEUED'", (payload["notificationId"],))


def process(repo: ActivityRepository, job: dict[str, Any]) -> None:
    AwardsHandler.repository = repo
    handlers = {"ADIF_IMPORT": process_adif, "AWARD_RECALCULATE": process_award_recalculation,
                "PDF_RENDER": process_pdf, "STATISTICS_REBUILD": process_statistics,
                "NOTIFICATION_SEND": process_notification}
    handler = handlers.get(job["kind"])
    if not handler:
        raise ValueError(f"unsupported activity job kind: {job['kind']}")
    handler(repo, job["payload"])


async def main() -> None:
    repo = ActivityRepository("CORE_DATABASE_URL")
    if not repo.durable:
        raise RuntimeError("CORE_DATABASE_URL is required for the activity worker")
    while True:
        job = await asyncio.to_thread(repo.claim_job)
        if not job:
            await asyncio.sleep(float(os.environ.get("MYOTA_ACTIVITY_WORKER_POLL_SECONDS", "1")))
            continue
        try:
            await asyncio.to_thread(process, repo, job)
            await asyncio.to_thread(repo.complete_job, job["id"])
        except Exception as exc:
            await asyncio.to_thread(repo.fail_job, job, exc)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        sys.exit(0)
