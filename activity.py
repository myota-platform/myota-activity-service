"""Activity execution API and the single activity/awards service process."""

from __future__ import annotations

import base64
import os
from datetime import datetime, timedelta, timezone
from typing import Any

from activity_domain import (
    evaluate_activation_rules,
    mask_callsign,
    normalize_qso,
    qso_deduplication_key,
    validate_activation,
)
from activity_repository import ActivityRepository
from common import (
    BoundedThreadingHTTPServer,
    JsonHandler,
    Store,
    new_id,
    now,
    page_result,
    read_json,
    require,
    verify_token,
)
from awards import AwardsHandler
from storage import ObjectStore


class ActivityHandler(JsonHandler):
    service = "activity-service"
    # The generic state store is deliberately test-only for this service. In
    # durable mode all activity and award writes use ActivityRepository.
    store = Store("activity", "ACTIVITY_DATABASE_URL", persist_state=False)
    repository = ActivityRepository("ACTIVITY_DATABASE_URL")

    def read_request_body(self) -> dict[str, Any]:
        if self.current_route == ("POST", "/v1/awards/previews"):
            if int(self.headers.get("Content-Length", "0")) > 64 * 1024:
                raise ValueError("preview design exceeds 64 KiB")
        if self.current_route == (
            "PUT",
            "/v1/awards/assets/{assetId}/content",
        ):
            from certificate_design import MAX_ASSET_BYTES

            self._authorize(
                {
                    "_http": "1",
                    "Authorization": self.headers.get("Authorization", ""),
                },
                {"awards.admin"},
            )
            media_type = self.headers.get("Content-Type", "").split(";")[0]
            if media_type not in {"image/png", "image/jpeg"}:
                raise ValueError("upload must use image/png or image/jpeg")
            length = int(self.headers.get("Content-Length", "0"))
            if not 0 < length <= MAX_ASSET_BYTES:
                raise ValueError("image upload must be at most 20 MiB")
            content = self.rfile.read(length)
            if len(content) != length:
                raise ValueError("image upload ended unexpectedly")
            return {"_imageBytes": content, "_mediaType": media_type}
        return read_json(self)

    @classmethod
    def metrics_extra(cls) -> dict[str, float]:
        if not cls.repository.durable:
            return {}
        try:
            return cls.repository.metrics()
        except Exception:
            # Metrics must never make the API unavailable when the database is
            # restarting or a migration is in progress.
            return {"myota_activity_metrics_database_unavailable": 1}

    @staticmethod
    def _claims(p: dict[str, str]) -> dict[str, Any]:
        authorization = p.get("Authorization", "")
        if not authorization.startswith("Bearer "):
            return {}
        return verify_token(authorization[7:])

    @staticmethod
    def _authorize(p: dict[str, str], scopes: set[str]) -> dict[str, Any]:
        claims = ActivityHandler._claims(p)
        if p.get("_http"):
            if not claims:
                raise PermissionError("Bearer authentication is required")
            if not {"*", *scopes}.intersection(set(claims.get("scp", []))):
                raise PermissionError("activity scope is required")
        return claims

    @staticmethod
    def _authorize_operator(
        p: dict[str, str], operator_id: str
    ) -> dict[str, Any]:
        """Allow the owning participant or an activity administrator to mutate activity."""
        if not p.get("_http"):
            return {}
        claims = ActivityHandler._claims(p)
        if not claims:
            raise PermissionError("Bearer authentication is required")
        scopes = set(claims.get("scp", []))
        if str(claims.get("sub")) != str(operator_id) and not {
            "*",
            "activity.admin",
        }.intersection(scopes):
            raise PermissionError(
                "the activity owner or activity.admin scope is required"
            )
        return claims

    @staticmethod
    def _in_memory_activation(activation_id: str) -> dict[str, Any]:
        try:
            return ActivityHandler.store.items[activation_id]
        except KeyError as exc:
            raise KeyError(activation_id) from exc

    @staticmethod
    def list_activations(_: JsonHandler, p: dict[str, str]) -> dict[str, Any]:
        ActivityHandler._authorize(
            p, {"activity.read", "activity.admin"}
        ) if p.get("_http") else None
        from urllib.parse import parse_qs, urlparse

        query = parse_qs(urlparse(p.get("_path", "")).query)
        programme = query.get("programme", [None])[0]
        operator_id = query.get("operatorId", [None])[0]
        if ActivityHandler.repository.durable:
            items = ActivityHandler.repository.list_activations(
                programme, operator_id
            )
        else:
            items = list(ActivityHandler.store.items.values())
            if programme:
                items = [
                    item
                    for item in items
                    if item.get("programmeSlug") == programme
                ]
            if operator_id:
                items = [
                    item
                    for item in items
                    if item.get("operatorId") == operator_id
                ]
        return page_result(items, query)

    @staticmethod
    def create_activation(_: JsonHandler, p: dict[str, str]) -> dict[str, Any]:
        body = validate_activation(p["_body"])
        require(body, "programmeSlug", "entityId", "operatorId", "startedAt")
        ActivityHandler._authorize_operator(p, body["operatorId"])
        if ActivityHandler.repository.durable:
            return {
                **ActivityHandler.repository.create_activation(
                    body, p.get("Idempotency-Key")
                ),
                "_status": 201,
            }

        def create() -> dict[str, Any]:
            activation = {
                "id": new_id(),
                "programmeSlug": body["programmeSlug"],
                "entityId": body["entityId"],
                "operatorId": body["operatorId"],
                "operatorCallsign": body.get("operatorCallsign"),
                "startedAt": body["startedAt"],
                "endedAt": None,
                "validityExpiresAt": None,
                "entityType": body.get("entityType"),
                "location": body.get("location"),
                "jurisdiction": body.get("jurisdiction"),
                "programmeRules": body.get("programmeRules", {}),
                "ruleEvaluation": {},
                "status": "OPEN",
                "qsos": [],
                "createdAt": now(),
                "updatedAt": now(),
            }
            validity = body.get(
                "validityDays",
                body.get("programmeRules", {}).get("activationValidityDays"),
            )
            if validity not in (None, "", "unlimited", "UNLIMITED"):
                activation["validityExpiresAt"] = (
                    (
                        datetime.fromisoformat(
                            body["startedAt"].replace("Z", "+00:00")
                        )
                        + timedelta(days=float(validity))
                    )
                    .isoformat()
                    .replace("+00:00", "Z")
                )
            ActivityHandler.store.items[activation["id"]] = activation
            ActivityHandler.store.event(
                "activity.activation.created.v1",
                "activation",
                activation["id"],
                activation,
            )
            return {**activation, "_status": 201}

        return ActivityHandler.store.once(p.get("Idempotency-Key"), create)

    @staticmethod
    def get_activation(_: JsonHandler, p: dict[str, str]) -> dict[str, Any]:
        ActivityHandler._authorize(
            p, {"activity.read", "activity.admin"}
        ) if p.get("_http") else None
        if ActivityHandler.repository.durable:
            return ActivityHandler.repository.get_activation(p["activationId"])
        return ActivityHandler._in_memory_activation(p["activationId"])

    @staticmethod
    def _record_qso(
        activation: dict[str, Any], body: dict[str, Any]
    ) -> dict[str, Any]:
        record = normalize_qso(
            {**body, "source": body.get("source", "manual")}
        )
        record["deduplicationKey"] = qso_deduplication_key(
            activation["id"], record
        )
        return record

    @staticmethod
    def add_qso(_: JsonHandler, p: dict[str, str]) -> dict[str, Any]:
        body = p["_body"]
        require(body, "workedCallsign", "timestamp")
        if ActivityHandler.repository.durable:
            activation = ActivityHandler.repository.get_activation(
                p["activationId"]
            )
            ActivityHandler._authorize_operator(p, activation["operatorId"])
            record = ActivityHandler._record_qso(activation, body)
            return {
                **ActivityHandler.repository.insert_qso(
                    p["activationId"], record, p.get("Idempotency-Key")
                ),
                "_status": 201,
            }
        activation = ActivityHandler._in_memory_activation(p["activationId"])
        ActivityHandler._authorize_operator(p, activation["operatorId"])
        if activation["status"] != "OPEN":
            raise ValueError("activation is not open")
        record = ActivityHandler._record_qso(activation, body)
        if any(
            item.get("deduplicationKey") == record["deduplicationKey"]
            for item in activation["qsos"]
        ):
            existing = next(
                item
                for item in activation["qsos"]
                if item.get("deduplicationKey") == record["deduplicationKey"]
            )
            return {
                "activationId": activation["id"],
                "qso": existing,
                "qsoCount": len(activation["qsos"]),
                "duplicate": True,
            }
        qso = {"id": new_id(), **record, "createdAt": now()}
        activation["qsos"].append(qso)
        activation["qsoCount"] = len(activation["qsos"])
        activation["uniqueCallsignCount"] = len(
            {item["workedCallsign"] for item in activation["qsos"]}
        )
        activation["uniqueEntityCount"] = len(
            {
                item.get("workedEntityId")
                for item in activation["qsos"]
                if item.get("workedEntityId")
            }
        )
        activation["updatedAt"] = now()
        ActivityHandler.store.event(
            "activity.qso.recorded.v1", "qso", qso["id"], qso
        )
        return {
            "activationId": activation["id"],
            "qso": qso,
            "qsoCount": len(activation["qsos"]),
            "duplicate": False,
            "_status": 201,
        }

    @staticmethod
    def add_qso_batch(_: JsonHandler, p: dict[str, str]) -> dict[str, Any]:
        body = p["_body"]
        records = body.get("qsos")
        if not isinstance(records, list) or not records:
            raise ValueError("qsos must be a non-empty array")
        if len(records) > int(os.environ.get("MYOTA_MAX_BATCH_QSOS", "5000")):
            raise ValueError("QSO batch exceeds the configured limit")
        if ActivityHandler.repository.durable:
            activation = ActivityHandler.repository.get_activation(
                p["activationId"]
            )
            ActivityHandler._authorize_operator(p, activation["operatorId"])
            normalized = [
                ActivityHandler._record_qso(activation, record)
                for record in records
            ]
            accepted = ActivityHandler.repository.insert_qso_batch(
                p["activationId"], normalized
            )
            return {
                "activationId": p["activationId"],
                "accepted": len(accepted),
                "duplicates": len(records) - len(accepted),
                "qsos": accepted,
                "_status": 201,
            }
        results = []
        for record in records:
            result = ActivityHandler.add_qso(
                None, {"activationId": p["activationId"], "_body": record}
            )
            results.append(result["qso"])
        return {
            "activationId": p["activationId"],
            "accepted": len(results),
            "qsos": results,
            "_status": 201,
        }

    @staticmethod
    def close_activation(_: JsonHandler, p: dict[str, str]) -> dict[str, Any]:
        activation = (
            ActivityHandler.repository.get_activation(p["activationId"])
            if ActivityHandler.repository.durable
            else ActivityHandler._in_memory_activation(p["activationId"])
        )
        ActivityHandler._authorize_operator(p, activation["operatorId"])
        if activation["status"] != "OPEN":
            raise ValueError("activation is not open")
        ended_at = p["_body"].get("endedAt", now())
        evaluated = evaluate_activation_rules(
            {**activation, "endedAt": ended_at}, activation.get("qsos", [])
        )
        if ActivityHandler.repository.durable:
            return ActivityHandler.repository.close_activation(
                p["activationId"], ended_at, evaluated
            )
        activation.update(
            {
                "status": "CLOSED" if evaluated["valid"] else "CLOSED_INVALID",
                "endedAt": ended_at,
                "ruleEvaluation": evaluated,
                "updatedAt": now(),
            }
        )
        ActivityHandler.store.event(
            "activity.activation.closed.v1",
            "activation",
            activation["id"],
            activation,
        )
        return activation

    @staticmethod
    def update_activation(_: JsonHandler, p: dict[str, str]) -> dict[str, Any]:
        """Preferred activation resource update; close is the first lifecycle transition."""
        body = dict(p.get("_body") or {})
        if body.get("status") != "CLOSED":
            raise ValueError(
                "the activation resource currently supports status=CLOSED only"
            )
        return ActivityHandler.close_activation(None, p)

    @staticmethod
    def _queue_job(
        kind: str, payload: dict[str, Any], idempotency_key: str | None = None
    ) -> dict[str, Any]:
        if ActivityHandler.repository.durable:
            job_id = ActivityHandler.repository.enqueue_job(
                kind, payload, idempotency_key
            )
            return {
                "id": job_id,
                "kind": kind,
                "payload": payload,
                "status": "QUEUED",
                "attempts": 0,
            }
        jobs = ActivityHandler.store.data.setdefault("jobs", [])
        if idempotency_key:
            existing = next(
                (
                    job
                    for job in jobs
                    if job.get("idempotencyKey") == idempotency_key
                ),
                None,
            )
            if existing:
                return existing
        job = {
            "id": new_id(),
            "kind": kind,
            "payload": payload,
            "status": "QUEUED",
            "attempts": 0,
            "idempotencyKey": idempotency_key,
            "createdAt": now(),
        }
        jobs.append(job)
        return job

    @staticmethod
    def get_job(_: JsonHandler, p: dict[str, str]) -> dict[str, Any]:
        if ActivityHandler.repository.durable:
            return ActivityHandler.repository.get_job(p["jobId"])
        job = next(
            (
                item
                for item in ActivityHandler.store.data.setdefault("jobs", [])
                if item.get("id") == p["jobId"]
            ),
            None,
        )
        if not job:
            raise KeyError(p["jobId"])
        return job

    @staticmethod
    def create_qso_ingestion(
        _: JsonHandler, p: dict[str, str]
    ) -> dict[str, Any]:
        body = p.get("_body") or {}
        require(body, "activationId", "qsos")
        activation = (
            ActivityHandler.repository.get_activation(body["activationId"])
            if ActivityHandler.repository.durable
            else ActivityHandler._in_memory_activation(body["activationId"])
        )
        ActivityHandler._authorize_operator(p, activation["operatorId"])
        records = body["qsos"]
        if not isinstance(records, list) or not records:
            raise ValueError("qsos must be a non-empty array")
        if len(records) > int(os.environ.get("MYOTA_MAX_BATCH_QSOS", "5000")):
            raise ValueError("QSO ingestion exceeds the configured limit")
        payload = {
            "activationId": body["activationId"],
            "records": records,
            "sourceFormat": body.get("sourceFormat", "JSON"),
        }
        job = ActivityHandler._queue_job(
            "QSO_INGESTION", payload, p.get("Idempotency-Key")
        )
        return {"ingestionId": job["id"], **job, "_status": 202}

    @staticmethod
    def review_correction_resource(
        _: JsonHandler, p: dict[str, str]
    ) -> dict[str, Any]:
        return ActivityHandler.review_correction(None, p)

    @staticmethod
    def create_adif_import(
        _: JsonHandler, p: dict[str, str]
    ) -> dict[str, Any]:
        body = p["_body"]
        require(body, "activationId", "filename", "contentBase64")
        activation = (
            ActivityHandler.repository.get_activation(body["activationId"])
            if ActivityHandler.repository.durable
            else ActivityHandler._in_memory_activation(body["activationId"])
        )
        ActivityHandler._authorize_operator(p, activation["operatorId"])
        try:
            content = base64.b64decode(body["contentBase64"], validate=True)
        except Exception as exc:
            raise ValueError("contentBase64 must be valid base64") from exc
        scan = ObjectStore.scan_content(content, body["filename"])
        object_key = f"adif/{body['activationId']}/{new_id()}-{body['filename'].replace('/', '_')}"
        bucket = os.environ.get("MYOTA_ADIF_BUCKET", "myota-adif")
        stored = ObjectStore().put(bucket, object_key, content, "text/plain")
        metadata = {
            "filename": body["filename"],
            "objectKey": object_key,
            "bucket": bucket,
            "sha256": stored["sha256"],
            "contentSize": stored["size"],
            "scan": scan,
        }
        if ActivityHandler.repository.durable:
            return {
                **ActivityHandler.repository.create_import(
                    body["activationId"], metadata
                ),
                "_status": 202,
            }
        import_id = new_id()
        record = {
            "id": import_id,
            "activationId": body["activationId"],
            **metadata,
            "status": "QUEUED",
            "malwareStatus": "CLEAN",
            "createdAt": now(),
        }
        ActivityHandler.store.data.setdefault("imports", {})[import_id] = (
            record
        )
        ActivityHandler.store.data.setdefault("jobs", []).append(
            {
                "id": new_id(),
                "kind": "ADIF_IMPORT",
                "payload": {"importId": import_id},
                "status": "QUEUED",
            }
        )
        return {**record, "_status": 202}

    @staticmethod
    def get_adif_import(_: JsonHandler, p: dict[str, str]) -> dict[str, Any]:
        if ActivityHandler.repository.durable:
            return ActivityHandler.repository.get_import(p["importId"])
        try:
            return ActivityHandler.store.data["imports"][p["importId"]]
        except KeyError as exc:
            raise KeyError(p["importId"]) from exc

    @staticmethod
    def request_correction(
        _: JsonHandler, p: dict[str, str]
    ) -> dict[str, Any]:
        body = p["_body"]
        require(body, "requestedBy", "reason", "proposedValues")
        if p.get("_http"):
            claims = ActivityHandler._claims(p)
            scopes = set(claims.get("scp", []))
            if str(claims.get("sub")) != str(body["requestedBy"]) and not {
                "*",
                "activity.admin",
            }.intersection(scopes):
                raise PermissionError(
                    "the correction requester or activity.admin scope is required"
                )
        if ActivityHandler.repository.durable:
            return ActivityHandler.repository.create_correction(
                p["qsoId"], body
            )
        correction = {
            "id": new_id(),
            "qsoId": p["qsoId"],
            **body,
            "status": "PENDING",
            "createdAt": now(),
        }
        ActivityHandler.store.data.setdefault("corrections", {})[
            correction["id"]
        ] = correction
        return {**correction, "_status": 201}

    @staticmethod
    def review_correction(_: JsonHandler, p: dict[str, str]) -> dict[str, Any]:
        body = p["_body"]
        require(body, "decision", "reviewedBy")
        ActivityHandler._authorize(p, {"activity.admin"}) if p.get(
            "_http"
        ) else None
        if body["decision"] not in {"APPLIED", "REJECTED"}:
            raise ValueError("decision must be APPLIED or REJECTED")
        if ActivityHandler.repository.durable:
            return ActivityHandler.repository.review_correction(
                p["correctionId"], body
            )
        correction = ActivityHandler.store.data.setdefault("corrections", {})[
            p["correctionId"]
        ]
        correction.update(
            {
                "status": body["decision"],
                "reviewedBy": body["reviewedBy"],
                "reviewNote": body.get("reviewNote"),
                "reviewedAt": now(),
            }
        )
        return correction

    @staticmethod
    def public_history(_: JsonHandler, p: dict[str, str]) -> dict[str, Any]:
        from urllib.parse import parse_qs, urlparse

        query = parse_qs(urlparse(p.get("_path", "")).query)
        programme = query.get("programme", [None])[0]
        items = (
            ActivityHandler.repository.public_history(programme)
            if ActivityHandler.repository.durable
            else list(ActivityHandler.store.items.values())
        )
        return page_result(
            [
                {
                    **item,
                    "operatorCallsign": mask_callsign(item["operatorCallsign"])
                    if item.get("operatorCallsign")
                    else None,
                }
                for item in items
            ],
            query,
        )

    @staticmethod
    def leaderboard(_: JsonHandler, p: dict[str, str]) -> dict[str, Any]:
        from urllib.parse import parse_qs, urlparse

        query = parse_qs(urlparse(p.get("_path", "")).query)
        programme = query.get("programme", [""])[0]
        category = query.get("category", ["HUNTER"])[0].upper()
        if not programme:
            raise ValueError("programme is required")
        if ActivityHandler.repository.durable:
            return {
                "programme": programme,
                "category": category,
                "items": ActivityHandler.repository.leaderboard(
                    programme, category
                ),
            }
        return {"programme": programme, "category": category, "items": []}

    @staticmethod
    def public_results(_: JsonHandler, p: dict[str, str]) -> dict[str, Any]:
        from urllib.parse import parse_qs, urlparse

        query = parse_qs(urlparse(p.get("_path", "")).query)
        programme = query.get("programme", [None])[0]
        items = (
            ActivityHandler.repository.public_history(programme)
            if ActivityHandler.repository.durable
            else list(ActivityHandler.store.items.values())
        )
        rows = [
            {
                "activationId": item["id"],
                "programme": item["programmeSlug"],
                "entityId": item["entityId"],
                "status": item["status"],
                "startedAt": item["startedAt"],
                "qsoCount": item.get("qsoCount", len(item.get("qsos", []))),
            }
            for item in items
        ]
        if query.get("format", ["json"])[0].lower() == "csv":
            header = (
                "activationId,programme,entityId,status,startedAt,qsoCount"
            )
            csv = "\n".join(
                [header]
                + [
                    ",".join(
                        str(row[field]).replace(",", " ")
                        for field in (
                            "activationId",
                            "programme",
                            "entityId",
                            "status",
                            "startedAt",
                            "qsoCount",
                        )
                    )
                    for row in rows
                ]
            )
            return {
                "format": "csv",
                "contentType": "text/csv",
                "content": csv,
                "downloadName": f"myota-results-{programme or 'all'}.csv",
            }
        return {
            "format": "json",
            "items": rows,
            "downloadName": f"myota-results-{programme or 'all'}.json",
        }

    @staticmethod
    def rebuild_statistics(
        _: JsonHandler, p: dict[str, str]
    ) -> dict[str, Any]:
        ActivityHandler._authorize(p, {"activity.admin"})
        programme = p.get("_body", {}).get("programmeSlug")
        job = ActivityHandler._queue_job(
            "STATISTICS_REBUILD",
            {"programmeSlug": programme},
            f"statistics:{programme or 'all'}:{datetime.now(timezone.utc).date()}",
        )
        return {"jobId": job["id"], **job, "_status": 202}

    @staticmethod
    def list_statistics(_: JsonHandler, p: dict[str, str]) -> dict[str, Any]:
        ActivityHandler._authorize(p, {"activity.read", "activity.admin"})
        from urllib.parse import parse_qs, urlparse

        query = parse_qs(urlparse(p.get("_path", "")).query)
        programme = query.get("programme", [None])[0]
        items = (
            ActivityHandler.repository.list_statistics(programme)
            if ActivityHandler.repository.durable
            else []
        )
        return page_result(items, query)

    @staticmethod
    def list_notifications(
        _: JsonHandler, p: dict[str, str]
    ) -> dict[str, Any]:
        claims = ActivityHandler._authorize(
            p, {"activity.read", "identity.me"}
        )
        from urllib.parse import parse_qs, urlparse

        query = parse_qs(urlparse(p.get("_path", "")).query)
        recipient = query.get("recipientId", [None])[0] or claims.get("sub")
        if not recipient:
            raise ValueError("recipientId is required")
        if claims.get("sub") not in (None, recipient):
            ActivityHandler._authorize(p, {"activity.admin"})
        items = (
            ActivityHandler.repository.list_notifications(recipient)
            if ActivityHandler.repository.durable
            else []
        )
        return {"items": items}

    @staticmethod
    def entity_deletion_impact(
        _: JsonHandler, p: dict[str, str]
    ) -> dict[str, Any]:
        ActivityHandler._authorize(p, {"activity.admin"})
        if ActivityHandler.repository.durable:
            return ActivityHandler.repository.entity_deletion_impact(
                p["entityId"]
            )
        qsos = [
            qso
            for activation in ActivityHandler.store.items.values()
            for qso in activation.get("qsos", [])
            if qso.get("workedEntityId") == p["entityId"]
            or activation.get("entityId") == p["entityId"]
        ]
        activations = [
            item
            for item in ActivityHandler.store.items.values()
            if item.get("entityId") == p["entityId"]
        ]
        return {
            "entityId": p["entityId"],
            "qsoCount": len(qsos),
            "activationCount": len(activations),
            "awardProgressCount": 0,
        }

    @staticmethod
    def cascade_delete_entity(
        _: JsonHandler, p: dict[str, str]
    ) -> dict[str, Any]:
        claims = ActivityHandler._authorize(p, {"activity.admin"})
        deleted_by = (
            claims.get("sub")
            or p.get("_body", {}).get("deletedBy")
            or "administrator"
        )
        if ActivityHandler.repository.durable:
            return ActivityHandler.repository.cascade_delete_entity(
                p["entityId"], deleted_by
            )
        qso_count = 0
        activation_count = 0
        for activation in ActivityHandler.store.items.values():
            if activation.get("entityId") == p["entityId"]:
                activation_count += 1
                activation["status"] = "CLOSED_INVALID"
            before = len(activation.get("qsos", []))
            activation["qsos"] = [
                qso
                for qso in activation.get("qsos", [])
                if qso.get("workedEntityId") != p["entityId"]
                and activation.get("entityId") != p["entityId"]
            ]
            qso_count += before - len(activation["qsos"])
        return {
            "entityId": p["entityId"],
            "deletedBy": deleted_by,
            "qsoCount": qso_count,
            "activationCount": activation_count,
            "awardRecalculationJobs": [],
        }

    @staticmethod
    def cascade_delete_entity_resource(
        _: JsonHandler, p: dict[str, str]
    ) -> dict[str, Any]:
        body = p.get("_body") or {}
        if not body.get("entityId"):
            raise ValueError("entityId is required")
        return ActivityHandler.cascade_delete_entity(
            None,
            {
                **p,
                "entityId": str(body["entityId"]),
                "_body": {"deletedBy": body.get("deletedBy")},
            },
        )


ActivityHandler.routes = {
    ("GET", "/v1/activations"): ActivityHandler.list_activations,
    ("POST", "/v1/activations"): ActivityHandler.create_activation,
    ("GET", "/v1/activations/{activationId}"): ActivityHandler.get_activation,
    (
        "PATCH",
        "/v1/activations/{activationId}",
    ): ActivityHandler.update_activation,
    ("POST", "/v1/activations/{activationId}/qsos"): ActivityHandler.add_qso,
    (
        "POST",
        "/v1/activations/{activationId}/qsos/batch",
    ): ActivityHandler.add_qso_batch,
    (
        "POST",
        "/v1/activations/{activationId}/close",
    ): ActivityHandler.close_activation,
    ("POST", "/v1/qso-ingestions"): ActivityHandler.create_qso_ingestion,
    ("GET", "/v1/qso-ingestions/{ingestionId}"): ActivityHandler.get_job,
    ("POST", "/v1/adif/imports"): ActivityHandler.create_adif_import,
    ("GET", "/v1/adif/imports/{importId}"): ActivityHandler.get_adif_import,
    (
        "POST",
        "/v1/qsos/{qsoId}/corrections",
    ): ActivityHandler.request_correction,
    (
        "POST",
        "/v1/qso-corrections/{correctionId}/review",
    ): ActivityHandler.review_correction,
    (
        "POST",
        "/v1/qsos/{qsoId}/corrections/{correctionId}/review",
    ): ActivityHandler.review_correction_resource,
    ("GET", "/v1/public/activations"): ActivityHandler.public_history,
    ("GET", "/v1/public/leaderboards"): ActivityHandler.leaderboard,
    ("GET", "/v1/public/results"): ActivityHandler.public_results,
    ("POST", "/v1/statistics/rebuild"): ActivityHandler.rebuild_statistics,
    (
        "POST",
        "/v1/statistics/rebuild-jobs",
    ): ActivityHandler.rebuild_statistics,
    ("GET", "/v1/statistics/rebuild-jobs/{jobId}"): ActivityHandler.get_job,
    ("GET", "/v1/statistics"): ActivityHandler.list_statistics,
    ("GET", "/v1/notifications"): ActivityHandler.list_notifications,
    (
        "GET",
        "/v1/activations/entity-deletion-impacts/{entityId}",
    ): ActivityHandler.entity_deletion_impact,
    (
        "POST",
        "/v1/activations/entity-deletion-cascades",
    ): ActivityHandler.cascade_delete_entity_resource,
    **AwardsHandler.routes,
}

ActivityHandler.deprecated_routes = {
    ("POST", "/v1/activations/{activationId}/close"),
    ("POST", "/v1/activations/{activationId}/qsos/batch"),
    ("POST", "/v1/adif/imports"),
    ("POST", "/v1/qso-corrections/{correctionId}/review"),
    ("POST", "/v1/statistics/rebuild"),
}

AwardsHandler.store = ActivityHandler.store
AwardsHandler.repository = ActivityHandler.repository


if __name__ == "__main__":
    BoundedThreadingHTTPServer(
        ("0.0.0.0", 8004), ActivityHandler
    ).serve_forever()
