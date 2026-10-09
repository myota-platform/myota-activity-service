"""Programme-owned award definitions, achievement evaluation, and issuance.

Award metadata, progress, requests, and immutable issuance records live in
service-owned relational tables; binary backgrounds, signatures, and generated
certificates are addressed as objects in the configured S3-compatible store.
"""

from __future__ import annotations

import math
import os
import base64
import threading
from http.server import ThreadingHTTPServer
from typing import Any

from common import (
    JsonHandler,
    Store,
    new_id,
    now,
    page_result,
    require,
    verify_token,
)
from storage import ObjectStore, decode_base64

PAGE_SIZES_MM = {"A4": (210.0, 297.0), "LETTER": (215.9, 279.4)}
ASSET_KINDS = {"BACKGROUND", "SIGNATURE"}
CATEGORIES = {"HUNTER", "ACTIVATOR"}
PREVIEW_RENDERER = threading.BoundedSemaphore(1)


def _render_certificate(issuance: dict[str, Any]) -> dict[str, Any] | None:
    """Render an issued certificate when both registered image assets are available."""
    try:
        from certificate_design import render_pdf
    except ImportError:
        return None
    store = ObjectStore()
    spec = issuance["renderSpec"]
    background = spec["backgroundAsset"]
    signature = spec["signatureAsset"]
    background_bytes = store.get(background["bucket"], background["objectKey"])
    signature_bytes = store.get(signature["bucket"], signature["objectKey"])
    if not background_bytes or not signature_bytes:
        return None
    values = {
        "AWARD_NAME": issuance["awardName"],
        "CALLSIGN": issuance["callsign"],
        "PERSON_NAME": issuance["personName"],
        "DATE_OBTAINED": issuance["dateObtained"],
        "MANAGER_NAME": issuance["managerName"],
    }
    content = render_pdf(spec, values, background_bytes, signature_bytes)
    artifact = issuance["artifact"]
    stored = store.put(
        artifact["bucket"],
        artifact["objectKey"],
        content,
        "application/pdf",
    )
    download_url = store.presigned_get(
        artifact["bucket"], artifact["objectKey"]
    )
    return {
        "downloadReady": True,
        "contentSha256": stored["sha256"],
        "byteSize": stored["size"],
        "renderedAt": now(),
        "downloadUrl": download_url,
    }


def _number(value: Any, name: str) -> float:
    try:
        return float(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{name} must be numeric") from exc


def _operator(actual: float, operator: str, expected: float) -> bool:
    return {
        "GTE": actual >= expected,
        "GT": actual > expected,
        "EQ": actual == expected,
        "LTE": actual <= expected,
        "LT": actual < expected,
    }.get(operator, False)


def evaluate_condition(
    condition: dict[str, Any], facts: dict[str, Any]
) -> bool:
    """Evaluate a programme-owned AND/OR condition AST."""
    kind = str(condition.get("kind", "")).upper()
    if kind in {"AND", "ALL"}:
        children = condition.get("conditions", [])
        return bool(children) and all(
            evaluate_condition(child, facts) for child in children
        )
    if kind in {"OR", "ANY"}:
        children = condition.get("conditions", [])
        return bool(children) and any(
            evaluate_condition(child, facts) for child in children
        )
    if kind == "NOT":
        return not evaluate_condition(condition.get("condition", {}), facts)
    if kind in {
        "QSO_COUNT",
        "ACTIVATION_COUNT",
        "UNIQUE_CALLSIGNS",
        "UNIQUE_ENTITIES",
    }:
        field = {
            "QSO_COUNT": "qsoCount",
            "ACTIVATION_COUNT": "activationCount",
            "UNIQUE_CALLSIGNS": "uniqueCallsignCount",
            "UNIQUE_ENTITIES": "uniqueEntityCount",
        }[kind]
        return _operator(
            _number(facts.get(field, 0), field),
            str(condition.get("operator", "GTE")).upper(),
            _number(condition.get("value", 0), "condition.value"),
        )
    if kind == "ENTITY_TYPE":
        return str(facts.get("entityType", "")) in {
            str(value) for value in condition.get("values", [])
        }
    if kind == "FIELD":
        field = str(condition.get("field", ""))
        return _operator(
            _number(facts.get(field, 0), field),
            str(condition.get("operator", "GTE")).upper(),
            _number(condition.get("value", 0), "condition.value"),
        )
    raise ValueError(f"unsupported award condition kind: {kind or 'missing'}")


def _print_spec(
    background: dict[str, Any], print_spec: dict[str, Any]
) -> dict[str, Any]:
    page = str(print_spec.get("page", "A4")).upper()
    orientation = str(print_spec.get("orientation", "PORTRAIT")).upper()
    if page not in PAGE_SIZES_MM or orientation not in {
        "PORTRAIT",
        "LANDSCAPE",
    }:
        raise ValueError(
            "print page must be A4 or LETTER and orientation must be PORTRAIT or LANDSCAPE"
        )
    width_mm, height_mm = PAGE_SIZES_MM[page]
    if orientation == "LANDSCAPE":
        width_mm, height_mm = height_mm, width_mm
    dpi = _number(print_spec.get("dpi", 300), "print.dpi")
    if dpi < 150:
        raise ValueError(
            "print.dpi must be at least 150 for a printable certificate"
        )
    recommended = {
        "widthPx": math.ceil(width_mm / 25.4 * dpi),
        "heightPx": math.ceil(height_mm / 25.4 * dpi),
    }
    width_px, height_px = (
        _number(background.get("widthPx", 0), "background.widthPx"),
        _number(background.get("heightPx", 0), "background.heightPx"),
    )
    if width_px <= 0 or height_px <= 0:
        raise ValueError(
            "background image dimensions are required for print readiness"
        )
    actual_ratio, expected_ratio = (
        width_px / height_px,
        recommended["widthPx"] / recommended["heightPx"],
    )
    ratio_delta = abs(actual_ratio - expected_ratio) / expected_ratio
    return {
        "page": page,
        "orientation": orientation,
        "dpi": dpi,
        "pageWidthMm": width_mm,
        "pageHeightMm": height_mm,
        "recommended": recommended,
        "actual": {"widthPx": width_px, "heightPx": height_px},
        "aspectRatioDelta": round(ratio_delta, 5),
        "printReady": ratio_delta <= 0.03
        and width_px >= recommended["widthPx"] * 0.9,
    }


def _validate_elements(elements: list[dict[str, Any]]) -> list[dict[str, Any]]:
    if not isinstance(elements, list):
        raise ValueError("template elements must be an array")
    required = {
        "AWARD_NAME",
        "CALLSIGN",
        "PERSON_NAME",
        "DATE_OBTAINED",
        "MANAGER_NAME",
        "MANAGER_SIGNATURE",
    }
    seen: set[str] = set()
    for element in elements:
        if not isinstance(element, dict):
            raise ValueError("template elements must be objects")
        kind = str(element.get("kind", ""))
        if kind not in required | {"CUSTOM_TEXT"}:
            raise ValueError(
                f"unsupported certificate element: {kind or 'missing'}"
            )
        seen.add(kind)
        if kind == "CUSTOM_TEXT" and not str(element.get("label", "")).strip():
            raise ValueError("custom text requires a label")
        for field in ("x", "y", "width", "height"):
            value = _number(element.get(field), f"template.{kind}.{field}")
            if not math.isfinite(value) or not 0 <= value <= 1:
                raise ValueError(
                    f"template.{kind}.{field} must be between 0 and 1"
                )
        if (
            _number(element.get("width"), "template.width") <= 0
            or _number(element.get("height"), "template.height") <= 0
        ):
            raise ValueError(f"template.{kind} must have positive dimensions")
        if (
            float(element["x"]) + float(element["width"]) > 1.000001
            or float(element["y"]) + float(element["height"]) > 1.000001
        ):
            raise ValueError(f"template.{kind} must fit inside the page")
    missing = required - seen
    if missing:
        raise ValueError(
            "certificate template is missing: " + ", ".join(sorted(missing))
        )
    return elements


class AwardsHandler(JsonHandler):
    service = "awards-service"
    store = Store("awards", "ACTIVITY_DATABASE_URL")
    repository: Any = None

    @staticmethod
    def _authorize(p: dict[str, str], scopes: set[str]) -> None:
        if not p.get("_http"):
            return
        authorization = p.get("Authorization", "")
        if not authorization.startswith("Bearer "):
            raise PermissionError("Bearer authentication is required")
        claims = verify_token(authorization[7:])
        granted = set(claims.get("scp", []))
        if not {"*", *scopes}.intersection(granted):
            raise PermissionError("award scope is required")

    @staticmethod
    def _bucket(name: str) -> dict[str, Any]:
        if (
            AwardsHandler.repository is not None
            and AwardsHandler.repository.durable
        ):
            return {
                str(item["id"]): item
                for item in AwardsHandler.repository.list_collection(name)
            }
        return AwardsHandler.store.data.setdefault(name, {})

    @staticmethod
    def _award(award_id: str) -> dict[str, Any]:
        if (
            AwardsHandler.repository is not None
            and AwardsHandler.repository.durable
        ):
            return AwardsHandler.repository.get_collection_record(
                "definitions", award_id
            )
        return AwardsHandler._bucket("definitions")[award_id]

    @staticmethod
    def _save(collection: str, record: dict[str, Any]) -> dict[str, Any]:
        if (
            AwardsHandler.repository is not None
            and AwardsHandler.repository.durable
        ):
            return AwardsHandler.repository.save_collection(collection, record)
        AwardsHandler._bucket(collection)[record["id"]] = record
        return record

    @staticmethod
    def _claims(p: dict[str, str]) -> dict[str, Any]:
        if not p.get("_http"):
            return {}
        authorization = p.get("Authorization", "")
        if not authorization.startswith("Bearer "):
            return {}
        return verify_token(authorization[7:])

    @staticmethod
    def subject_facts(
        award: dict[str, Any], subject_id: str
    ) -> dict[str, Any]:
        if (
            AwardsHandler.repository is not None
            and AwardsHandler.repository.durable
        ):
            return AwardsHandler.repository.subject_facts(
                award["programmeSlug"],
                subject_id,
                award.get("category", "HUNTER"),
            )
        activations = [
            item
            for item in AwardsHandler.store.items.values()
            if item.get("programmeSlug") == award["programmeSlug"]
        ]
        if award.get("category") == "ACTIVATOR":
            activations = [
                item
                for item in activations
                if item.get("operatorId") == subject_id
            ]
            qsos = [
                qso
                for activation in activations
                for qso in activation.get("qsos", [])
            ]
        else:
            qsos = [
                qso
                for activation in activations
                for qso in activation.get("qsos", [])
                if qso.get("hunterId") == subject_id
            ]
        return {
            "qsoCount": len(qsos),
            "activationCount": len(activations),
            "uniqueCallsignCount": len(
                {
                    qso.get("workedCallsign")
                    for qso in qsos
                    if qso.get("workedCallsign")
                }
            ),
            "uniqueEntityCount": len(
                {
                    activation.get("entityId")
                    for activation in activations
                    if activation.get("entityId")
                }
            ),
            "entityType": next(
                (
                    activation.get("entityType")
                    for activation in activations
                    if activation.get("entityType")
                ),
                None,
            ),
        }

    @staticmethod
    def list_awards(_: JsonHandler, p: dict[str, str]) -> dict[str, Any]:
        claims = AwardsHandler._claims(p)
        if p.get("_http") and not claims:
            public = True
        else:
            AwardsHandler._authorize(p, {"awards.read", "awards.admin"})
            public = False
        from urllib.parse import parse_qs, urlparse

        query = parse_qs(urlparse(p.get("_path", "")).query)
        items = list(AwardsHandler._bucket("definitions").values())
        if public:
            items = [
                item for item in items if item.get("status") == "PUBLISHED"
            ]
        if query.get("programme"):
            items = [
                item
                for item in items
                if item.get("programmeSlug") == query["programme"][0]
            ]
        if query.get("category"):
            items = [
                item
                for item in items
                if item.get("category") == query["category"][0].upper()
            ]
        return page_result(items, query)

    @staticmethod
    def get_award(_: JsonHandler, p: dict[str, str]) -> dict[str, Any]:
        claims = AwardsHandler._claims(p)
        award = AwardsHandler._award(p["awardId"])
        if (
            p.get("_http")
            and not claims
            and award.get("status") != "PUBLISHED"
        ):
            raise PermissionError("only published awards are public")
        if claims:
            AwardsHandler._authorize(p, {"awards.read", "awards.admin"})
        return award

    @staticmethod
    def save_award(_: JsonHandler, p: dict[str, str]) -> dict[str, Any]:
        AwardsHandler._authorize(p, {"awards.admin"})
        body = p["_body"]
        require(
            body,
            "programmeSlug",
            "code",
            "name",
            "category",
            "condition",
            "levels",
            "backgroundAsset",
            "template",
        )
        category = str(body["category"]).upper()
        if category not in CATEGORIES:
            raise ValueError("category must be HUNTER or ACTIVATOR")
        levels = body["levels"]
        if (
            not isinstance(levels, list)
            or not levels
            or any(
                _number(level.get("threshold"), "level.threshold") <= 0
                for level in levels
            )
        ):
            raise ValueError("levels must contain positive thresholds")
        background = dict(body["backgroundAsset"])
        if background.get("kind", "BACKGROUND") != "BACKGROUND":
            raise ValueError("backgroundAsset.kind must be BACKGROUND")
        template = dict(body["template"])
        _validate_elements(template.get("elements", []))
        record_id = body.get("awardId") or new_id()
        existing = AwardsHandler._bucket("definitions").get(record_id)
        if existing and existing.get("status") not in {
            "DRAFT",
            "CHANGES_REQUESTED",
        }:
            raise ValueError("only draft awards can be edited")
        record = {
            **(existing or {}),
            "id": record_id,
            "programmeSlug": body["programmeSlug"],
            "code": body["code"],
            "name": body["name"],
            "description": body.get("description", ""),
            "category": category,
            "version": int(
                body.get("version", (existing or {}).get("version", 1))
            ),
            "achievementMetric": body.get("achievementMetric", "QSO_COUNT"),
            "condition": body["condition"],
            "levels": levels,
            "backgroundAsset": background,
            "printSpec": body.get(
                "printSpec",
                {"page": "A4", "orientation": "PORTRAIT", "dpi": 300},
            ),
            "template": template,
            "status": existing.get("status", "DRAFT") if existing else "DRAFT",
            "updatedAt": now(),
            "createdAt": existing.get("createdAt", now())
            if existing
            else now(),
        }
        record["printReadiness"] = _print_spec(background, record["printSpec"])
        for field in ("signatureAssetId", "managerName", "effectiveFrom"):
            if field in body:
                record[field] = body[field]
        if record.get("signatureAssetId"):
            signature = AwardsHandler._bucket("assets").get(
                record["signatureAssetId"]
            )
            if not signature or signature.get("kind") != "SIGNATURE":
                raise ValueError("choose a registered signature asset")
        AwardsHandler._save("definitions", record)
        AwardsHandler.store.event(
            "awards.definition.saved.v1", "award", record_id, record
        )
        return {**record, "_status": 201 if not existing else 200}

    @staticmethod
    def submit_award(_: JsonHandler, p: dict[str, str]) -> dict[str, Any]:
        AwardsHandler._authorize(p, {"awards.admin"})
        award = AwardsHandler._award(p["awardId"])
        if award["status"] not in {"DRAFT", "CHANGES_REQUESTED"}:
            raise ValueError("only draft awards can be submitted")
        award["status"], award["submittedAt"] = "UNDER_REVIEW", now()
        AwardsHandler._save("definitions", award)
        return award

    @staticmethod
    def review_award(_: JsonHandler, p: dict[str, str]) -> dict[str, Any]:
        AwardsHandler._authorize(p, {"awards.admin"})
        award, body = AwardsHandler._award(p["awardId"]), p["_body"]
        require(body, "decision", "reviewerId")
        if award["status"] != "UNDER_REVIEW" or body["decision"] not in {
            "APPROVED",
            "CHANGES_REQUESTED",
        }:
            raise ValueError(
                "award must be under review and decision must be APPROVED or CHANGES_REQUESTED"
            )
        award["status"], award["review"] = (
            body["decision"],
            {
                "reviewerId": body["reviewerId"],
                "note": body.get("note"),
                "reviewedAt": now(),
            },
        )
        AwardsHandler._save("definitions", award)
        return award

    @staticmethod
    def publish_award(_: JsonHandler, p: dict[str, str]) -> dict[str, Any]:
        AwardsHandler._authorize(p, {"awards.admin"})
        award, body = AwardsHandler._award(p["awardId"]), p["_body"]
        require(body, "effectiveFrom", "publisherId")
        if award["status"] != "APPROVED":
            raise ValueError("only approved awards can be published")
        if not award.get("printReadiness", {}).get("printReady"):
            raise ValueError(
                "background image does not meet the selected A4/Letter print profile"
            )
        award.update(
            {
                "status": "PUBLISHED",
                "effectiveFrom": body["effectiveFrom"],
                "publisherId": body["publisherId"],
                "publishedAt": now(),
            }
        )
        AwardsHandler._save("definitions", award)
        AwardsHandler.store.event(
            "awards.definition.published.v1", "award", award["id"], award
        )
        return award

    @staticmethod
    def retire_award(_: JsonHandler, p: dict[str, str]) -> dict[str, Any]:
        AwardsHandler._authorize(p, {"awards.admin"})
        award = AwardsHandler._award(p["awardId"])
        body = p.get("_body", {})
        require(body, "retiredAt", "retiredBy")
        if award.get("status") not in {"PUBLISHED", "APPROVED"}:
            raise ValueError(
                "only published or approved awards can be retired"
            )
        award.update(
            {
                "status": "RETIRED",
                "retiredAt": body["retiredAt"],
                "retiredBy": body["retiredBy"],
            }
        )
        AwardsHandler._save("definitions", award)
        return award

    @staticmethod
    def patch_award(_: JsonHandler, p: dict[str, str]) -> dict[str, Any]:
        """Preferred award resource update, delegating lifecycle transitions."""
        body = dict(p.get("_body") or {})
        status = body.pop("status", None)
        action = {**p, "_body": body}
        if status == "UNDER_REVIEW":
            return AwardsHandler.submit_award(None, action)
        if status in ("APPROVED", "CHANGES_REQUESTED"):
            return AwardsHandler.review_award(
                None, {**p, "_body": {**body, "decision": status}}
            )
        if status == "PUBLISHED":
            return AwardsHandler.publish_award(None, action)
        if status == "RETIRED":
            return AwardsHandler.retire_award(None, action)
        award = AwardsHandler._award(p["awardId"])
        return AwardsHandler.save_award(
            None, {**p, "_body": {**award, **body, "awardId": p["awardId"]}}
        )

    @staticmethod
    def _queue_job(
        kind: str, payload: dict[str, Any], idempotency_key: str | None = None
    ) -> dict[str, Any]:
        if (
            AwardsHandler.repository is not None
            and AwardsHandler.repository.durable
        ):
            job_id = AwardsHandler.repository.enqueue_job(
                kind, payload, idempotency_key
            )
            return {
                "id": job_id,
                "kind": kind,
                "payload": payload,
                "status": "QUEUED",
                "attempts": 0,
            }
        jobs = AwardsHandler.store.data.setdefault("jobs", [])
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
        if (
            AwardsHandler.repository is not None
            and AwardsHandler.repository.durable
        ):
            return AwardsHandler.repository.get_job(p["jobId"])
        job = next(
            (
                item
                for item in AwardsHandler.store.data.setdefault("jobs", [])
                if item.get("id") == p["jobId"]
            ),
            None,
        )
        if not job:
            raise KeyError(p["jobId"])
        return job

    @staticmethod
    def recalculate_award(_: JsonHandler, p: dict[str, str]) -> dict[str, Any]:
        AwardsHandler._authorize(p, {"awards.admin"})
        award = AwardsHandler._award(p["awardId"])
        body = p.get("_body", {})
        subjects = body.get("subjectIds") or []
        if not isinstance(subjects, list):
            raise ValueError("subjectIds must be an array")
        job = AwardsHandler._queue_job(
            "AWARD_RECALCULATE",
            {
                "programmeSlug": award["programmeSlug"],
                "subjectIds": subjects,
                "awardId": award["id"],
                "ruleVersion": award.get("version", 1),
            },
            f"award-recalculate:{award['id']}:{award.get('version', 1)}:{','.join(sorted(map(str, subjects)))}",
        )
        return {
            "awardId": award["id"],
            "ruleVersion": award.get("version", 1),
            "jobId": job["id"],
            **job,
            "_status": 202,
        }

    @staticmethod
    def create_evaluation_job(
        _: JsonHandler, p: dict[str, str]
    ) -> dict[str, Any]:
        body = p.get("_body") or {}
        require(body, "awardId", "subjectId")
        if p.get("_http"):
            claims = AwardsHandler._claims(p)
            if str(claims.get("sub")) != str(body["subjectId"]):
                AwardsHandler._authorize(
                    p, {"awards.read", "awards.request", "awards.admin"}
                )
            else:
                # Participant evaluations always use service-owned aggregates;
                # only trusted administrative callers may provide a snapshot.
                body = {**body, "facts": None}
        award = AwardsHandler._award(body["awardId"])
        payload = {
            "awardId": award["id"],
            "subjectId": body["subjectId"],
            "facts": body.get("facts"),
        }
        job = AwardsHandler._queue_job(
            "AWARD_EVALUATION", payload, p.get("Idempotency-Key")
        )
        return {"evaluationId": job["id"], **job, "_status": 202}

    @staticmethod
    def create_render_job(_: JsonHandler, p: dict[str, str]) -> dict[str, Any]:
        AwardsHandler._authorize(p, {"awards.admin"})
        issuance = AwardsHandler._bucket("issuances")[p["issuanceId"]]
        job = AwardsHandler._queue_job(
            "PDF_RENDER",
            {"issuanceId": issuance["id"]},
            p.get("Idempotency-Key"),
        )
        return {
            "renderJobId": job["id"],
            "issuanceId": issuance["id"],
            **job,
            "_status": 202,
        }

    @staticmethod
    def issue_request_resource(
        _: JsonHandler, p: dict[str, str]
    ) -> dict[str, Any]:
        return AwardsHandler.issue_request(None, p)

    @staticmethod
    def artifact(_: JsonHandler, p: dict[str, str]) -> dict[str, Any]:
        return AwardsHandler.download_issuance(
            None, {**p, "issuanceId": p["issuanceId"]}
        )

    @staticmethod
    def register_asset(_: JsonHandler, p: dict[str, str]) -> dict[str, Any]:
        AwardsHandler._authorize(p, {"awards.admin"})
        body = p["_body"]
        require(
            body,
            "kind",
            "name",
            "objectKey",
            "mediaType",
            "widthPx",
            "heightPx",
        )
        if body["kind"] not in ASSET_KINDS or not str(
            body["mediaType"]
        ).startswith("image/"):
            raise ValueError(
                "asset kind must be BACKGROUND or SIGNATURE and mediaType must be an image"
            )
        # Keep editable award artwork, manager signatures, and immutable issued
        # certificates in separate buckets so retention/access policies cannot
        # accidentally cross asset classes. The caller cannot override this.
        bucket_env = (
            "MYOTA_AWARD_ASSET_BUCKET"
            if body["kind"] == "BACKGROUND"
            else "MYOTA_AWARD_SIGNATURE_BUCKET"
        )
        default_bucket = (
            "myota-award-assets"
            if body["kind"] == "BACKGROUND"
            else "myota-award-signatures"
        )
        asset = {
            "id": body.get("assetId") or new_id(),
            "kind": body["kind"],
            "name": body["name"],
            "objectKey": body["objectKey"],
            "mediaType": body["mediaType"],
            "widthPx": int(body["widthPx"]),
            "heightPx": int(body["heightPx"]),
            "sha256": body.get("sha256"),
            "storage": "S3",
            "bucket": os.environ.get(bucket_env, default_bucket),
            "objectStorageEndpoint": os.environ.get(
                "MYOTA_OBJECT_STORAGE_PUBLIC_ENDPOINT",
                os.environ.get(
                    "MYOTA_OBJECT_STORAGE_ENDPOINT", "http://seaweedfs:8333"
                ),
            ),
            "contentStatus": "MISSING",
            "createdAt": now(),
        }
        AwardsHandler._save("assets", asset)
        return {**asset, "_status": 201}

    @staticmethod
    def asset_upload_url(_: JsonHandler, p: dict[str, str]) -> dict[str, Any]:
        AwardsHandler._authorize(p, {"awards.admin"})
        asset = AwardsHandler._bucket("assets")[p["assetId"]]
        url = ObjectStore().presigned_put(asset["bucket"], asset["objectKey"])
        if not url:
            raise ValueError(
                "object storage presigned uploads are unavailable; configure SeaweedFS or another S3-compatible store"
            )
        return {
            "assetId": asset["id"],
            "method": "PUT",
            "url": url,
            "expiresInSeconds": 900,
        }

    @staticmethod
    def asset_content(_: JsonHandler, p: dict[str, str]) -> dict[str, Any]:
        """Small/local upload path; larger clients should use asset_upload_url."""
        AwardsHandler._authorize(p, {"awards.admin"})
        asset = AwardsHandler._bucket("assets")[p["assetId"]]
        body = p["_body"]
        require(body, "contentBase64")
        content = decode_base64(body["contentBase64"])
        ObjectStore.scan_content(content, asset["objectKey"])
        stored = ObjectStore().put(
            asset["bucket"], asset["objectKey"], content, asset["mediaType"]
        )
        asset.update(
            {
                "contentStatus": "STORED",
                "contentSha256": stored["sha256"],
                "contentSize": stored["size"],
                "storedAt": now(),
            }
        )
        AwardsHandler._save("assets", asset)
        return asset

    @staticmethod
    def list_assets(_: JsonHandler, p: dict[str, str]) -> dict[str, Any]:
        AwardsHandler._authorize(p, {"awards.read", "awards.admin"})
        from urllib.parse import parse_qs, urlparse

        return page_result(
            list(AwardsHandler._bucket("assets").values()),
            parse_qs(urlparse(p.get("_path", "")).query),
        )

    @staticmethod
    def put_asset_content(_: JsonHandler, p: dict[str, str]) -> dict[str, Any]:
        from certificate_design import image_metadata

        AwardsHandler._authorize(p, {"awards.admin"})
        asset = AwardsHandler._bucket("assets")[p["assetId"]]
        content = p["_body"]["_imageBytes"]
        metadata = image_metadata(content)
        if metadata["mediaType"] != p["_body"].get("_mediaType"):
            raise ValueError(
                "declared media type does not match image content"
            )
        ObjectStore.scan_content(content, asset["objectKey"])
        stored = ObjectStore().put(
            asset["bucket"],
            asset["objectKey"],
            content,
            metadata["mediaType"],
        )
        asset.update(
            {
                **metadata,
                "contentStatus": "STORED",
                "contentSha256": stored["sha256"],
                "contentSize": stored["size"],
                "storedAt": now(),
            }
        )
        return AwardsHandler._save("assets", asset)

    @staticmethod
    def get_asset_content(_: JsonHandler, p: dict[str, str]) -> dict[str, Any]:
        from certificate_design import image_metadata

        AwardsHandler._authorize(p, {"awards.read", "awards.admin"})
        asset = AwardsHandler._bucket("assets")[p["assetId"]]
        content = ObjectStore().get(asset["bucket"], asset["objectKey"])
        if not content:
            raise ValueError("registered image has no uploaded content")
        metadata = image_metadata(content)
        return {
            "asset": asset,
            "mediaType": metadata["mediaType"],
            "contentBase64": base64.b64encode(content).decode(),
        }

    @staticmethod
    def preview(_: JsonHandler, p: dict[str, str]) -> dict[str, Any]:
        """Render a transient design, never an issuance or persisted object."""
        AwardsHandler._authorize(p, {"awards.admin"})
        if not PREVIEW_RENDERER.acquire(blocking=False):
            return {
                "_status": 429,
                "status": 429,
                "code": "preview_busy",
                "detail": "Preview renderer is busy; please retry shortly.",
            }
        try:
            return AwardsHandler._preview_body(p["_body"])
        finally:
            PREVIEW_RENDERER.release()

    @staticmethod
    def _preview_body(body: dict[str, Any]) -> dict[str, Any]:
        from certificate_design import render_pdf

        require(body, "name", "template", "printSpec")
        elements = _validate_elements(body["template"].get("elements", []))
        if len(elements) > 30:
            raise ValueError("a preview may contain at most 30 elements")
        background = body.get("backgroundAsset") or {}
        assets = AwardsHandler._bucket("assets")
        background_bytes = None
        if background.get("objectKey"):
            registered = next(
                (
                    asset
                    for asset in assets.values()
                    if asset["kind"] == "BACKGROUND"
                    and asset["objectKey"] == background["objectKey"]
                ),
                None,
            )
            if not registered:
                raise ValueError("choose a registered background")
            background_bytes = ObjectStore().get(
                registered["bucket"], registered["objectKey"]
            )
            if not background_bytes:
                raise ValueError("background content has not been uploaded")
        signature_bytes = None
        if body.get("signatureAssetId"):
            signature = assets.get(body["signatureAssetId"])
            if not signature or signature["kind"] != "SIGNATURE":
                raise ValueError("choose a registered signature")
            signature_bytes = ObjectStore().get(
                signature["bucket"], signature["objectKey"]
            )
            if not signature_bytes:
                raise ValueError("signature content has not been uploaded")
        mock_data = {
            "AWARD_NAME": str(body["name"])[:200],
            "CALLSIGN": "EA7TEST",
            "PERSON_NAME": "Demo Radio Operator",
            "DATE_OBTAINED": now()[:10],
            "MANAGER_NAME": str(
                body.get("managerName") or "Demo Award Manager"
            )[:200],
            "MANAGER_SIGNATURE": "Signature preview",
        }
        content = render_pdf(
            {"printSpec": body["printSpec"], "elements": elements},
            mock_data,
            background_bytes,
            signature_bytes,
            preview=True,
        )
        return {
            "preview": True,
            "mediaType": "application/pdf",
            "filename": "myota-award-preview.pdf",
            "mockData": mock_data,
            "contentBase64": base64.b64encode(content).decode(),
        }

    @staticmethod
    def evaluate(_: JsonHandler, p: dict[str, str]) -> dict[str, Any]:
        AwardsHandler._authorize(
            p, {"awards.read", "awards.request", "awards.admin"}
        )
        body = p["_body"]
        require(body, "awardId", "subjectId", "facts")
        award = AwardsHandler._award(body["awardId"])
        facts = dict(body["facts"])
        condition_met = evaluate_condition(award["condition"], facts)
        metric = str(award.get("achievementMetric", "QSO_COUNT"))
        metric_field = {
            "QSO_COUNT": "qsoCount",
            "UNIQUE_CALLSIGNS": "uniqueCallsignCount",
            "UNIQUE_ENTITIES": "uniqueEntityCount",
            "ACTIVATION_COUNT": "activationCount",
        }.get(metric, metric)
        progress = _number(facts.get(metric_field, 0), metric_field)
        levels = [
            {
                **level,
                "eligible": condition_met
                and progress >= _number(level["threshold"], "level.threshold"),
            }
            for level in award["levels"]
        ]
        return {
            "awardId": award["id"],
            "subjectId": body["subjectId"],
            "category": award["category"],
            "conditionMet": condition_met,
            "metric": metric,
            "progress": progress,
            "levels": levels,
        }

    @staticmethod
    def progress(_: JsonHandler, p: dict[str, str]) -> dict[str, Any]:
        claims = AwardsHandler._claims(p)
        body = p.get("_body", {})
        if not body and p.get("_path"):
            from urllib.parse import parse_qs, urlparse

            query = parse_qs(urlparse(p["_path"]).query)
            body = {
                "awardId": query.get("awardId", [None])[0],
                "subjectId": query.get(
                    "participantId", query.get("subjectId", [None])
                )[0],
            }
        award_id = body.get("awardId") or p.get("awardId")
        subject_id = body.get("subjectId") or p.get("subjectId")
        require(
            {"awardId": award_id, "subjectId": subject_id},
            "awardId",
            "subjectId",
        )
        if claims and claims.get("sub") != subject_id:
            AwardsHandler._authorize(p, {"awards.read", "awards.admin"})
        elif p.get("_http"):
            AwardsHandler._authorize(
                p,
                {
                    "awards.read",
                    "awards.request",
                    "awards.admin",
                    "identity.me",
                },
            )
        award = AwardsHandler._award(award_id)
        facts = AwardsHandler.subject_facts(award, subject_id)
        result = AwardsHandler.evaluate(
            None,
            {
                "_body": {
                    "awardId": award_id,
                    "subjectId": subject_id,
                    "facts": facts,
                }
            },
        )
        if (
            AwardsHandler.repository is not None
            and AwardsHandler.repository.durable
        ):
            AwardsHandler.repository.save_progress(
                award,
                subject_id,
                award.get("category", "HUNTER"),
                facts,
                result,
            )
        return result

    @staticmethod
    def request_award(_: JsonHandler, p: dict[str, str]) -> dict[str, Any]:
        body = p["_body"]
        require(
            body, "awardId", "levelId", "subjectId", "callsign", "personName"
        )
        claims = AwardsHandler._claims(p)
        if (
            claims
            and claims.get("sub") == body["subjectId"]
            and "awards.request" not in set(claims.get("scp", []))
        ):
            AwardsHandler._authorize(p, {"identity.me"})
        else:
            AwardsHandler._authorize(p, {"awards.request", "awards.admin"})
        award = AwardsHandler._award(body["awardId"])
        if award["status"] != "PUBLISHED":
            raise ValueError("only published awards can be requested")
        facts = (
            AwardsHandler.subject_facts(award, body["subjectId"])
            if claims
            else dict(body.get("facts") or {})
        )
        evaluation = AwardsHandler.evaluate(
            None,
            {
                "_body": {
                    "awardId": award["id"],
                    "subjectId": body["subjectId"],
                    "facts": facts,
                }
            },
        )
        level = next(
            (
                level
                for level in evaluation["levels"]
                if level.get("id") == body["levelId"]
            ),
            None,
        )
        if not level or not level["eligible"]:
            raise ValueError(
                "the requested award level is not currently eligible"
            )
        existing = next(
            (
                item
                for item in AwardsHandler._bucket("requests").values()
                if item["awardId"] == award["id"]
                and item["levelId"] == body["levelId"]
                and item["subjectId"] == body["subjectId"]
                and item["status"] in {"REQUESTED", "ISSUED"}
            ),
            None,
        )
        if existing:
            return existing
        request = {
            "id": new_id(),
            "awardId": award["id"],
            "programmeSlug": award["programmeSlug"],
            "levelId": body["levelId"],
            "subjectId": body["subjectId"],
            "category": award["category"],
            "callsign": body["callsign"],
            "personName": body["personName"],
            "facts": facts,
            "status": "REQUESTED",
            "requestedAt": now(),
        }
        AwardsHandler._save("requests", request)
        AwardsHandler.store.event(
            "awards.request.created.v1",
            "award_request",
            request["id"],
            request,
        )
        return {**request, "_status": 201}

    @staticmethod
    def list_requests(_: JsonHandler, p: dict[str, str]) -> dict[str, Any]:
        AwardsHandler._authorize(p, {"awards.read", "awards.admin"})
        from urllib.parse import parse_qs, urlparse

        query = parse_qs(urlparse(p.get("_path", "")).query)
        items = list(AwardsHandler._bucket("requests").values())
        if query.get("subjectId"):
            items = [
                item
                for item in items
                if item["subjectId"] == query["subjectId"][0]
            ]
        return page_result(items, query)

    @staticmethod
    def issue_request(_: JsonHandler, p: dict[str, str]) -> dict[str, Any]:
        AwardsHandler._authorize(p, {"awards.admin"})
        request = AwardsHandler._bucket("requests")[p["requestId"]]
        body = p["_body"]
        require(body, "managerName", "signatureAssetId")
        if request["status"] != "REQUESTED":
            raise ValueError("only requested awards can be issued")
        award = AwardsHandler._award(request["awardId"])
        signature = AwardsHandler._bucket("assets").get(
            body["signatureAssetId"]
        )
        if not signature or signature["kind"] != "SIGNATURE":
            raise ValueError("a registered signature asset is required")
        issued_at = now()
        issuance = {
            "id": new_id(),
            "requestId": request["id"],
            "awardId": award["id"],
            "programmeSlug": award["programmeSlug"],
            "levelId": request["levelId"],
            "category": request["category"],
            "subjectId": request["subjectId"],
            "callsign": request["callsign"],
            "personName": request["personName"],
            "awardName": award["name"],
            "dateObtained": body.get("dateObtained", issued_at),
            "managerName": body["managerName"],
            "signatureAssetId": signature["id"],
            "issuedAt": issued_at,
            "artifact": {
                "storage": "S3",
                "bucket": os.environ.get(
                    "MYOTA_CERTIFICATE_BUCKET", "myota-certificates"
                ),
                "objectKey": f"{award['programmeSlug']}/{request['subjectId']}/{award['code']}-{request['levelId']}-{request['id']}.pdf",
                "mediaType": "application/pdf",
                "downloadReady": False,
            },
            "renderSpec": {
                "backgroundAsset": award["backgroundAsset"],
                "printSpec": award["printSpec"],
                "elements": award["template"]["elements"],
                "signatureAsset": signature,
            },
        }
        rendered = _render_certificate(issuance)
        if rendered:
            issuance["artifact"].update(rendered)
        else:
            issuance["artifact"]["renderStatus"] = "WAITING_FOR_ASSETS"
        AwardsHandler._bucket("issuances")[issuance["id"]] = issuance
        request.update(
            {
                "status": "ISSUED",
                "issuedAwardId": issuance["id"],
                "issuedAt": issued_at,
            }
        )
        AwardsHandler._save("requests", request)
        AwardsHandler._save("issuances", issuance)
        AwardsHandler.store.event(
            "awards.issued.v1", "award_issuance", issuance["id"], issuance
        )
        return {**issuance, "_status": 201}

    @staticmethod
    def list_issuances(_: JsonHandler, p: dict[str, str]) -> dict[str, Any]:
        AwardsHandler._authorize(p, {"awards.read", "awards.admin"})
        return page_result(list(AwardsHandler._bucket("issuances").values()))

    @staticmethod
    def render_issuance(_: JsonHandler, p: dict[str, str]) -> dict[str, Any]:
        AwardsHandler._authorize(p, {"awards.admin"})
        issuance = AwardsHandler._bucket("issuances")[p["issuanceId"]]
        rendered = _render_certificate(issuance)
        if not rendered:
            raise ValueError(
                "certificate assets are not available or the PDF renderer is not installed"
            )
        issuance["artifact"].update(rendered)
        AwardsHandler._save("issuances", issuance)
        AwardsHandler.store.event(
            "awards.rendered.v1", "award_issuance", issuance["id"], issuance
        )
        return issuance

    @staticmethod
    def download_issuance(_: JsonHandler, p: dict[str, str]) -> dict[str, Any]:
        AwardsHandler._authorize(
            p, {"awards.read", "awards.request", "awards.admin"}
        )
        issuance = AwardsHandler._bucket("issuances")[p["issuanceId"]]
        if not issuance["artifact"].get("downloadReady"):
            raise ValueError("certificate is not ready for download")
        artifact = issuance["artifact"]
        url = ObjectStore().presigned_get(
            artifact["bucket"], artifact["objectKey"]
        )
        return {
            "issuanceId": issuance["id"],
            "downloadUrl": url,
            "objectKey": artifact["objectKey"],
            "expiresInSeconds": 900,
        }


AwardsHandler.routes = {
    ("GET", "/v1/awards"): AwardsHandler.list_awards,
    ("POST", "/v1/awards"): AwardsHandler.save_award,
    ("GET", "/v1/awards/assets"): AwardsHandler.list_assets,
    ("POST", "/v1/awards/assets"): AwardsHandler.register_asset,
    ("POST", "/v1/awards/previews"): AwardsHandler.preview,
    (
        "GET",
        "/v1/awards/assets/{assetId}/content",
    ): AwardsHandler.get_asset_content,
    (
        "PUT",
        "/v1/awards/assets/{assetId}/content",
    ): AwardsHandler.put_asset_content,
    ("GET", "/v1/awards/requests"): AwardsHandler.list_requests,
    ("GET", "/v1/awards/issuances"): AwardsHandler.list_issuances,
    ("GET", "/v1/awards/{awardId}"): AwardsHandler.get_award,
    ("PATCH", "/v1/awards/{awardId}"): AwardsHandler.patch_award,
    ("POST", "/v1/awards/{awardId}/submit"): AwardsHandler.submit_award,
    ("POST", "/v1/awards/{awardId}/review"): AwardsHandler.review_award,
    ("POST", "/v1/awards/{awardId}/publish"): AwardsHandler.publish_award,
    ("POST", "/v1/awards/{awardId}/retire"): AwardsHandler.retire_award,
    (
        "POST",
        "/v1/awards/{awardId}/recalculate",
    ): AwardsHandler.recalculate_award,
    (
        "POST",
        "/v1/awards/{awardId}/recalculation-jobs",
    ): AwardsHandler.recalculate_award,
    (
        "GET",
        "/v1/awards/{awardId}/recalculation-jobs/{jobId}",
    ): AwardsHandler.get_job,
    (
        "POST",
        "/v1/awards/assets/{assetId}/upload-url",
    ): AwardsHandler.asset_upload_url,
    (
        "POST",
        "/v1/awards/assets/{assetId}/content",
    ): AwardsHandler.asset_content,
    ("POST", "/v1/awards/evaluate"): AwardsHandler.evaluate,
    (
        "POST",
        "/v1/awards/evaluation-jobs",
    ): AwardsHandler.create_evaluation_job,
    ("GET", "/v1/awards/evaluation-jobs/{jobId}"): AwardsHandler.get_job,
    ("POST", "/v1/awards/progress"): AwardsHandler.progress,
    ("GET", "/v1/awards/progress"): AwardsHandler.progress,
    ("POST", "/v1/awards/requests"): AwardsHandler.request_award,
    (
        "POST",
        "/v1/awards/requests/{requestId}/issue",
    ): AwardsHandler.issue_request,
    (
        "POST",
        "/v1/awards/requests/{requestId}/issuances",
    ): AwardsHandler.issue_request_resource,
    (
        "POST",
        "/v1/awards/issuances/{issuanceId}/render",
    ): AwardsHandler.render_issuance,
    (
        "POST",
        "/v1/awards/issuances/{issuanceId}/render-jobs",
    ): AwardsHandler.create_render_job,
    (
        "GET",
        "/v1/awards/issuances/{issuanceId}/render-jobs/{jobId}",
    ): AwardsHandler.get_job,
    (
        "GET",
        "/v1/awards/issuances/{issuanceId}/download",
    ): AwardsHandler.download_issuance,
    (
        "GET",
        "/v1/awards/issuances/{issuanceId}/artifact",
    ): AwardsHandler.artifact,
}

AwardsHandler.deprecated_routes = {
    ("POST", "/v1/awards/{awardId}/submit"),
    ("POST", "/v1/awards/{awardId}/review"),
    ("POST", "/v1/awards/{awardId}/publish"),
    ("POST", "/v1/awards/{awardId}/retire"),
    ("POST", "/v1/awards/{awardId}/recalculate"),
    ("POST", "/v1/awards/issuances/{issuanceId}/render"),
    ("POST", "/v1/awards/requests/{requestId}/issue"),
    ("POST", "/v1/awards/evaluate"),
    ("POST", "/v1/awards/progress"),
}


if __name__ == "__main__":
    ThreadingHTTPServer(("0.0.0.0", 8004), AwardsHandler).serve_forever()
