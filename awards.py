"""Programme-owned award definitions, achievement evaluation, and issuance.

Award metadata and immutable issuance records live in PostgreSQL-backed service
state; binary backgrounds, signatures, and generated certificates are addressed
as objects in the configured S3-compatible store (MinIO locally).
"""
from __future__ import annotations

import math
import os
from http.server import ThreadingHTTPServer
from typing import Any

from common import JsonHandler, Store, new_id, now, page_result, require, verify_token

PAGE_SIZES_MM = {"A4": (210.0, 297.0), "LETTER": (215.9, 279.4)}
ASSET_KINDS = {"BACKGROUND", "SIGNATURE"}
CATEGORIES = {"HUNTER", "ACTIVATOR"}


def _number(value: Any, name: str) -> float:
    try:
        return float(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{name} must be numeric") from exc


def _operator(actual: float, operator: str, expected: float) -> bool:
    return {"GTE": actual >= expected, "GT": actual > expected, "EQ": actual == expected,
            "LTE": actual <= expected, "LT": actual < expected}.get(operator, False)


def evaluate_condition(condition: dict[str, Any], facts: dict[str, Any]) -> bool:
    """Evaluate a programme-owned AND/OR condition AST."""
    kind = str(condition.get("kind", "")).upper()
    if kind in {"AND", "ALL"}:
        children = condition.get("conditions", [])
        return bool(children) and all(evaluate_condition(child, facts) for child in children)
    if kind in {"OR", "ANY"}:
        children = condition.get("conditions", [])
        return bool(children) and any(evaluate_condition(child, facts) for child in children)
    if kind == "NOT":
        return not evaluate_condition(condition.get("condition", {}), facts)
    if kind in {"QSO_COUNT", "ACTIVATION_COUNT", "UNIQUE_CALLSIGNS", "UNIQUE_ENTITIES"}:
        field = {"QSO_COUNT": "qsoCount", "ACTIVATION_COUNT": "activationCount",
                 "UNIQUE_CALLSIGNS": "uniqueCallsignCount", "UNIQUE_ENTITIES": "uniqueEntityCount"}[kind]
        return _operator(_number(facts.get(field, 0), field), str(condition.get("operator", "GTE")).upper(),
                         _number(condition.get("value", 0), "condition.value"))
    if kind == "ENTITY_TYPE":
        return str(facts.get("entityType", "")) in {str(value) for value in condition.get("values", [])}
    if kind == "FIELD":
        field = str(condition.get("field", ""))
        return _operator(_number(facts.get(field, 0), field), str(condition.get("operator", "GTE")).upper(),
                         _number(condition.get("value", 0), "condition.value"))
    raise ValueError(f"unsupported award condition kind: {kind or 'missing'}")


def _print_spec(background: dict[str, Any], print_spec: dict[str, Any]) -> dict[str, Any]:
    page = str(print_spec.get("page", "A4")).upper()
    orientation = str(print_spec.get("orientation", "PORTRAIT")).upper()
    if page not in PAGE_SIZES_MM or orientation not in {"PORTRAIT", "LANDSCAPE"}:
        raise ValueError("print page must be A4 or LETTER and orientation must be PORTRAIT or LANDSCAPE")
    width_mm, height_mm = PAGE_SIZES_MM[page]
    if orientation == "LANDSCAPE":
        width_mm, height_mm = height_mm, width_mm
    dpi = _number(print_spec.get("dpi", 300), "print.dpi")
    if dpi < 150:
        raise ValueError("print.dpi must be at least 150 for a printable certificate")
    recommended = {"widthPx": math.ceil(width_mm / 25.4 * dpi), "heightPx": math.ceil(height_mm / 25.4 * dpi)}
    width_px, height_px = _number(background.get("widthPx", 0), "background.widthPx"), _number(background.get("heightPx", 0), "background.heightPx")
    if width_px <= 0 or height_px <= 0:
        raise ValueError("background image dimensions are required for print readiness")
    actual_ratio, expected_ratio = width_px / height_px, recommended["widthPx"] / recommended["heightPx"]
    ratio_delta = abs(actual_ratio - expected_ratio) / expected_ratio
    return {"page": page, "orientation": orientation, "dpi": dpi, "pageWidthMm": width_mm,
            "pageHeightMm": height_mm, "recommended": recommended, "actual": {"widthPx": width_px, "heightPx": height_px},
            "aspectRatioDelta": round(ratio_delta, 5), "printReady": ratio_delta <= 0.03 and width_px >= recommended["widthPx"] * 0.9}


def _validate_elements(elements: list[dict[str, Any]]) -> list[dict[str, Any]]:
    required = {"AWARD_NAME", "CALLSIGN", "PERSON_NAME", "DATE_OBTAINED", "MANAGER_NAME", "MANAGER_SIGNATURE"}
    seen: set[str] = set()
    for element in elements:
        kind = str(element.get("kind", ""))
        if kind not in required:
            raise ValueError(f"unsupported certificate element: {kind or 'missing'}")
        seen.add(kind)
        for field in ("x", "y", "width", "height"):
            value = _number(element.get(field), f"template.{kind}.{field}")
            if not 0 <= value <= 1:
                raise ValueError(f"template.{kind}.{field} must be between 0 and 1")
        if _number(element.get("width"), "template.width") <= 0 or _number(element.get("height"), "template.height") <= 0:
            raise ValueError(f"template.{kind} must have positive dimensions")
    missing = required - seen
    if missing:
        raise ValueError("certificate template is missing: " + ", ".join(sorted(missing)))
    return elements


class AwardsHandler(JsonHandler):
    service = "awards-service"
    store = Store("awards", "CORE_DATABASE_URL")

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
        return AwardsHandler.store.data.setdefault(name, {})

    @staticmethod
    def _award(award_id: str) -> dict[str, Any]:
        return AwardsHandler._bucket("definitions")[award_id]

    @staticmethod
    def list_awards(_: JsonHandler, p: dict[str, str]) -> dict[str, Any]:
        AwardsHandler._authorize(p, {"awards.read", "awards.admin"})
        from urllib.parse import parse_qs, urlparse
        query = parse_qs(urlparse(p.get("_path", "")).query)
        items = list(AwardsHandler._bucket("definitions").values())
        if query.get("programme"):
            items = [item for item in items if item.get("programmeSlug") == query["programme"][0]]
        if query.get("category"):
            items = [item for item in items if item.get("category") == query["category"][0].upper()]
        return page_result(items, query)

    @staticmethod
    def get_award(_: JsonHandler, p: dict[str, str]) -> dict[str, Any]:
        AwardsHandler._authorize(p, {"awards.read", "awards.admin"})
        return AwardsHandler._award(p["awardId"])

    @staticmethod
    def save_award(_: JsonHandler, p: dict[str, str]) -> dict[str, Any]:
        AwardsHandler._authorize(p, {"awards.admin"})
        body = p["_body"]
        require(body, "programmeSlug", "code", "name", "category", "condition", "levels", "backgroundAsset", "template")
        category = str(body["category"]).upper()
        if category not in CATEGORIES:
            raise ValueError("category must be HUNTER or ACTIVATOR")
        levels = body["levels"]
        if not isinstance(levels, list) or not levels or any(_number(level.get("threshold"), "level.threshold") <= 0 for level in levels):
            raise ValueError("levels must contain positive thresholds")
        background = dict(body["backgroundAsset"])
        if background.get("kind", "BACKGROUND") != "BACKGROUND":
            raise ValueError("backgroundAsset.kind must be BACKGROUND")
        template = dict(body["template"])
        _validate_elements(template.get("elements", []))
        record_id = body.get("awardId") or new_id()
        existing = AwardsHandler._bucket("definitions").get(record_id)
        if existing and existing.get("status") not in {"DRAFT", "CHANGES_REQUESTED"}:
            raise ValueError("only draft awards can be edited")
        record = {**(existing or {}), "id": record_id, "programmeSlug": body["programmeSlug"], "code": body["code"],
                  "name": body["name"], "description": body.get("description", ""), "category": category,
                  "achievementMetric": body.get("achievementMetric", "QSO_COUNT"), "condition": body["condition"],
                  "levels": levels, "backgroundAsset": background, "printSpec": body.get("printSpec", {"page": "A4", "orientation": "PORTRAIT", "dpi": 300}),
                  "template": template, "status": existing.get("status", "DRAFT") if existing else "DRAFT",
                  "updatedAt": now(), "createdAt": existing.get("createdAt", now()) if existing else now()}
        record["printReadiness"] = _print_spec(background, record["printSpec"])
        AwardsHandler._bucket("definitions")[record_id] = record
        AwardsHandler.store.event("awards.definition.saved.v1", "award", record_id, record)
        return {**record, "_status": 201 if not existing else 200}

    @staticmethod
    def submit_award(_: JsonHandler, p: dict[str, str]) -> dict[str, Any]:
        AwardsHandler._authorize(p, {"awards.admin"})
        award = AwardsHandler._award(p["awardId"])
        if award["status"] not in {"DRAFT", "CHANGES_REQUESTED"}:
            raise ValueError("only draft awards can be submitted")
        award["status"], award["submittedAt"] = "UNDER_REVIEW", now()
        return award

    @staticmethod
    def review_award(_: JsonHandler, p: dict[str, str]) -> dict[str, Any]:
        AwardsHandler._authorize(p, {"awards.admin"})
        award, body = AwardsHandler._award(p["awardId"]), p["_body"]
        require(body, "decision", "reviewerId")
        if award["status"] != "UNDER_REVIEW" or body["decision"] not in {"APPROVED", "CHANGES_REQUESTED"}:
            raise ValueError("award must be under review and decision must be APPROVED or CHANGES_REQUESTED")
        award["status"], award["review"] = body["decision"], {"reviewerId": body["reviewerId"], "note": body.get("note"), "reviewedAt": now()}
        return award

    @staticmethod
    def publish_award(_: JsonHandler, p: dict[str, str]) -> dict[str, Any]:
        AwardsHandler._authorize(p, {"awards.admin"})
        award, body = AwardsHandler._award(p["awardId"]), p["_body"]
        require(body, "effectiveFrom", "publisherId")
        if award["status"] != "APPROVED":
            raise ValueError("only approved awards can be published")
        if not award.get("printReadiness", {}).get("printReady"):
            raise ValueError("background image does not meet the selected A4/Letter print profile")
        award.update({"status": "PUBLISHED", "effectiveFrom": body["effectiveFrom"], "publisherId": body["publisherId"], "publishedAt": now()})
        AwardsHandler.store.event("awards.definition.published.v1", "award", award["id"], award)
        return award

    @staticmethod
    def register_asset(_: JsonHandler, p: dict[str, str]) -> dict[str, Any]:
        AwardsHandler._authorize(p, {"awards.admin"})
        body = p["_body"]
        require(body, "kind", "name", "objectKey", "mediaType", "widthPx", "heightPx")
        if body["kind"] not in ASSET_KINDS or not str(body["mediaType"]).startswith("image/"):
            raise ValueError("asset kind must be BACKGROUND or SIGNATURE and mediaType must be an image")
        asset = {"id": body.get("assetId") or new_id(), "kind": body["kind"], "name": body["name"],
                 "objectKey": body["objectKey"], "mediaType": body["mediaType"], "widthPx": int(body["widthPx"]),
                 "heightPx": int(body["heightPx"]), "sha256": body.get("sha256"), "storage": "MINIO",
                 "bucket": body.get("bucket") or os.environ.get("MYOTA_OBJECT_STORAGE_BUCKET", "myota-awards"),
                 "objectStorageEndpoint": os.environ.get("MYOTA_OBJECT_STORAGE_ENDPOINT", "http://minio:9000"),
                 "createdAt": now()}
        AwardsHandler._bucket("assets")[asset["id"]] = asset
        return {**asset, "_status": 201}

    @staticmethod
    def list_assets(_: JsonHandler, p: dict[str, str]) -> dict[str, Any]:
        AwardsHandler._authorize(p, {"awards.read", "awards.admin"})
        return page_result(list(AwardsHandler._bucket("assets").values()))

    @staticmethod
    def evaluate(_: JsonHandler, p: dict[str, str]) -> dict[str, Any]:
        AwardsHandler._authorize(p, {"awards.read", "awards.request", "awards.admin"})
        body = p["_body"]
        require(body, "awardId", "subjectId", "facts")
        award = AwardsHandler._award(body["awardId"])
        facts = dict(body["facts"])
        condition_met = evaluate_condition(award["condition"], facts)
        metric = str(award.get("achievementMetric", "QSO_COUNT"))
        metric_field = {"QSO_COUNT": "qsoCount", "UNIQUE_CALLSIGNS": "uniqueCallsignCount", "UNIQUE_ENTITIES": "uniqueEntityCount", "ACTIVATION_COUNT": "activationCount"}.get(metric, metric)
        progress = _number(facts.get(metric_field, 0), metric_field)
        levels = [{**level, "eligible": condition_met and progress >= _number(level["threshold"], "level.threshold")} for level in award["levels"]]
        return {"awardId": award["id"], "subjectId": body["subjectId"], "category": award["category"], "conditionMet": condition_met,
                "metric": metric, "progress": progress, "levels": levels}

    @staticmethod
    def request_award(_: JsonHandler, p: dict[str, str]) -> dict[str, Any]:
        AwardsHandler._authorize(p, {"awards.request", "awards.admin"})
        body = p["_body"]
        require(body, "awardId", "levelId", "subjectId", "callsign", "personName", "facts")
        award = AwardsHandler._award(body["awardId"])
        if award["status"] != "PUBLISHED":
            raise ValueError("only published awards can be requested")
        evaluation = AwardsHandler.evaluate(None, {"_body": {"awardId": award["id"], "subjectId": body["subjectId"], "facts": body["facts"]}})
        level = next((level for level in evaluation["levels"] if level.get("id") == body["levelId"]), None)
        if not level or not level["eligible"]:
            raise ValueError("the requested award level is not currently eligible")
        existing = next((item for item in AwardsHandler._bucket("requests").values()
                         if item["awardId"] == award["id"] and item["levelId"] == body["levelId"] and item["subjectId"] == body["subjectId"]
                         and item["status"] in {"REQUESTED", "ISSUED"}), None)
        if existing:
            return existing
        request = {"id": new_id(), "awardId": award["id"], "programmeSlug": award["programmeSlug"], "levelId": body["levelId"],
                   "subjectId": body["subjectId"], "category": award["category"], "callsign": body["callsign"], "personName": body["personName"],
                   "facts": body["facts"], "status": "REQUESTED", "requestedAt": now()}
        AwardsHandler._bucket("requests")[request["id"]] = request
        AwardsHandler.store.event("awards.request.created.v1", "award_request", request["id"], request)
        return {**request, "_status": 201}

    @staticmethod
    def list_requests(_: JsonHandler, p: dict[str, str]) -> dict[str, Any]:
        AwardsHandler._authorize(p, {"awards.read", "awards.admin"})
        from urllib.parse import parse_qs, urlparse
        query = parse_qs(urlparse(p.get("_path", "")).query)
        items = list(AwardsHandler._bucket("requests").values())
        if query.get("subjectId"):
            items = [item for item in items if item["subjectId"] == query["subjectId"][0]]
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
        signature = AwardsHandler._bucket("assets").get(body["signatureAssetId"])
        if not signature or signature["kind"] != "SIGNATURE":
            raise ValueError("a registered signature asset is required")
        issued_at = now()
        issuance = {"id": new_id(), "requestId": request["id"], "awardId": award["id"], "programmeSlug": award["programmeSlug"],
                    "levelId": request["levelId"], "category": request["category"], "callsign": request["callsign"],
                    "personName": request["personName"], "awardName": award["name"], "dateObtained": body.get("dateObtained", issued_at),
                    "managerName": body["managerName"], "signatureAssetId": signature["id"], "issuedAt": issued_at,
                    "artifact": {"storage": "MINIO", "bucket": os.environ.get("MYOTA_CERTIFICATE_BUCKET", "myota-certificates"),
                                  "objectKey": f"{award['programmeSlug']}/{request['subjectId']}/{award['code']}-{request['levelId']}-{request['id']}.pdf",
                                  "mediaType": "application/pdf", "downloadReady": False},
                    "renderSpec": {"backgroundAsset": award["backgroundAsset"], "printSpec": award["printSpec"],
                                   "elements": award["template"]["elements"], "signatureAsset": signature}}
        AwardsHandler._bucket("issuances")[issuance["id"]] = issuance
        request.update({"status": "ISSUED", "issuedAwardId": issuance["id"], "issuedAt": issued_at})
        AwardsHandler.store.event("awards.issued.v1", "award_issuance", issuance["id"], issuance)
        return {**issuance, "_status": 201}

    @staticmethod
    def list_issuances(_: JsonHandler, p: dict[str, str]) -> dict[str, Any]:
        AwardsHandler._authorize(p, {"awards.read", "awards.admin"})
        return page_result(list(AwardsHandler._bucket("issuances").values()))


AwardsHandler.routes = {
    ("GET", "/v1/awards"): AwardsHandler.list_awards,
    ("GET", "/v1/awards/{awardId}"): AwardsHandler.get_award,
    ("POST", "/v1/awards"): AwardsHandler.save_award,
    ("POST", "/v1/awards/{awardId}/submit"): AwardsHandler.submit_award,
    ("POST", "/v1/awards/{awardId}/review"): AwardsHandler.review_award,
    ("POST", "/v1/awards/{awardId}/publish"): AwardsHandler.publish_award,
    ("GET", "/v1/awards/assets"): AwardsHandler.list_assets,
    ("POST", "/v1/awards/assets"): AwardsHandler.register_asset,
    ("POST", "/v1/awards/evaluate"): AwardsHandler.evaluate,
    ("GET", "/v1/awards/requests"): AwardsHandler.list_requests,
    ("POST", "/v1/awards/requests"): AwardsHandler.request_award,
    ("POST", "/v1/awards/requests/{requestId}/issue"): AwardsHandler.issue_request,
    ("GET", "/v1/awards/issuances"): AwardsHandler.list_issuances,
}


if __name__ == "__main__":
    ThreadingHTTPServer(("0.0.0.0", 8004), AwardsHandler).serve_forever()
