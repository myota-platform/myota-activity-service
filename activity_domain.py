"""Pure activity-domain validation and normalization helpers.

Keeping these functions free of HTTP and database concerns makes them usable
by the API, ADIF worker, correction workflow, and deterministic aggregation
jobs alike.
"""
from __future__ import annotations

import hashlib
import math
import re
from datetime import datetime, timezone
from typing import Any

VALID_BANDS = {
    "2190M", "630M", "160M", "80M", "60M", "40M", "30M", "20M", "17M",
    "15M", "12M", "10M", "6M", "4M", "2M", "70CM", "23CM",
}
VALID_MODES = {
    "AM", "CW", "FM", "FT8", "FT4", "JS8", "RTTY", "SSB", "USB", "LSB",
    "DIGITAL", "PSK31", "SSTV", "DSTAR", "DMR", "C4FM",
}


def parse_timestamp(value: str | datetime) -> datetime:
    if isinstance(value, datetime):
        parsed = value
    else:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def iso_timestamp(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def normalize_callsign(value: str) -> str:
    callsign = " ".join(str(value).upper().strip().split())
    if not callsign or len(callsign) > 32 or not re.fullmatch(r"[A-Z0-9/ -]+", callsign):
        raise ValueError("callsign is invalid")
    return callsign


def normalize_band(value: str | None) -> str | None:
    if value in (None, ""):
        return None
    band = str(value).upper().replace(" ", "")
    if band not in VALID_BANDS:
        raise ValueError(f"unsupported band: {value}")
    return band


def normalize_mode(value: str | None) -> str | None:
    if value in (None, ""):
        return None
    mode = str(value).upper().replace("-", "")
    if mode not in VALID_MODES:
        raise ValueError(f"unsupported mode: {value}")
    return mode


def worked_station_key(callsign: str, locator: str | None = None) -> str:
    return f"{normalize_callsign(callsign)}|{str(locator or '').upper().strip()}"


def qso_deduplication_key(activation_id: str, record: dict[str, Any]) -> str:
    canonical = "|".join([
        str(activation_id), normalize_callsign(record["workedCallsign"]),
        parse_timestamp(record["timestamp"]).isoformat(),
        str(record.get("band") or "").upper(), str(record.get("mode") or "").upper(),
        str(record.get("hunterId") or ""), str(record.get("workedEntityId") or ""),
    ])
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def point_in_bounds(location: dict[str, Any] | None, bounds: dict[str, Any] | None) -> bool:
    if not bounds:
        return True
    if not location or location.get("latitude") is None or location.get("longitude") is None:
        return False
    return (float(bounds["minLatitude"]) <= float(location["latitude"]) <= float(bounds["maxLatitude"])
            and float(bounds["minLongitude"]) <= float(location["longitude"]) <= float(bounds["maxLongitude"]))


def validate_activation(data: dict[str, Any]) -> dict[str, Any]:
    rules = dict(data.get("programmeRules") or data.get("rules") or {})
    location = dict(data.get("location") or {})
    if rules.get("requireLocation") and (location.get("latitude") is None or location.get("longitude") is None):
        raise ValueError("programme rules require an activation location")
    if not point_in_bounds(location, rules.get("entityBounds")):
        raise ValueError("activation location is outside the entity bounds")
    callsign = data.get("operatorCallsign")
    authorized = {normalize_callsign(value) for value in data.get("authorizedCallsigns", [])}
    if callsign:
        callsign = normalize_callsign(callsign)
        if authorized and callsign not in authorized:
            raise ValueError("operator callsign is not authorized for this account")
        if data.get("callsignLifecycleStatus") == "RETIRED":
            raise ValueError("retired callsigns cannot start activations")
        if data.get("callsignLifecycleStatus") not in (None, "VERIFIED") and not rules.get("allowUnverifiedCallsign"):
            raise ValueError("a verified callsign is required by this programme")
    started_at = parse_timestamp(data["startedAt"])
    max_hours = rules.get("maxActivationHours")
    if max_hours is not None and float(max_hours) <= 0:
        raise ValueError("maxActivationHours must be positive")
    return {**data, "operatorCallsign": callsign, "programmeRules": rules,
            "location": location, "startedAt": iso_timestamp(started_at)}


def evaluate_activation_rules(activation: dict[str, Any], qsos: list[dict[str, Any]]) -> dict[str, Any]:
    rules = dict(activation.get("programmeRules") or {})
    started = parse_timestamp(activation["startedAt"])
    ended = parse_timestamp(activation["endedAt"]) if activation.get("endedAt") else None
    violations: list[str] = []
    if ended and ended < started:
        violations.append("endedAt must be after startedAt")
    if activation.get("validityExpiresAt") and ended and ended > parse_timestamp(activation["validityExpiresAt"]):
        violations.append("activation exceeded its validity window")
    if rules.get("minimumQsos") is not None and len(qsos) < int(rules["minimumQsos"]):
        violations.append("minimum QSO requirement was not met")
    allowed_bands = {str(v).upper() for v in rules.get("allowedBands", [])}
    allowed_modes = {str(v).upper().replace("-", "") for v in rules.get("allowedModes", [])}
    if allowed_bands:
        violations.extend(f"band {q.get('band')} is not allowed" for q in qsos if q.get("band") and q["band"] not in allowed_bands)
    if allowed_modes:
        violations.extend(f"mode {q.get('mode')} is not allowed" for q in qsos if q.get("mode") and q["mode"] not in allowed_modes)
    return {"valid": not violations, "violations": violations, "evaluatedAt": iso_timestamp(datetime.now(timezone.utc)),
            "qsoCount": len(qsos), "ruleVersion": rules.get("version")}


def normalize_qso(record: dict[str, Any]) -> dict[str, Any]:
    callsign = normalize_callsign(record["workedCallsign"])
    timestamp = parse_timestamp(record.get("timestamp") or record.get("timeOn"))
    band = normalize_band(record.get("band"))
    mode = normalize_mode(record.get("mode"))
    return {**record, "workedCallsign": callsign, "timestamp": iso_timestamp(timestamp),
            "band": band, "mode": mode,
            "workedStationKey": worked_station_key(callsign, record.get("workedLocator"))}


def parse_adif(text: str) -> list[dict[str, Any]]:
    """Parse the common ADIF fields without imposing programme policy."""
    records: list[dict[str, Any]] = []
    fields = re.findall(r"<([^:>]+)(?::(\d+))?(?:[:][^>]*)?>(.*?)(?=<[^>]*>|$)", text, flags=re.IGNORECASE | re.DOTALL)
    current: dict[str, Any] = {}
    for name, length, value in fields:
        key = name.upper().strip()
        raw = value[: int(length)] if length else value
        raw = raw.strip()
        if key == "EOR":
            if current:
                records.append(current)
            current = {}
            continue
        mapping = {"CALL": "workedCallsign", "QSO_DATE_TIME": "timestamp", "QSO_DATE": "date", "TIME_ON": "time",
                   "BAND": "band", "MODE": "mode", "RST_SENT": "rst", "RST_RCVD": "rstReceived",
                   "STATION_CALLSIGN": "operatorCallsign", "MY_SIG_INFO": "workedEntityId", "SIG_INFO": "workedEntityId",
                   "GRIDSQUARE": "workedLocator"}
        if key in mapping:
            current[mapping[key]] = raw
    if current:
        records.append(current)
    normalized: list[dict[str, Any]] = []
    for record in records:
        if not record.get("timestamp") and record.get("date") and record.get("time"):
            value = record["date"] + "T" + record["time"]
            if len(value) == 15:
                value = value[:8] + "T" + value[9:11] + ":" + value[11:13] + ":" + value[13:]
            record["timestamp"] = value.replace("/", "-") + "Z"
        if record.get("workedCallsign") and record.get("timestamp"):
            normalized.append(normalize_qso(record))
    return normalized


def mask_callsign(callsign: str) -> str:
    value = normalize_callsign(callsign)
    if len(value) <= 3:
        return "***"
    return value[:2] + "***" + value[-1:]


def haversine_km(first: dict[str, Any], second: dict[str, Any]) -> float:
    lat1, lon1 = math.radians(float(first["latitude"])), math.radians(float(first["longitude"]))
    lat2, lon2 = math.radians(float(second["latitude"])), math.radians(float(second["longitude"]))
    dlat, dlon = lat2 - lat1, lon2 - lon1
    value = math.sin(dlat / 2) ** 2 + math.cos(lat1) * math.cos(lat2) * math.sin(dlon / 2) ** 2
    return 6371.0088 * 2 * math.asin(math.sqrt(value))
