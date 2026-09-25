"""Relational persistence owned by myota-activity-service.

The repository intentionally has no JSON state snapshot fallback in durable
mode.  The small in-memory path is retained only so contract/unit tests can
run without PostgreSQL.
"""
from __future__ import annotations

import json
import os
import time
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from typing import Any, Iterator

from activity_domain import iso_timestamp, parse_timestamp
from common import new_id, now, require_durable_database


class ActivityRepository:
    def __init__(self, dsn_env: str = "CORE_DATABASE_URL") -> None:
        self.dsn = os.environ.get(dsn_env, "")
        require_durable_database(dsn_env, self.dsn)
        self.pool: Any = None

    @property
    def durable(self) -> bool:
        return bool(self.dsn)

    def _ensure_pool(self) -> Any:
        if self.pool is not None:
            return self.pool
        try:
            from psycopg_pool import ConnectionPool
        except ImportError as exc:  # pragma: no cover
            raise RuntimeError("psycopg[binary,pool] is required for durable activity storage") from exc
        last: Exception | None = None
        max_size = max(1, int(os.environ.get("MYOTA_ACTIVITY_DB_POOL_MAX", os.environ.get("MYOTA_DB_POOL_MAX", "12"))))
        for attempt in range(1, 6):
            try:
                self.pool = ConnectionPool(self.dsn, min_size=1, max_size=max_size, open=True,
                                           kwargs={"connect_timeout": 5})
                return self.pool
            except Exception as exc:  # pragma: no cover
                last = exc
                time.sleep(min(2 ** (attempt - 1), 8))
        raise RuntimeError(f"unable to connect to activity PostgreSQL after retries: {last}")

    @contextmanager
    def transaction(self) -> Iterator[Any]:
        if not self.durable:
            yield None
            return
        from psycopg.rows import dict_row
        with self._ensure_pool().connection() as connection:
            connection.row_factory = dict_row
            try:
                yield connection
                connection.commit()
            except Exception:
                connection.rollback()
                raise

    @staticmethod
    def _json(value: Any) -> Any:
        from psycopg.types.json import Jsonb
        return Jsonb(value)

    @staticmethod
    def _dt(value: str | datetime | None) -> datetime | None:
        return parse_timestamp(value) if value else None

    @staticmethod
    def _iso(value: Any) -> Any:
        if isinstance(value, datetime):
            return iso_timestamp(value)
        return str(value) if value.__class__.__name__ == "UUID" else value

    @classmethod
    def _activation_record(cls, row: dict[str, Any], qsos: list[dict[str, Any]] | None = None) -> dict[str, Any]:
        record = {"id": cls._iso(row["id"]), "programmeSlug": row["programme_slug"], "entityId": row["entity_id"],
                  "entityType": row.get("entity_type"), "jurisdiction": row.get("jurisdiction_code"), "operatorId": row["operator_id"],
                  "operatorCallsign": row.get("operator_callsign"), "startedAt": cls._iso(row["started_at"]),
                  "endedAt": cls._iso(row.get("ended_at")), "validityExpiresAt": cls._iso(row.get("validity_expires_at")),
                  "status": row["status"], "location": row.get("location") or {},
                  "programmeRules": row.get("programme_rules") or {}, "ruleEvaluation": row.get("rule_evaluation") or {},
                  "qsoCount": row.get("qso_count", 0), "uniqueCallsignCount": row.get("unique_callsign_count", 0),
                  "uniqueEntityCount": row.get("unique_entity_count", 0), "createdAt": cls._iso(row.get("created_at")),
                  "updatedAt": cls._iso(row.get("updated_at"))}
        if qsos is not None:
            record["qsos"] = [cls._qso_record(item) for item in qsos]
        return record

    @classmethod
    def _qso_record(cls, row: dict[str, Any]) -> dict[str, Any]:
        return {"id": cls._iso(row["id"]), "activationId": cls._iso(row["activation_id"]),
                "programmeSlug": row["programme_slug"], "operatorId": row["operator_id"],
                "operatorCallsign": row.get("operator_callsign"), "hunterId": row.get("hunter_id"),
                "hunterCallsign": row.get("hunter_callsign"), "workedCallsign": row["worked_callsign"],
                "workedStationKey": row["worked_station_key"], "workedEntityId": row.get("worked_entity_id"),
                "timestamp": cls._iso(row["occurred_at"]), "band": row.get("band"), "mode": row.get("mode"),
                "rst": row.get("rst"), "source": row["source"], "status": row["status"],
                "deduplicationKey": row["deduplication_key"], "createdAt": cls._iso(row.get("created_at"))}

    def _event(self, connection: Any, event_type: str, aggregate_type: str, aggregate_id: str, payload: dict[str, Any]) -> None:
        connection.execute(
            "INSERT INTO outbox_event(event_id,event_type,producer,aggregate_type,aggregate_id,payload,occurred_at) "
            "VALUES (%s,%s,%s,%s,%s,%s,%s)",
            (new_id(), event_type, "activity-service", aggregate_type, str(aggregate_id), self._json(payload), self._dt(now())))

    def _job(self, connection: Any, kind: str, payload: dict[str, Any], idempotency_key: str | None = None) -> str:
        job_id = new_id()
        row = connection.execute(
            "INSERT INTO activity_job(id,kind,payload,idempotency_key) VALUES (%s,%s,%s,%s) "
            "ON CONFLICT (idempotency_key) DO UPDATE SET id=activity_job.id RETURNING id",
            (job_id, kind, self._json(payload), idempotency_key)).fetchone()
        return self._iso(row["id"] if isinstance(row, dict) else row[0])

    def _upsert_subject(self, connection: Any, programme: str, subject: str, category: str,
                        qso_delta: int = 0, activation_delta: int = 0, callsign: str | None = None,
                        entity_id: str | None = None, entity_type: str | None = None) -> None:
        connection.execute(
            "INSERT INTO activity_subject_aggregate(programme_slug,subject_id,category,qso_count,activation_count,last_entity_type) "
            "VALUES (%s,%s,%s,%s,%s,%s) ON CONFLICT (programme_slug,subject_id,category) DO UPDATE SET "
            "qso_count=activity_subject_aggregate.qso_count+EXCLUDED.qso_count, "
            "activation_count=activity_subject_aggregate.activation_count+EXCLUDED.activation_count, "
            "last_entity_type=COALESCE(EXCLUDED.last_entity_type,activity_subject_aggregate.last_entity_type),updated_at=now()",
            (programme, subject, category, qso_delta, activation_delta, entity_type))
        if callsign:
            connection.execute("INSERT INTO activity_subject_callsign(programme_slug,subject_id,category,callsign) VALUES (%s,%s,%s,%s) ON CONFLICT DO NOTHING",
                               (programme, subject, category, callsign))
        if entity_id:
            connection.execute("INSERT INTO activity_subject_entity(programme_slug,subject_id,category,entity_id) VALUES (%s,%s,%s,%s) ON CONFLICT DO NOTHING",
                               (programme, subject, category, entity_id))
        connection.execute(
            "UPDATE activity_subject_aggregate a SET unique_callsign_count=(SELECT count(*) FROM activity_subject_callsign s WHERE s.programme_slug=a.programme_slug AND s.subject_id=a.subject_id AND s.category=a.category), "
            "unique_entity_count=(SELECT count(*) FROM activity_subject_entity e WHERE e.programme_slug=a.programme_slug AND e.subject_id=a.subject_id AND e.category=a.category), updated_at=now() "
            "WHERE a.programme_slug=%s AND a.subject_id=%s AND a.category=%s", (programme, subject, category))

    def create_activation(self, data: dict[str, Any], idempotency_key: str | None = None) -> dict[str, Any]:
        with self.transaction() as connection:
            if idempotency_key:
                previous = connection.execute("SELECT response FROM idempotency_record WHERE service=%s AND key=%s",
                                              ("activity-service", idempotency_key)).fetchone()
                if previous:
                    return previous["response"]
            activation_id = new_id()
            started = self._dt(data["startedAt"])
            expires = None
            validity = data.get("validityDays", data.get("programmeRules", {}).get("activationValidityDays"))
            if validity not in (None, "", "unlimited", "UNLIMITED"):
                expires = started + timedelta(days=float(validity))
            record = {**data, "id": activation_id, "status": "OPEN", "validityExpiresAt": iso_timestamp(expires) if expires else None,
                      "qsoCount": 0, "uniqueCallsignCount": 0, "uniqueEntityCount": 0, "qsos": [], "createdAt": now(), "updatedAt": now()}
            row = connection.execute(
                "INSERT INTO activity_activation(id,programme_slug,entity_id,entity_type,jurisdiction_code,operator_id,operator_callsign,started_at,validity_expires_at,location,programme_rules) "
                "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) RETURNING *",
                (activation_id, data["programmeSlug"], data["entityId"], data.get("entityType"), data.get("jurisdiction"), data["operatorId"], data.get("operatorCallsign"),
                 started, expires, self._json(data.get("location") or {}), self._json(data.get("programmeRules") or {}))).fetchone()
            self._upsert_subject(connection, data["programmeSlug"], data["operatorId"], "ACTIVATOR", activation_delta=1,
                                 entity_id=data["entityId"], entity_type=data.get("entityType"))
            self._event(connection, "activity.activation.created.v1", "activation", activation_id, record)
            if idempotency_key:
                connection.execute("INSERT INTO idempotency_record(service,key,response) VALUES (%s,%s,%s) ON CONFLICT DO NOTHING",
                                   ("activity-service", idempotency_key, self._json(record)))
            return self._activation_record(row, [])

    def get_activation(self, activation_id: str) -> dict[str, Any]:
        with self.transaction() as connection:
            row = connection.execute("SELECT * FROM activity_activation WHERE id=%s", (activation_id,)).fetchone()
            if not row:
                raise KeyError(activation_id)
            qsos = connection.execute("SELECT * FROM activity_qso WHERE activation_id=%s AND status <> 'VOID' ORDER BY occurred_at", (activation_id,)).fetchall()
            return self._activation_record(row, qsos)

    def list_activations(self, programme: str | None = None, operator_id: str | None = None) -> list[dict[str, Any]]:
        with self.transaction() as connection:
            clauses, args = [], []
            if programme:
                clauses.append("programme_slug=%s"); args.append(programme)
            if operator_id:
                clauses.append("operator_id=%s"); args.append(operator_id)
            where = " WHERE " + " AND ".join(clauses) if clauses else ""
            rows = connection.execute(f"SELECT * FROM activity_activation{where} ORDER BY started_at DESC", args).fetchall()
            return [self._activation_record(row) for row in rows]

    def insert_qso(self, activation_id: str, record: dict[str, Any], idempotency_key: str | None = None) -> dict[str, Any]:
        with self.transaction() as connection:
            if idempotency_key:
                previous = connection.execute("SELECT response FROM idempotency_record WHERE service=%s AND key=%s",
                                              ("activity-service", idempotency_key)).fetchone()
                if previous:
                    return previous["response"]
            activation = connection.execute("SELECT * FROM activity_activation WHERE id=%s FOR UPDATE", (activation_id,)).fetchone()
            if not activation:
                raise KeyError(activation_id)
            if activation["status"] != "OPEN":
                raise ValueError("activation is not open")
            occurred = self._dt(record["timestamp"])
            if occurred < activation["started_at"] or (activation["validity_expires_at"] and occurred > activation["validity_expires_at"]):
                raise ValueError("QSO is outside the activation validity window")
            qso_id = new_id()
            row = connection.execute(
                "INSERT INTO activity_qso(id,activation_id,programme_slug,operator_id,operator_callsign,hunter_id,hunter_callsign,worked_callsign,worked_station_key,worked_entity_id,occurred_at,band,mode,rst,source,source_import_id,deduplication_key,raw_payload) "
                "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT (deduplication_key) DO NOTHING RETURNING *",
                (qso_id, activation_id, activation["programme_slug"], activation["operator_id"], activation.get("operator_callsign"),
                 record.get("hunterId"), record.get("hunterCallsign"), record["workedCallsign"], record["workedStationKey"], record.get("workedEntityId"),
                 occurred, record.get("band"), record.get("mode"), record.get("rst"), record.get("source", "manual"), record.get("sourceImportId"),
                 record["deduplicationKey"], self._json(record))).fetchone()
            duplicate = row is None
            if duplicate:
                row = connection.execute("SELECT * FROM activity_qso WHERE deduplication_key=%s", (record["deduplicationKey"],)).fetchone()
            if not duplicate:
                self._upsert_subject(connection, activation["programme_slug"], activation["operator_id"], "ACTIVATOR", qso_delta=1,
                                     callsign=record["workedCallsign"], entity_id=activation["entity_id"], entity_type=activation.get("entity_type"))
                if record.get("hunterId"):
                    self._upsert_subject(connection, activation["programme_slug"], record["hunterId"], "HUNTER", qso_delta=1,
                                         callsign=record["workedCallsign"], entity_id=record.get("workedEntityId"), entity_type=activation.get("entity_type"))
                connection.execute("UPDATE activity_activation SET qso_count=qso_count+1,unique_callsign_count=(SELECT count(DISTINCT worked_callsign) FROM activity_qso WHERE activation_id=%s AND status <> 'VOID'),unique_entity_count=(SELECT count(DISTINCT worked_entity_id) FROM activity_qso WHERE activation_id=%s AND worked_entity_id IS NOT NULL AND status <> 'VOID'),updated_at=now() WHERE id=%s",
                                   (activation_id, activation_id, activation_id))
                self._event(connection, "activity.qso.recorded.v1", "qso", row["id"], self._qso_record(row))
                self._job(connection, "AWARD_RECALCULATE", {"programmeSlug": activation["programme_slug"], "subjectIds": [activation["operator_id"], record.get("hunterId")]},
                          f"qso:{row['id']}")
            result = {"activationId": activation_id, "qso": self._qso_record(row), "qsoCount": int(activation["qso_count"]) + (0 if duplicate else 1), "duplicate": duplicate}
            if idempotency_key:
                connection.execute("INSERT INTO idempotency_record(service,key,response) VALUES (%s,%s,%s) ON CONFLICT DO NOTHING",
                                   ("activity-service", idempotency_key, self._json(result)))
            return result

    def insert_qso_batch(self, activation_id: str, records: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """High-volume ingestion path using PostgreSQL COPY into a staging table."""
        with self.transaction() as connection:
            activation = connection.execute("SELECT * FROM activity_activation WHERE id=%s FOR UPDATE", (activation_id,)).fetchone()
            if not activation:
                raise KeyError(activation_id)
            if activation["status"] != "OPEN":
                raise ValueError("activation is not open")
            for record in records:
                occurred = self._dt(record["timestamp"])
                if occurred < activation["started_at"] or (activation["validity_expires_at"] and occurred > activation["validity_expires_at"]):
                    raise ValueError("a QSO is outside the activation validity window")
            connection.execute("CREATE TEMP TABLE activity_qso_stage (id uuid,activation_id uuid,programme_slug text,operator_id text,operator_callsign text,hunter_id text,hunter_callsign text,worked_callsign text,worked_station_key text,worked_entity_id text,occurred_at timestamptz,band text,mode text,rst text,source text,source_import_id uuid,deduplication_key text,raw_payload jsonb) ON COMMIT DROP")
            with connection.cursor().copy("COPY activity_qso_stage FROM STDIN") as copy:
                for record in records:
                    copy.write_row((new_id(), activation_id, activation["programme_slug"], activation["operator_id"], activation.get("operator_callsign"), record.get("hunterId"), record.get("hunterCallsign"), record["workedCallsign"], record["workedStationKey"], record.get("workedEntityId"), self._dt(record["timestamp"]), record.get("band"), record.get("mode"), record.get("rst"), record.get("source", "manual"), record.get("sourceImportId"), record["deduplicationKey"], json.dumps(record)))
            rows = connection.execute("INSERT INTO activity_qso(id,activation_id,programme_slug,operator_id,operator_callsign,hunter_id,hunter_callsign,worked_callsign,worked_station_key,worked_entity_id,occurred_at,band,mode,rst,source,source_import_id,deduplication_key,raw_payload) SELECT id,activation_id,programme_slug,operator_id,operator_callsign,hunter_id,hunter_callsign,worked_callsign,worked_station_key,worked_entity_id,occurred_at,band,mode, rst,source,source_import_id,deduplication_key,raw_payload FROM activity_qso_stage ON CONFLICT (deduplication_key) DO NOTHING RETURNING *").fetchall()
            for row in rows:
                record = self._qso_record(row)
                self._upsert_subject(connection, activation["programme_slug"], activation["operator_id"], "ACTIVATOR", qso_delta=1, callsign=record["workedCallsign"], entity_id=activation["entity_id"], entity_type=activation.get("entity_type"))
                if record.get("hunterId"):
                    self._upsert_subject(connection, activation["programme_slug"], record["hunterId"], "HUNTER", qso_delta=1, callsign=record["workedCallsign"], entity_id=record.get("workedEntityId"), entity_type=activation.get("entity_type"))
                self._event(connection, "activity.qso.recorded.v1", "qso", row["id"], record)
            if rows:
                connection.execute("UPDATE activity_activation SET qso_count=qso_count+%s,unique_callsign_count=(SELECT count(DISTINCT worked_callsign) FROM activity_qso WHERE activation_id=%s AND status <> 'VOID'),unique_entity_count=(SELECT count(DISTINCT worked_entity_id) FROM activity_qso WHERE activation_id=%s AND worked_entity_id IS NOT NULL AND status <> 'VOID'),updated_at=now() WHERE id=%s", (len(rows), activation_id, activation_id, activation_id))
                self._job(connection, "AWARD_RECALCULATE", {"programmeSlug": activation["programme_slug"], "subjectIds": list({activation["operator_id"], *(record.get("hunterId") for record in records if record.get("hunterId"))})}, f"batch:{activation_id}:{records[0]['deduplicationKey']}")
            return [self._qso_record(row) for row in rows]

    def close_activation(self, activation_id: str, ended_at: str, evaluation: dict[str, Any]) -> dict[str, Any]:
        with self.transaction() as connection:
            row = connection.execute("SELECT * FROM activity_activation WHERE id=%s FOR UPDATE", (activation_id,)).fetchone()
            if not row:
                raise KeyError(activation_id)
            qsos = connection.execute("SELECT * FROM activity_qso WHERE activation_id=%s AND status <> 'VOID' ORDER BY occurred_at", (activation_id,)).fetchall()
            status = "CLOSED" if evaluation.get("valid") else "CLOSED_INVALID"
            updated = connection.execute("UPDATE activity_activation SET status=%s,ended_at=%s,rule_evaluation=%s,updated_at=now() WHERE id=%s RETURNING *",
                                         (status, self._dt(ended_at), self._json(evaluation), activation_id)).fetchone()
            self._event(connection, "activity.activation.closed.v1", "activation", activation_id, self._activation_record(updated, qsos))
            return self._activation_record(updated, qsos)

    def subject_facts(self, programme: str, subject_id: str, category: str) -> dict[str, Any]:
        with self.transaction() as connection:
            row = connection.execute("SELECT * FROM activity_subject_aggregate WHERE programme_slug=%s AND subject_id=%s AND category=%s",
                                     (programme, subject_id, category)).fetchone()
            if not row:
                return {"qsoCount": 0, "activationCount": 0, "uniqueCallsignCount": 0, "uniqueEntityCount": 0, "entityType": None}
            return {"qsoCount": int(row["qso_count"]), "activationCount": int(row["activation_count"]),
                    "uniqueCallsignCount": int(row["unique_callsign_count"]), "uniqueEntityCount": int(row["unique_entity_count"]),
                    "entityType": row.get("last_entity_type")}

    def save_collection(self, collection: str, record: dict[str, Any]) -> dict[str, Any]:
        """Persist award/asset/request/issuance records as service-owned rows."""
        with self.transaction() as connection:
            if collection == "definitions":
                connection.execute(
                    "INSERT INTO activity_award_definition(id,programme_slug,code,version,status,effective_from,retired_at,definition) VALUES (%s,%s,%s,%s,%s,%s,%s,%s) "
                    "ON CONFLICT (id) DO UPDATE SET programme_slug=EXCLUDED.programme_slug,code=EXCLUDED.code,version=EXCLUDED.version,status=EXCLUDED.status,effective_from=EXCLUDED.effective_from,retired_at=EXCLUDED.retired_at,definition=EXCLUDED.definition,updated_at=now()",
                    (record["id"], record["programmeSlug"], record["code"], int(record.get("version", 1)), record.get("status", "DRAFT"), self._dt(record.get("effectiveFrom")), self._dt(record.get("retiredAt")), self._json(record)))
            elif collection == "assets":
                connection.execute(
                    "INSERT INTO activity_asset(id,kind,object_key,bucket,media_type,width_px,height_px,content_status,metadata) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT (id) DO UPDATE SET content_status=EXCLUDED.content_status,metadata=EXCLUDED.metadata,updated_at=now()",
                    (record["id"], record["kind"], record["objectKey"], record["bucket"], record["mediaType"], int(record["widthPx"]), int(record["heightPx"]), record.get("contentStatus", "MISSING"), self._json(record)))
            elif collection == "requests":
                connection.execute(
                    "INSERT INTO activity_award_request(id,award_id,programme_slug,level_id,subject_id,category,callsign,person_name,facts,status,issued_award_id,requested_at,issued_at) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT (id) DO UPDATE SET status=EXCLUDED.status,issued_award_id=EXCLUDED.issued_award_id,issued_at=EXCLUDED.issued_at,facts=EXCLUDED.facts",
                    (record["id"], record["awardId"], record["programmeSlug"], record["levelId"], record["subjectId"], record["category"], record["callsign"], record["personName"], self._json(record.get("facts") or {}), record.get("status", "REQUESTED"), record.get("issuedAwardId"), self._dt(record.get("requestedAt")), self._dt(record.get("issuedAt"))))
            elif collection == "issuances":
                connection.execute(
                    "INSERT INTO activity_award_issuance(id,request_id,award_id,programme_slug,level_id,subject_id,issuance) VALUES (%s,%s,%s,%s,%s,%s,%s) ON CONFLICT (id) DO UPDATE SET issuance=EXCLUDED.issuance",
                    (record["id"], record["requestId"], record["awardId"], record["programmeSlug"], record["levelId"], record.get("subjectId", ""), self._json(record)))
            else:
                raise ValueError(f"unsupported durable collection: {collection}")
            return record

    def list_collection(self, collection: str) -> list[dict[str, Any]]:
        with self.transaction() as connection:
            if collection == "definitions":
                rows = connection.execute("SELECT definition FROM activity_award_definition ORDER BY programme_slug,code,version").fetchall()
            elif collection == "assets":
                rows = connection.execute("SELECT metadata FROM activity_asset ORDER BY created_at").fetchall()
            elif collection == "requests":
                rows = connection.execute("SELECT id,award_id,programme_slug,level_id,subject_id,category,callsign,person_name,facts,status,issued_award_id,requested_at,issued_at FROM activity_award_request ORDER BY requested_at DESC").fetchall()
                return [{"id": self._iso(row["id"]), "awardId": self._iso(row["award_id"]), "programmeSlug": row["programme_slug"], "levelId": row["level_id"], "subjectId": row["subject_id"], "category": row["category"], "callsign": row["callsign"], "personName": row["person_name"], "facts": row["facts"], "status": row["status"], "issuedAwardId": self._iso(row.get("issued_award_id")) if row.get("issued_award_id") else None, "requestedAt": self._iso(row["requested_at"]), "issuedAt": self._iso(row.get("issued_at"))} for row in rows]
            elif collection == "issuances":
                rows = connection.execute("SELECT issuance FROM activity_award_issuance ORDER BY created_at DESC").fetchall()
            else:
                raise ValueError(f"unsupported durable collection: {collection}")
            return [row["definition" if collection == "definitions" else "metadata" if collection == "assets" else "issuance"] for row in rows]

    def get_collection_record(self, collection: str, record_id: str) -> dict[str, Any]:
        with self.transaction() as connection:
            column = {"definitions": "definition", "assets": "metadata", "requests": None, "issuances": "issuance"}.get(collection)
            if collection == "requests":
                row = connection.execute("SELECT * FROM activity_award_request WHERE id=%s", (record_id,)).fetchone()
                if not row:
                    raise KeyError(record_id)
                return {"id": self._iso(row["id"]), "awardId": self._iso(row["award_id"]), "programmeSlug": row["programme_slug"], "levelId": row["level_id"], "subjectId": row["subject_id"], "category": row["category"], "callsign": row["callsign"], "personName": row["person_name"], "facts": row["facts"], "status": row["status"], "issuedAwardId": self._iso(row.get("issued_award_id")) if row.get("issued_award_id") else None, "requestedAt": self._iso(row["requested_at"]), "issuedAt": self._iso(row.get("issued_at"))}
            table = {"definitions": "activity_award_definition", "assets": "activity_asset", "issuances": "activity_award_issuance"}[collection]
            row = connection.execute(f"SELECT {column} FROM {table} WHERE id=%s", (record_id,)).fetchone()
            if not row:
                raise KeyError(record_id)
            return row[column]

    def create_import(self, activation_id: str, metadata: dict[str, Any]) -> dict[str, Any]:
        with self.transaction() as connection:
            import_id = new_id()
            record = {"id": import_id, "activationId": activation_id, **metadata, "status": "QUEUED", "malwareStatus": "CLEAN", "recordsSeen": 0, "recordsAccepted": 0, "recordsRejected": 0, "errors": [], "createdAt": now()}
            connection.execute("INSERT INTO activity_import(id,activation_id,filename,object_key,bucket,sha256,content_size,malware_status) VALUES (%s,%s,%s,%s,%s,%s,%s,%s)",
                               (import_id, activation_id, metadata["filename"], metadata["objectKey"], metadata["bucket"], metadata["sha256"], metadata["contentSize"], "CLEAN"))
            self._job(connection, "ADIF_IMPORT", {"importId": import_id}, f"adif:{import_id}")
            self._event(connection, "activity.adif.queued.v1", "adif_import", import_id, record)
            return record

    def get_import(self, import_id: str) -> dict[str, Any]:
        with self.transaction() as connection:
            row = connection.execute("SELECT * FROM activity_import WHERE id=%s", (import_id,)).fetchone()
            if not row:
                raise KeyError(import_id)
            return {"id": self._iso(row["id"]), "activationId": self._iso(row["activation_id"]), "filename": row["filename"], "objectKey": row["object_key"], "bucket": row["bucket"], "sha256": row["sha256"], "contentSize": row["content_size"], "malwareStatus": row["malware_status"], "status": row["status"], "recordsSeen": row["records_seen"], "recordsAccepted": row["records_accepted"], "recordsRejected": row["records_rejected"], "errors": row["errors"], "createdAt": self._iso(row["created_at"]), "startedAt": self._iso(row.get("started_at")), "completedAt": self._iso(row.get("completed_at"))}

    def start_import(self, import_id: str) -> dict[str, Any]:
        with self.transaction() as connection:
            connection.execute("UPDATE activity_import SET status='PROCESSING',started_at=now() WHERE id=%s AND status='QUEUED'", (import_id,))
        return self.get_import(import_id)

    def finish_import(self, import_id: str, seen: int, accepted: int, rejected: int, errors: list[str], status: str = "COMPLETED") -> dict[str, Any]:
        with self.transaction() as connection:
            connection.execute("UPDATE activity_import SET status=%s,records_seen=%s,records_accepted=%s,records_rejected=%s,errors=%s,completed_at=now() WHERE id=%s",
                               (status, seen, accepted, rejected, self._json(errors), import_id))
        return self.get_import(import_id)

    def claim_job(self) -> dict[str, Any] | None:
        with self.transaction() as connection:
            row = connection.execute("WITH next_job AS (SELECT id FROM activity_job WHERE status='QUEUED' AND available_at <= now() ORDER BY available_at,id FOR UPDATE SKIP LOCKED LIMIT 1) UPDATE activity_job j SET status='RUNNING',attempts=j.attempts+1,started_at=now() FROM next_job n WHERE j.id=n.id RETURNING j.*").fetchone()
            if not row:
                return None
            return {"id": self._iso(row["id"]), "kind": row["kind"], "payload": row["payload"], "attempts": row["attempts"]}

    def complete_job(self, job_id: str) -> None:
        with self.transaction() as connection:
            connection.execute("UPDATE activity_job SET status='SUCCEEDED',completed_at=now(),last_error=NULL WHERE id=%s", (job_id,))

    def fail_job(self, job: dict[str, Any], error: Exception) -> None:
        with self.transaction() as connection:
            if int(job["attempts"]) >= int(os.environ.get("MYOTA_ACTIVITY_JOB_MAX_ATTEMPTS", "8")):
                connection.execute("UPDATE activity_job SET status='FAILED',completed_at=now(),last_error=%s WHERE id=%s", (str(error), job["id"]))
            else:
                delay = min(300, 2 ** min(int(job["attempts"]), 8))
                connection.execute("UPDATE activity_job SET status='QUEUED',available_at=now()+make_interval(secs => %s),last_error=%s WHERE id=%s", (delay, str(error), job["id"]))

    def save_progress(self, award: dict[str, Any], subject_id: str, category: str, facts: dict[str, Any], evaluation: dict[str, Any]) -> None:
        with self.transaction() as connection:
            connection.execute("INSERT INTO activity_award_progress(award_id,award_version,subject_id,category,facts,evaluation) VALUES (%s,%s,%s,%s,%s,%s) ON CONFLICT (award_id,award_version,subject_id) DO UPDATE SET facts=EXCLUDED.facts,evaluation=EXCLUDED.evaluation,computed_at=now()",
                               (award["id"], int(award.get("version", 1)), subject_id, category, self._json(facts), self._json(evaluation)))

    def enqueue_job(self, kind: str, payload: dict[str, Any], idempotency_key: str | None = None) -> str:
        with self.transaction() as connection:
            return self._job(connection, kind, payload, idempotency_key)

    def create_correction(self, qso_id: str, body: dict[str, Any]) -> dict[str, Any]:
        with self.transaction() as connection:
            correction_id = new_id()
            row = connection.execute("INSERT INTO activity_qso_correction(id,qso_id,requested_by,reason,proposed_values) VALUES (%s,%s,%s,%s,%s) RETURNING *",
                                     (correction_id, qso_id, body["requestedBy"], body["reason"], self._json(body["proposedValues"]))).fetchone()
            return {"id": self._iso(row["id"]), "qsoId": self._iso(row["qso_id"]), "requestedBy": row["requested_by"], "reason": row["reason"], "proposedValues": row["proposed_values"], "status": row["status"], "createdAt": self._iso(row["created_at"]), "_status": 201}

    def review_correction(self, correction_id: str, body: dict[str, Any]) -> dict[str, Any]:
        with self.transaction() as connection:
            correction = connection.execute("SELECT * FROM activity_qso_correction WHERE id=%s FOR UPDATE", (correction_id,)).fetchone()
            if not correction:
                raise KeyError(correction_id)
            if correction["status"] != "PENDING":
                raise ValueError("correction is no longer pending")
            if body["decision"] == "APPLIED":
                values = correction["proposed_values"]
                allowed = {"workedCallsign", "workedStationKey", "workedEntityId", "occurredAt", "band", "mode", "rst", "hunterId", "hunterCallsign"}
                updates = {key: value for key, value in values.items() if key in allowed}
                assignments, args = ["status='CORRECTED'", "corrected_at=now()"], []
                columns = {"workedCallsign": "worked_callsign", "workedStationKey": "worked_station_key", "workedEntityId": "worked_entity_id", "occurredAt": "occurred_at", "band": "band", "mode": "mode", "rst": "rst", "hunterId": "hunter_id", "hunterCallsign": "hunter_callsign"}
                for key, value in updates.items():
                    assignments.append(f"{columns[key]}=%s")
                    args.append(self._dt(value) if key == "occurredAt" else value)
                args.append(str(correction["qso_id"]))
                connection.execute(f"UPDATE activity_qso SET {','.join(assignments)} WHERE id=%s", args)
            row = connection.execute("UPDATE activity_qso_correction SET status=%s,reviewed_by=%s,review_note=%s,reviewed_at=now() WHERE id=%s RETURNING *",
                                     (body["decision"], body["reviewedBy"], body.get("reviewNote"), correction_id)).fetchone()
            return {"id": self._iso(row["id"]), "qsoId": self._iso(row["qso_id"]), "requestedBy": row["requested_by"], "reason": row["reason"], "proposedValues": row["proposed_values"], "status": row["status"], "reviewedBy": row.get("reviewed_by"), "reviewNote": row.get("review_note"), "reviewedAt": self._iso(row.get("reviewed_at"))}

    def create_notification(self, recipient_id: str, notification_type: str, payload: dict[str, Any], deduplication_key: str | None = None) -> dict[str, Any]:
        with self.transaction() as connection:
            notification_id = new_id()
            row = connection.execute("INSERT INTO activity_notification(id,recipient_id,notification_type,payload,deduplication_key) VALUES (%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING RETURNING *",
                                     (notification_id, recipient_id, notification_type, self._json(payload), deduplication_key)).fetchone()
            if row is None and deduplication_key:
                row = connection.execute("SELECT * FROM activity_notification WHERE deduplication_key=%s", (deduplication_key,)).fetchone()
            if row is None:
                raise RuntimeError("notification insert did not return a row")
            actual_id = self._iso(row["id"])
            if actual_id == notification_id:
                self._job(connection, "NOTIFICATION_SEND", {"notificationId": actual_id}, f"notification:{actual_id}")
            return {"id": actual_id, "recipientId": row["recipient_id"], "type": row["notification_type"], "payload": row["payload"], "status": row["status"], "createdAt": self._iso(row["created_at"])}

    def rebuild_statistics(self, programme: str | None = None) -> dict[str, Any]:
        with self.transaction() as connection:
            programmes = [programme] if programme else [row["programme_slug"] for row in connection.execute("SELECT DISTINCT programme_slug FROM activity_activation").fetchall()]
            generated = []
            for slug in programmes:
                for category in ("ACTIVATOR", "HUNTER"):
                    row = connection.execute("SELECT count(*) AS participants,coalesce(sum(qso_count),0) AS qsos,coalesce(sum(activation_count),0) AS activations,coalesce(max(qso_count),0) AS max_qsos FROM activity_subject_aggregate WHERE programme_slug=%s AND category=%s", (slug, category)).fetchone()
                    snapshot = {"participants": row["participants"], "qsos": int(row["qsos"]), "activations": int(row["activations"]), "maxQsos": int(row["max_qsos"])}
                    snapshot_id = new_id()
                    connection.execute("INSERT INTO activity_statistic_snapshot(id,programme_slug,statistic_type,scope_key,metric_values,algorithm_version) VALUES (%s,%s,%s,%s,%s,%s)",
                                       (snapshot_id, slug, "PROGRAMME_SUMMARY", category, self._json(snapshot), "activity-v1"))
                    generated.append({"id": snapshot_id, "programme": slug, "category": category, **snapshot})
                for statistic_type, group_column in (("ENTITY", "entity_id"), ("JURISDICTION", "jurisdiction_code")):
                    rows = connection.execute(f"SELECT {group_column} AS scope_key,count(DISTINCT operator_id) AS participants,coalesce(sum(qso_count),0) AS qsos,count(*) AS activations FROM activity_activation WHERE programme_slug=%s AND {group_column} IS NOT NULL GROUP BY {group_column} ORDER BY {group_column}", (slug,)).fetchall()
                    for row in rows:
                        snapshot = {"participants": row["participants"], "qsos": int(row["qsos"]), "activations": row["activations"]}
                        snapshot_id = new_id()
                        connection.execute("INSERT INTO activity_statistic_snapshot(id,programme_slug,statistic_type,scope_key,metric_values,algorithm_version) VALUES (%s,%s,%s,%s,%s,%s)",
                                           (snapshot_id, slug, statistic_type, str(row["scope_key"]), self._json(snapshot), "activity-v1"))
                        generated.append({"id": snapshot_id, "programme": slug, "type": statistic_type, "scope": str(row["scope_key"]), **snapshot})
            return {"algorithmVersion": "activity-v1", "items": generated, "generatedAt": now()}

    def list_statistics(self, programme: str | None = None) -> list[dict[str, Any]]:
        with self.transaction() as connection:
            if programme:
                rows = connection.execute("SELECT id,programme_slug,statistic_type,scope_key,metric_values,algorithm_version,generated_at FROM activity_statistic_snapshot WHERE programme_slug=%s ORDER BY generated_at DESC", (programme,)).fetchall()
            else:
                rows = connection.execute("SELECT id,programme_slug,statistic_type,scope_key,metric_values,algorithm_version,generated_at FROM activity_statistic_snapshot ORDER BY generated_at DESC").fetchall()
            return [{"id": self._iso(row["id"]), "programme": row["programme_slug"], "type": row["statistic_type"], "scope": row["scope_key"], "values": row["metric_values"], "algorithmVersion": row["algorithm_version"], "generatedAt": self._iso(row["generated_at"])} for row in rows]

    def list_notifications(self, recipient_id: str) -> list[dict[str, Any]]:
        with self.transaction() as connection:
            rows = connection.execute("SELECT id,recipient_id,notification_type,payload,status,created_at,delivered_at FROM activity_notification WHERE recipient_id=%s ORDER BY created_at DESC", (recipient_id,)).fetchall()
            return [{"id": self._iso(row["id"]), "recipientId": row["recipient_id"], "type": row["notification_type"], "payload": row["payload"], "status": row["status"], "createdAt": self._iso(row["created_at"]), "deliveredAt": self._iso(row.get("delivered_at"))} for row in rows]

    def public_history(self, programme: str | None = None) -> list[dict[str, Any]]:
        return self.list_activations(programme)

    def leaderboard(self, programme: str, category: str, limit: int = 50) -> list[dict[str, Any]]:
        with self.transaction() as connection:
            rows = connection.execute("SELECT subject_id,qso_count,activation_count,unique_callsign_count,unique_entity_count FROM activity_subject_aggregate WHERE programme_slug=%s AND category=%s ORDER BY qso_count DESC,subject_id LIMIT %s", (programme, category, limit)).fetchall()
            return [dict(row) for row in rows]

    def close(self) -> None:
        if self.pool is not None:
            self.pool.close()
            self.pool = None
