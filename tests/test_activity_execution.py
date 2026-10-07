from __future__ import annotations

import unittest
import sys
from pathlib import Path

_root = Path(__file__).parents[1]
sys.path.insert(
    0, str(_root / "services" if (_root / "services").is_dir() else _root)
)
from activity import ActivityHandler
from activity_domain import parse_adif
from activity_worker import process_award_recalculation
from storage import ObjectStore


class ActivityExecutionTests(unittest.TestCase):
    def setUp(self) -> None:
        ActivityHandler.store.items.clear()
        ActivityHandler.store.events.clear()
        ActivityHandler.store.data.clear()
        ActivityHandler.store.idempotency.clear()

    def test_activation_rules_and_qso_normalization(self) -> None:
        activation = ActivityHandler.create_activation(
            None,
            {
                "_body": {
                    "programmeSlug": "sevilla-demo",
                    "entityId": "park-1",
                    "entityType": "PARK",
                    "operatorId": "operator-1",
                    "operatorCallsign": "EA7TEST",
                    "callsignLifecycleStatus": "VERIFIED",
                    "startedAt": "2026-01-01T10:00:00Z",
                    "programmeRules": {
                        "minimumQsos": 1,
                        "allowedBands": ["20M"],
                        "allowedModes": ["SSB"],
                    },
                    "location": {"latitude": 37.38, "longitude": -5.99},
                }
            },
        )
        with self.assertRaises(ValueError):
            ActivityHandler.add_qso(
                None,
                {
                    "activationId": activation["id"],
                    "_body": {
                        "workedCallsign": "K1ABC",
                        "timestamp": "2026-01-01T10:05:00Z",
                        "band": "11M",
                        "mode": "SSB",
                    },
                },
            )
        qso = ActivityHandler.add_qso(
            None,
            {
                "activationId": activation["id"],
                "_body": {
                    "workedCallsign": "k1abc",
                    "timestamp": "2026-01-01T10:05:00Z",
                    "band": "20M",
                    "mode": "SSB",
                },
            },
        )
        self.assertEqual(qso["qso"]["workedCallsign"], "K1ABC")
        closed = ActivityHandler.close_activation(
            None,
            {
                "activationId": activation["id"],
                "_body": {"endedAt": "2026-01-01T11:00:00Z"},
            },
        )
        self.assertEqual(closed["status"], "CLOSED")
        self.assertTrue(closed["ruleEvaluation"]["valid"])

    def test_adif_normalization_and_malware_gate(self) -> None:
        records = parse_adif(
            "<CALL:7>EA7TEST<QSO_DATE:8>20260101<TIME_ON:6>100500<BAND:3>20M<MODE:3>SSB<EOR>"
        )
        self.assertEqual(records[0]["workedCallsign"], "EA7TEST")
        self.assertEqual(records[0]["timestamp"], "2026-01-01T10:05:00Z")
        clean = ObjectStore.scan_content(b"<ADIF_VER:5>3.1.0", "log.adi")
        self.assertEqual(clean["status"], "CLEAN")
        with self.assertRaises(ValueError):
            ObjectStore.scan_content(
                b"X5O!P%@AP[4\\PZX54(P^)7CC)7}$EICAR-STANDARD-ANTIVIRUS-TEST-FILE!$H+H*",
                "infected.adi",
            )

    def test_phase3_activation_and_ingestion_resources_queue_jobs(
        self,
    ) -> None:
        activation = ActivityHandler.create_activation(
            None,
            {
                "_body": {
                    "programmeSlug": "sevilla-demo",
                    "entityId": "park-1",
                    "operatorId": "operator-1",
                    "startedAt": "2026-01-01T10:00:00Z",
                    "programmeRules": {"minimumQsos": 0},
                }
            },
        )
        ingestion = ActivityHandler.create_qso_ingestion(
            None,
            {
                "_body": {
                    "activationId": activation["id"],
                    "qsos": [
                        {
                            "workedCallsign": "K1ABC",
                            "timestamp": "2026-01-01T10:05:00Z",
                        }
                    ],
                },
                "Idempotency-Key": "phase3-ingestion",
            },
        )
        self.assertEqual(ingestion["status"], "QUEUED")
        self.assertEqual(
            ActivityHandler.get_job(None, {"jobId": ingestion["id"]})["kind"],
            "QSO_INGESTION",
        )
        closed = ActivityHandler.update_activation(
            None,
            {
                "activationId": activation["id"],
                "_body": {
                    "status": "CLOSED",
                    "endedAt": "2026-01-01T11:00:00Z",
                },
            },
        )
        self.assertEqual(closed["status"], "CLOSED")

    def test_phase3_statistics_job_resource_has_status(self) -> None:
        queued = ActivityHandler.rebuild_statistics(
            None,
            {
                "_body": {"programmeSlug": "sevilla-demo"},
                "Idempotency-Key": "phase3-statistics",
            },
        )
        self.assertEqual(queued["status"], "QUEUED")
        self.assertEqual(
            ActivityHandler.get_job(None, {"jobId": queued["jobId"]})["kind"],
            "STATISTICS_REBUILD",
        )

    def test_entity_deletion_resource_routes_are_available(self) -> None:
        self.assertIn(
            ("GET", "/v1/activations/entity-deletion-impacts/{entityId}"),
            ActivityHandler.routes,
        )
        self.assertIn(
            ("POST", "/v1/activations/entity-deletion-cascades"),
            ActivityHandler.routes,
        )
        impact = ActivityHandler.entity_deletion_impact(
            None, {"entityId": "park-1"}
        )
        self.assertEqual(impact["entityId"], "park-1")

    def test_http_activity_mutation_requires_owner_or_admin(self) -> None:
        with self.assertRaises(PermissionError):
            ActivityHandler.create_activation(
                None,
                {
                    "_http": "1",
                    "Authorization": "",
                    "_body": {
                        "programmeSlug": "sevilla-demo",
                        "entityId": "park-1",
                        "operatorId": "operator-1",
                        "startedAt": "2026-01-01T10:00:00Z",
                    },
                },
            )

    def test_award_recalculation_uses_requested_version_and_all_subjects(
        self,
    ) -> None:
        class FakeRepository:
            def __init__(self) -> None:
                self.saved = []
                self.notifications = []

            def list_collection(self, collection):
                return [
                    {
                        "id": "award-v1",
                        "programmeSlug": "demo",
                        "status": "PUBLISHED",
                        "version": 1,
                        "category": "HUNTER",
                        "condition": {
                            "kind": "QSO_COUNT",
                            "operator": "GTE",
                            "value": 1,
                        },
                        "achievementMetric": "QSO_COUNT",
                        "levels": [{"id": "one", "threshold": 1}],
                        "code": "ONE",
                    },
                    {
                        "id": "award-v2",
                        "programmeSlug": "demo",
                        "status": "PUBLISHED",
                        "version": 2,
                        "category": "HUNTER",
                        "condition": {
                            "kind": "QSO_COUNT",
                            "operator": "GTE",
                            "value": 1,
                        },
                        "achievementMetric": "QSO_COUNT",
                        "levels": [{"id": "one", "threshold": 1}],
                        "code": "ONE",
                    },
                ]

            def list_subject_ids(self, programme, category):
                return ["hunter-1", "hunter-2"]

            def subject_facts(self, programme, subject_id, category):
                return {
                    "qsoCount": 1,
                    "uniqueCallsignCount": 1,
                    "uniqueEntityCount": 1,
                    "activationCount": 1,
                }

            def save_progress(
                self, award, subject_id, category, facts, evaluation
            ):
                self.saved.append(
                    (award["id"], subject_id, evaluation["ruleVersion"])
                )

            def create_notification(self, *args):
                self.notifications.append(args)

        repository = FakeRepository()
        process_award_recalculation(
            repository,
            {"programmeSlug": "demo", "awardId": "award-v1", "ruleVersion": 1},
        )
        self.assertEqual(
            repository.saved,
            [("award-v1", "hunter-1", 1), ("award-v1", "hunter-2", 1)],
        )


if __name__ == "__main__":
    unittest.main()
