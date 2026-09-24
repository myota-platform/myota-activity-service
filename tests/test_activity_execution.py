from __future__ import annotations

import unittest
import sys
from pathlib import Path

_root = Path(__file__).parents[1]
sys.path.insert(0, str(_root / "services" if (_root / "services").is_dir() else _root))
from activity import ActivityHandler
from activity_domain import parse_adif
from storage import ObjectStore


class ActivityExecutionTests(unittest.TestCase):
    def setUp(self) -> None:
        ActivityHandler.store.items.clear()
        ActivityHandler.store.events.clear()
        ActivityHandler.store.data.clear()
        ActivityHandler.store.idempotency.clear()

    def test_activation_rules_and_qso_normalization(self) -> None:
        activation = ActivityHandler.create_activation(None, {"_body": {
            "programmeSlug": "sevilla-demo", "entityId": "park-1", "entityType": "PARK",
            "operatorId": "operator-1", "operatorCallsign": "EA7TEST", "callsignLifecycleStatus": "VERIFIED",
            "startedAt": "2026-01-01T10:00:00Z", "programmeRules": {"minimumQsos": 1, "allowedBands": ["20M"], "allowedModes": ["SSB"]},
            "location": {"latitude": 37.38, "longitude": -5.99}}})
        with self.assertRaises(ValueError):
            ActivityHandler.add_qso(None, {"activationId": activation["id"], "_body": {"workedCallsign": "K1ABC", "timestamp": "2026-01-01T10:05:00Z", "band": "11M", "mode": "SSB"}})
        qso = ActivityHandler.add_qso(None, {"activationId": activation["id"], "_body": {"workedCallsign": "k1abc", "timestamp": "2026-01-01T10:05:00Z", "band": "20M", "mode": "SSB"}})
        self.assertEqual(qso["qso"]["workedCallsign"], "K1ABC")
        closed = ActivityHandler.close_activation(None, {"activationId": activation["id"], "_body": {"endedAt": "2026-01-01T11:00:00Z"}})
        self.assertEqual(closed["status"], "CLOSED")
        self.assertTrue(closed["ruleEvaluation"]["valid"])

    def test_adif_normalization_and_malware_gate(self) -> None:
        records = parse_adif("<CALL:7>EA7TEST<QSO_DATE:8>20260101<TIME_ON:6>100500<BAND:3>20M<MODE:3>SSB<EOR>")
        self.assertEqual(records[0]["workedCallsign"], "EA7TEST")
        self.assertEqual(records[0]["timestamp"], "2026-01-01T10:05:00Z")
        clean = ObjectStore.scan_content(b"<ADIF_VER:5>3.1.0", "log.adi")
        self.assertEqual(clean["status"], "CLEAN")
        with self.assertRaises(ValueError):
            ObjectStore.scan_content(b"X5O!P%@AP[4\\PZX54(P^)7CC)7}$EICAR-STANDARD-ANTIVIRUS-TEST-FILE!$H+H*", "infected.adi")


if __name__ == "__main__":
    unittest.main()
