from __future__ import annotations

import base64
import os
import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parents[1]))

from awards import AwardsHandler, evaluate_condition


class AwardServiceTests(unittest.TestCase):
    def setUp(self) -> None:
        AwardsHandler.store.items.clear()
        AwardsHandler.store.events.clear()
        AwardsHandler.store.data.clear()
        AwardsHandler.store.idempotency.clear()

    def tearDown(self) -> None:
        os.environ.pop("MYOTA_OBJECT_STORAGE_LOCAL_DIR", None)

    def _template(self) -> dict:
        kinds = ("AWARD_NAME", "CALLSIGN", "PERSON_NAME", "DATE_OBTAINED", "MANAGER_NAME", "MANAGER_SIGNATURE")
        return {"elements": [{"kind": kind, "x": 0.1, "y": 0.1 + index * 0.1, "width": 0.8, "height": 0.06}
                             for index, kind in enumerate(kinds)]}

    def test_condition_ast_supports_nested_and_or(self) -> None:
        condition = {"kind": "AND", "conditions": [
            {"kind": "OR", "conditions": [{"kind": "QSO_COUNT", "operator": "GTE", "value": 10},
                                               {"kind": "UNIQUE_ENTITIES", "operator": "GTE", "value": 3}]},
            {"kind": "ENTITY_TYPE", "values": ["MUNICIPAL_PARK"]}]}
        self.assertTrue(evaluate_condition(condition, {"qsoCount": 12, "uniqueEntityCount": 1, "entityType": "MUNICIPAL_PARK"}))
        self.assertFalse(evaluate_condition(condition, {"qsoCount": 2, "uniqueEntityCount": 1, "entityType": "MUNICIPAL_PARK"}))

    def test_award_lifecycle_levels_request_and_issuance(self) -> None:
        background = AwardsHandler.register_asset(None, {"_body": {"kind": "BACKGROUND", "name": "A4 certificate",
            "objectKey": "backgrounds/a4.png", "mediaType": "image/png", "widthPx": 2481, "heightPx": 3508}})
        signature = AwardsHandler.register_asset(None, {"_body": {"kind": "SIGNATURE", "name": "Award manager",
            "objectKey": "signatures/manager.png", "mediaType": "image/png", "widthPx": 1200, "heightPx": 360}})
        award = AwardsHandler.save_award(None, {"_body": {"programmeSlug": "regional-ota", "code": "SEVILLA-50",
            "name": "Sevilla Fifty", "category": "HUNTER", "achievementMetric": "QSO_COUNT",
            "condition": {"kind": "ENTITY_TYPE", "values": ["MUNICIPAL_PARK"]},
            "levels": [{"id": "10", "label": "Bronze", "threshold": 10}, {"id": "50", "label": "Gold", "threshold": 50}],
            "backgroundAsset": background, "printSpec": {"page": "A4", "orientation": "PORTRAIT", "dpi": 300},
            "template": self._template()}})
        award = AwardsHandler.submit_award(None, {"awardId": award["id"]})
        award = AwardsHandler.review_award(None, {"awardId": award["id"], "_body": {"decision": "APPROVED", "reviewerId": "admin"}})
        award = AwardsHandler.publish_award(None, {"awardId": award["id"], "_body": {"effectiveFrom": "2026-01-01T00:00:00Z", "publisherId": "admin"}})
        evaluation = AwardsHandler.evaluate(None, {"_body": {"awardId": award["id"], "subjectId": "operator-1",
            "facts": {"qsoCount": 12, "entityType": "MUNICIPAL_PARK"}}})
        self.assertTrue(evaluation["levels"][0]["eligible"])
        self.assertFalse(evaluation["levels"][1]["eligible"])
        request = AwardsHandler.request_award(None, {"_body": {"awardId": award["id"], "levelId": "10", "subjectId": "operator-1",
            "callsign": "EA7TEST", "personName": "Test Operator", "facts": {"qsoCount": 12, "entityType": "MUNICIPAL_PARK"}}})
        issuance = AwardsHandler.issue_request(None, {"requestId": request["id"], "_body": {"managerName": "Award Manager", "signatureAssetId": signature["id"]}})
        self.assertEqual(issuance["artifact"]["mediaType"], "application/pdf")
        self.assertEqual(issuance["renderSpec"]["printSpec"]["page"], "A4")
        higher = AwardsHandler.request_award(None, {"_body": {"awardId": award["id"], "levelId": "50", "subjectId": "operator-1",
            "callsign": "EA7TEST", "personName": "Test Operator", "facts": {"qsoCount": 55, "entityType": "MUNICIPAL_PARK"}}})
        self.assertEqual(higher["levelId"], "50")

    def test_asset_content_is_persisted_in_local_object_store(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            os.environ["MYOTA_OBJECT_STORAGE_LOCAL_DIR"] = directory
            asset = AwardsHandler.register_asset(None, {"_body": {"kind": "SIGNATURE", "name": "Manager",
                "objectKey": "signatures/manager.bin", "mediaType": "image/png", "widthPx": 120, "heightPx": 40}})
            content = base64.b64encode(b"test-image-bytes").decode()
            stored = AwardsHandler.asset_content(None, {"assetId": asset["id"], "_body": {"contentBase64": content}})
            self.assertEqual(stored["contentStatus"], "STORED")
            self.assertTrue(stored["contentSha256"])
            self.assertTrue(os.path.exists(os.path.join(directory, "myota-awards", "signatures", "manager.bin")))

    def test_issuance_renders_pdf_when_local_assets_exist(self) -> None:
        try:
            from PIL import Image
            from io import BytesIO
        except ImportError:
            self.skipTest("Pillow is not installed in the dependency-free host test environment")
        with tempfile.TemporaryDirectory() as directory:
            os.environ["MYOTA_OBJECT_STORAGE_LOCAL_DIR"] = directory
            def png(width: int, height: int, color: str) -> str:
                output = BytesIO(); Image.new("RGBA", (width, height), color).save(output, format="PNG")
                return base64.b64encode(output.getvalue()).decode()
            background = AwardsHandler.register_asset(None, {"_body": {"kind": "BACKGROUND", "name": "A4",
                "objectKey": "backgrounds/a4.png", "mediaType": "image/png", "widthPx": 2481, "heightPx": 3508}})
            signature = AwardsHandler.register_asset(None, {"_body": {"kind": "SIGNATURE", "name": "Manager",
                "objectKey": "signatures/manager.png", "mediaType": "image/png", "widthPx": 1200, "heightPx": 360}})
            AwardsHandler.asset_content(None, {"assetId": background["id"], "_body": {"contentBase64": png(24, 32, "white")}})
            AwardsHandler.asset_content(None, {"assetId": signature["id"], "_body": {"contentBase64": png(24, 8, "black")}})
            award = AwardsHandler.save_award(None, {"_body": {"programmeSlug": "regional-ota", "code": "LOCAL-10",
                "name": "Local Ten", "category": "ACTIVATOR", "condition": {"kind": "QSO_COUNT", "operator": "GTE", "value": 1},
                "levels": [{"id": "10", "threshold": 1}], "backgroundAsset": background,
                "printSpec": {"page": "A4", "orientation": "PORTRAIT", "dpi": 150}, "template": self._template()}})
            award = AwardsHandler.submit_award(None, {"awardId": award["id"]})
            award = AwardsHandler.review_award(None, {"awardId": award["id"], "_body": {"decision": "APPROVED", "reviewerId": "admin"}})
            award = AwardsHandler.publish_award(None, {"awardId": award["id"], "_body": {"effectiveFrom": "2026-01-01T00:00:00Z", "publisherId": "admin"}})
            request = AwardsHandler.request_award(None, {"_body": {"awardId": award["id"], "levelId": "10", "subjectId": "operator-1",
                "callsign": "EA7TEST", "personName": "Test Operator", "facts": {"qsoCount": 1}}})
            issuance = AwardsHandler.issue_request(None, {"requestId": request["id"], "_body": {"managerName": "Award Manager", "signatureAssetId": signature["id"]}})
            self.assertTrue(issuance["artifact"]["downloadReady"])
            self.assertGreater(issuance["artifact"]["byteSize"], 0)


if __name__ == "__main__":
    unittest.main()
