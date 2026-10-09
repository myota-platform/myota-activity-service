from __future__ import annotations

import base64
import json
import os
import re
import tempfile
import threading
import time
import unittest
from io import BytesIO
from pathlib import Path
from unittest.mock import patch
from urllib.error import HTTPError
from urllib.request import Request, urlopen

from PIL import Image

from activity import ActivityHandler
from awards import PREVIEW_RENDERER, AwardsHandler
from common import BoundedThreadingHTTPServer, sign_token


class AwardDesignTests(unittest.TestCase):
    def setUp(self) -> None:
        self.directory = tempfile.TemporaryDirectory()
        self.environment = patch.dict(
            os.environ,
            {
                "MYOTA_OBJECT_STORAGE_LOCAL_DIR": self.directory.name,
            },
        )
        self.environment.start()
        AwardsHandler.store.data.clear()
        AwardsHandler.store.events.clear()
        self.server = BoundedThreadingHTTPServer(
            ("127.0.0.1", 0), ActivityHandler
        )
        self.thread = threading.Thread(
            target=self.server.serve_forever, daemon=True
        )
        self.thread.start()
        self.url = f"http://127.0.0.1:{self.server.server_port}"
        self.token = sign_token(
            {
                "sub": "fixture",
                "scp": ["awards.admin"],
                "exp": int(time.time()) + 60,
            }
        )

    def tearDown(self) -> None:
        self.server.shutdown()
        self.server.server_close()
        self.thread.join()
        self.environment.stop()
        self.directory.cleanup()

    def request(self, path, method="GET", data=None, media_type=None):
        headers = {"Authorization": f"Bearer {self.token}"}
        if media_type:
            headers["Content-Type"] = media_type
        elif data is not None:
            headers["Content-Type"] = "application/json"
            data = json.dumps(data).encode()
        with urlopen(
            Request(
                self.url + path, data=data, headers=headers, method=method
            ),
            timeout=5,
        ) as result:
            return json.load(result)

    def register(self, kind="BACKGROUND", object_key="art/test.png"):
        return self.request(
            "/v1/awards/assets",
            "POST",
            {
                "kind": kind,
                "name": "Named artwork",
                "objectKey": object_key,
                "mediaType": "image/png",
                "widthPx": 1,
                "heightPx": 1,
            },
        )

    def test_png_jpeg_binary_uploads_and_read_actual_dimensions(self):
        for image_format, mime in (
            ("PNG", "image/png"),
            ("JPEG", "image/jpeg"),
        ):
            asset = self.register(object_key=f"art/{image_format.lower()}")
            output = BytesIO()
            Image.effect_noise((1000, 1000), 100).convert("RGB").save(
                output, format=image_format
            )
            content = output.getvalue()
            if image_format == "PNG":
                self.assertGreater(len(content), 700_000)
            stored = self.request(
                f"/v1/awards/assets/{asset['id']}/content",
                "PUT",
                content,
                mime,
            )
            self.assertEqual(stored["contentStatus"], "STORED")
            self.assertEqual(stored["widthPx"], 1000)
            self.assertEqual(stored["heightPx"], 1000)
            result = self.request(f"/v1/awards/assets/{asset['id']}/content")
            self.assertEqual(
                base64.b64decode(result["contentBase64"]), content
            )
            self.assertEqual(result["mediaType"], mime)

    def test_rejects_fake_image_and_unauthorized_preview(self):
        asset = self.register()
        with self.assertRaises(HTTPError) as error:
            self.request(
                f"/v1/awards/assets/{asset['id']}/content",
                "PUT",
                b"not an image",
                "image/png",
            )
        self.assertEqual(error.exception.code, 400)
        error.exception.close()
        self.token = sign_token(
            {"scp": ["awards.read"], "exp": int(time.time()) + 60}
        )
        with self.assertRaises(HTTPError) as error:
            self.request("/v1/awards/previews", "POST", {})
        self.assertEqual(error.exception.code, 403)
        error.exception.close()

    def template(self):
        return {
            "elements": [
                {
                    "kind": kind,
                    "x": 0.1,
                    "y": 0.1 + index * 0.12,
                    "width": 0.8,
                    "height": 0.07,
                }
                for index, kind in enumerate(
                    [
                        "AWARD_NAME",
                        "CALLSIGN",
                        "PERSON_NAME",
                        "DATE_OBTAINED",
                        "MANAGER_NAME",
                        "MANAGER_SIGNATURE",
                    ]
                )
            ]
        }

    def test_preview_dimensions_mock_data_and_no_persisted_side_effects(self):
        for page, dimensions in (
            ("A4", (210, 297)),
            ("LETTER", (215.9, 279.4)),
        ):
            for orientation in ("PORTRAIT", "LANDSCAPE"):
                result = self.request(
                    "/v1/awards/previews",
                    "POST",
                    {
                        "name": "Test Award",
                        "printSpec": {
                            "page": page,
                            "orientation": orientation,
                            "dpi": 150,
                        },
                        "template": self.template(),
                    },
                )
                self.assertEqual(result["mockData"]["CALLSIGN"], "EA7TEST")
                pdf = base64.b64decode(result["contentBase64"])
                self.assertTrue(pdf.startswith(b"%PDF"))
                width, height = map(
                    float,
                    re.search(
                        rb"/MediaBox\s*\[\s*0\s+0\s+([\d.]+)\s+([\d.]+)", pdf
                    ).groups(),
                )
                expected = (
                    dimensions
                    if orientation == "PORTRAIT"
                    else dimensions[::-1]
                )
                self.assertAlmostEqual(width, expected[0] / 25.4 * 72, delta=1)
                self.assertAlmostEqual(
                    height, expected[1] / 25.4 * 72, delta=1
                )
        self.assertEqual(AwardsHandler.store.events, [])
        self.assertFalse(AwardsHandler._bucket("definitions"))
        self.assertFalse(AwardsHandler._bucket("requests"))
        self.assertFalse(AwardsHandler._bucket("issuances"))
        self.assertEqual(list(Path(self.directory.name).rglob("*.pdf")), [])

    def test_preview_missing_background_and_invalid_layout_are_errors(self):
        template = self.template()
        template["elements"][0]["x"] = 0.9
        for extra in (
            {"backgroundAsset": {"objectKey": "unregistered"}},
            {"template": template},
            {"printSpec": {"dpi": 500}},
        ):
            with self.assertRaises(HTTPError) as error:
                self.request(
                    "/v1/awards/previews",
                    "POST",
                    {
                        "name": "Test",
                        "template": self.template(),
                        "printSpec": {"dpi": 150},
                        **extra,
                    },
                )
            self.assertEqual(error.exception.code, 400)
            error.exception.close()

    def test_preview_backpressure_does_not_allocate_another_renderer(self):
        self.assertTrue(PREVIEW_RENDERER.acquire(blocking=False))
        try:
            with self.assertRaises(HTTPError) as error:
                self.request("/v1/awards/previews", "POST", {})
            self.assertEqual(error.exception.code, 429)
            error.exception.close()
        finally:
            PREVIEW_RENDERER.release()

    def test_draft_roundtrip_preserves_signature_manager_and_custom_text(self):
        signature = self.register("SIGNATURE", "signature/manager.png")
        template = self.template()
        template["elements"].append(
            {
                "kind": "CUSTOM_TEXT",
                "label": "Programme-owned wording",
                "x": 0.1,
                "y": 0.9,
                "width": 0.8,
                "height": 0.05,
            }
        )
        saved = self.request(
            "/v1/awards",
            "POST",
            {
                "programmeSlug": "fixture",
                "code": "ROUNDTRIP",
                "name": "Award",
                "category": "HUNTER",
                "condition": {"kind": "QSO_COUNT", "value": 10},
                "levels": [{"id": "10", "threshold": 10}],
                "backgroundAsset": {
                    "kind": "BACKGROUND",
                    "objectKey": "legacy.png",
                    "widthPx": 3508,
                    "heightPx": 2481,
                },
                "printSpec": {
                    "page": "A4",
                    "orientation": "LANDSCAPE",
                    "dpi": 300,
                },
                "signatureAssetId": signature["id"],
                "managerName": "Award manager",
                "effectiveFrom": "2026-10-09T12:00:00Z",
                "template": template,
            },
        )
        saved["name"] = "Edited award"
        updated = self.request(f"/v1/awards/{saved['id']}", "PATCH", saved)
        loaded = self.request(f"/v1/awards/{saved['id']}")
        self.assertEqual(updated["name"], "Edited award")
        for field in (
            "signatureAssetId",
            "managerName",
            "effectiveFrom",
            "template",
            "printSpec",
        ):
            self.assertEqual(loaded[field], saved[field])
