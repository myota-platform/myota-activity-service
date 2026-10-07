from __future__ import annotations

import sys
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path

_root = Path(__file__).parents[1]
sys.path.insert(0, str(_root))
from adif_retention_worker import run_retention_pass
from activity_repository import ActivityRepository


class FakeRepository:
    def __init__(self) -> None:
        self.records = [
            {
                "id": "completed-1",
                "bucket": "myota-adif",
                "objectKey": "adif/a/one.adi",
            },
            {
                "id": "completed-2",
                "bucket": "myota-adif",
                "objectKey": "adif/b/two.adi",
            },
        ]
        self.marked: list[str] = []
        self.cutoffs: list[datetime] = []

    def list_adif_objects_due_for_retention(
        self, cutoff: datetime, bucket: str, limit: int
    ):
        self.cutoffs.append(cutoff)
        return [
            record for record in self.records if record["bucket"] == bucket
        ][:limit]

    def mark_adif_source_deleted(self, import_id: str) -> None:
        self.marked.append(import_id)
        self.records = [
            record for record in self.records if record["id"] != import_id
        ]


class FakeObjectStore:
    def __init__(self) -> None:
        self.deleted: list[tuple[str, str]] = []

    def delete(self, bucket: str, object_key: str) -> None:
        self.deleted.append((bucket, object_key))


class AdifRetentionTests(unittest.TestCase):
    def test_repository_scope_is_terminal_old_adif_objects_only(self) -> None:
        class FakeConnection:
            query = ""
            params = ()

            def execute(self, query, params):
                self.query = query
                self.params = params
                return self

            def fetchall(self):
                return []

        repository = object.__new__(ActivityRepository)
        connection = FakeConnection()

        from contextlib import contextmanager

        @contextmanager
        def transaction():
            yield connection

        repository.transaction = transaction
        cutoff = datetime(2026, 9, 20, tzinfo=timezone.utc)
        repository.list_adif_objects_due_for_retention(
            cutoff, "myota-adif", 100
        )

        self.assertIn("status IN ('COMPLETED','FAILED')", connection.query)
        self.assertIn("completed_at < %s", connection.query)
        self.assertIn("source_deleted_at IS NULL", connection.query)
        self.assertIn("bucket=%s", connection.query)
        self.assertEqual(connection.params, (cutoff, "myota-adif", 100))

    def test_deletes_only_objects_returned_from_adif_retention_query(
        self,
    ) -> None:
        repository = FakeRepository()
        store = FakeObjectStore()
        current = datetime(2026, 10, 5, 12, tzinfo=timezone.utc)

        deleted = run_retention_pass(
            repository, store, bucket="myota-adif", now_utc=current
        )

        self.assertEqual(deleted, 2)
        self.assertEqual(repository.marked, ["completed-1", "completed-2"])
        self.assertEqual(
            store.deleted,
            [
                ("myota-adif", "adif/a/one.adi"),
                ("myota-adif", "adif/b/two.adi"),
            ],
        )
        self.assertEqual(repository.cutoffs, [current - timedelta(days=15)])

    def test_object_store_failure_does_not_mark_source_deleted(self) -> None:
        repository = FakeRepository()

        class FailingStore(FakeObjectStore):
            def delete(self, bucket: str, object_key: str) -> None:
                raise OSError("storage unavailable")

        with self.assertLogs("myota.adif_retention", level="ERROR"):
            with self.assertRaisesRegex(RuntimeError, "completed-1"):
                run_retention_pass(
                    repository, FailingStore(), bucket="myota-adif"
                )

        self.assertEqual(repository.marked, [])

    def test_invalid_retention_configuration_is_rejected(self) -> None:
        with self.assertRaises(ValueError):
            run_retention_pass(
                FakeRepository(),
                FakeObjectStore(),
                bucket="myota-adif",
                retention_days=0,
            )


if __name__ == "__main__":
    unittest.main()
