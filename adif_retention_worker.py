"""Delete completed and failed ADIF source objects after their retention window."""

from __future__ import annotations

import argparse
import logging
import os
import time
from datetime import datetime, timedelta, timezone

from activity_repository import ActivityRepository
from storage import ObjectStore

logger = logging.getLogger("myota.adif_retention")


def run_retention_pass(
    repository: ActivityRepository,
    store: ObjectStore,
    *,
    bucket: str,
    retention_days: int = 15,
    batch_size: int = 500,
    now_utc: datetime | None = None,
) -> int:
    if retention_days < 1 or batch_size < 1:
        raise ValueError("retention days and batch size must be positive")
    cutoff = (now_utc or datetime.now(timezone.utc)) - timedelta(
        days=retention_days
    )
    deleted = 0
    while True:
        records = repository.list_adif_objects_due_for_retention(
            cutoff, bucket, batch_size
        )
        if not records:
            break
        errors: list[tuple[str, Exception]] = []
        for record in records:
            try:
                # Defend against cross-purpose deletion even if a repository
                # implementation accidentally returns an unexpected bucket.
                if record["bucket"] != bucket:
                    raise ValueError(
                        "refusing to delete an object outside the ADIF bucket"
                    )
                store.delete(bucket, record["objectKey"])
                repository.mark_adif_source_deleted(record["id"])
                deleted += 1
            except Exception as exc:  # retain DB marker so this object retries
                logger.exception(
                    "ADIF source object cleanup failed for import %s",
                    record["id"],
                )
                errors.append((record["id"], exc))
        if errors:
            failed_ids = ", ".join(import_id for import_id, _ in errors[:10])
            raise RuntimeError(
                f"could not delete ADIF source objects for imports: {failed_ids}"
            ) from errors[0][1]
        if len(records) < batch_size:
            break
    return deleted


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--loop",
        action="store_true",
        help="repeat at the configured interval (for Compose)",
    )
    args = parser.parse_args()
    logging.basicConfig(level=os.environ.get("LOG_LEVEL", "INFO"))
    repository = ActivityRepository("ACTIVITY_DATABASE_URL")
    if not repository.durable:
        raise RuntimeError(
            "ACTIVITY_DATABASE_URL is required for ADIF retention"
        )
    store = ObjectStore()
    bucket = os.environ.get("MYOTA_ADIF_BUCKET", "myota-adif")
    retention_days = int(os.environ.get("MYOTA_ADIF_RETENTION_DAYS", "15"))
    batch_size = int(os.environ.get("MYOTA_ADIF_RETENTION_BATCH_SIZE", "500"))
    interval = max(
        60,
        int(os.environ.get("MYOTA_ADIF_RETENTION_INTERVAL_SECONDS", "86400")),
    )

    while True:
        deleted = run_retention_pass(
            repository,
            store,
            bucket=bucket,
            retention_days=retention_days,
            batch_size=batch_size,
        )
        logger.info(
            "ADIF retention pass finished; deleted %d source objects", deleted
        )
        if not args.loop:
            return
        time.sleep(interval)


if __name__ == "__main__":
    main()
