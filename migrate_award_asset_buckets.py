"""Copy legacy shared-bucket award assets to their kind-specific buckets.

Run once during an upgrade with ACTIVITY_DATABASE_URL and the S3 settings set.
The default is a report-only dry run; --apply copies/verifies objects and then
updates the activity_asset metadata. Source objects are deliberately retained.
"""

from __future__ import annotations

import argparse
import hashlib
import os
import sys

from activity_repository import ActivityRepository
from awards import AwardsHandler
from storage import ObjectStore


def target_bucket(asset: dict[str, object]) -> str:
    kind = str(asset.get("kind", "")).upper()
    if kind == "BACKGROUND":
        return os.environ.get("MYOTA_AWARD_ASSET_BUCKET", "myota-award-assets")
    if kind == "SIGNATURE":
        return os.environ.get(
            "MYOTA_AWARD_SIGNATURE_BUCKET", "myota-award-signatures"
        )
    raise ValueError(f"unsupported asset kind for {asset.get('id')}: {kind}")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--apply",
        action="store_true",
        help="copy objects and update metadata (default: dry run)",
    )
    args = parser.parse_args()
    repository = ActivityRepository()
    if not repository.durable:
        parser.error(
            "ACTIVITY_DATABASE_URL must point to the durable activity database"
        )
    AwardsHandler.repository = repository
    store = ObjectStore()
    assets = repository.list_collection("assets")
    moved = skipped = failed = 0
    for asset in assets:
        source = str(asset.get("bucket") or "myota-awards")
        destination = target_bucket(asset)
        if source == destination:
            skipped += 1
            continue
        print(
            f"{asset['id']} {asset['kind']}: {source}/{asset['objectKey']} -> {destination}/{asset['objectKey']}"
        )
        if not args.apply:
            continue
        content = store.get(source, str(asset["objectKey"]))
        if content is None:
            if asset.get("contentStatus") == "MISSING":
                # A registered placeholder with no stored object can be safely
                # re-pointed; its next upload will create it in the new bucket.
                AwardsHandler._save("assets", {**asset, "bucket": destination})
                moved += 1
                print(
                    f"No stored object; updated placeholder metadata for {asset['id']}"
                )
                continue
            print(
                f"ERROR: source object not found; metadata unchanged for {asset['id']}",
                file=sys.stderr,
            )
            failed += 1
            continue
        copied = store.put(
            destination,
            str(asset["objectKey"]),
            content,
            str(asset["mediaType"]),
        )
        verification = store.get(destination, str(asset["objectKey"]))
        if (
            not verification
            or copied["sha256"] != hashlib.sha256(verification).hexdigest()
            or (
                asset.get("contentSha256")
                and copied["sha256"] != asset["contentSha256"]
            )
        ):
            print(
                f"ERROR: checksum mismatch; metadata unchanged for {asset['id']}",
                file=sys.stderr,
            )
            failed += 1
            continue
        updated = {**asset, "bucket": destination}
        AwardsHandler._save("assets", updated)
        moved += 1
    print(
        f"assets={len(assets)} copied={moved} unchanged={skipped} failed={failed} mode={'apply' if args.apply else 'dry-run'}"
    )
    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
