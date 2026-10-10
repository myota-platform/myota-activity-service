# MyOTA activity and awards service

MyOTA is a programme-agnostic platform for outdoor activation programmes. MPOTA is represented as a configured programme, not as the platform itself. No rules or charter text are copied from POTA or any other programme: every programme supplies its own configuration, policy, eligibility, awards and public charter.

This repository owns the activity bounded context: activations, normalized QSOs,
ADIF ingestion, programme-owned awards, progress, certificates and execution
statistics. Activity and awards intentionally share one API process and port
(`8004`). Deployment and migration orchestration live in `myota-deploy`; the
versioned activity migration in `migrations/` is copied and applied there.

Activity notification replicas share the registry-owned pull durable
`activity-notifications-v1`. Its exact filters cover the 19 selected Identity
facts and two Geodata review/status facts; unrelated events are not consumed as
no-ops. Activity stores a minimal event ID/type notice and uses the stable
`event:<eventId>` uniqueness key plus its processed-event table for redelivery
safety. Delivery uses explicit ACK, bounded retry/backoff, a redacted and
auditable dead-letter/redrive path, and graceful unsubscribe/drain. Metrics and
alerts expose processed, duplicate, retry, and dead-letter outcomes. The
deployment hooks create the successor before rollout and retire only the known
broad legacy durables after validating it. See the
[notification consumer runbook](https://github.com/myota-platform/myota-docs/blob/main/docs/operations/messaging/activity-notification-consumer.md)
and [Phase 3 evidence](https://github.com/myota-platform/myota-docs/blob/main/docs/operations/messaging/evidence/phase3-domain-consumers-2026-10-10.md).

Phase 4 moves the six accepted job kinds to `MYOTA_ACTIVITY_WORK`. The work
command carries only a stable job ID; the Activity database owns status,
payload, leases, retries, and redrive audit. Per-kind pull durables share
workers by group and use explicit post-commit ACK with UUID lease fencing. The
migration preserves `activity_job` history, drops the DB-poller claim index,
and removes only synthetic state-only notification jobs. The implementation
and isolated tests are complete, but production still runs the old poller until
the staged drain/backfill/rollout is verified. Follow the [Activity work queue
runbook](https://github.com/myota-platform/myota-docs/blob/main/docs/operations/messaging/activity-work-queues.md)
and [Phase 4 evidence](https://github.com/myota-platform/myota-docs/blob/main/docs/operations/messaging/evidence/phase4-activity-work-2026-10-10.md).

The platform purpose and the distinction between reusable activity capability
and programme-owned policy are documented in the
[MyOTA charter](https://github.com/myota-platform/myota-docs/blob/main/docs/project-charter.md).
The current production-readiness and participant-experience gaps are tracked
in the [charter gap analysis](https://github.com/myota-platform/myota-docs/blob/main/docs/charter-gap-analysis.md).

## What works now

- Activity consumes identity/callsign authorization and programme-owned rule
  inputs through service APIs. Identity, shared categories, themes, OIDC and
  geodata import/review remain owned by their respective services.
- Relational, indexed activation/QSO storage with deduplication, idempotency,
  aggregate facts and PostgreSQL `COPY` batch ingestion; the generic JSONB
  state store is not used in durable activity mode.
- The activity migration creates local state, idempotency, outbox,
  consumer-checkpoint and dead-letter tables inside `myota_activity`; these
  infrastructure tables are not read from `myota_core`.
- Migration `005_outbox_dead_letter_redrive.sql` adds durable resolution state
  and an operator-audited redrive history. Redrive preserves the original
  dead-letter record and republishes from the retained Activity outbox row.
- Activation validity windows, location/rule checks, verified callsign inputs,
  normalization, band/mode validation, correction workflows and close-time
  programme rule evaluation.
- ADIF upload safety gate, SeaweedFS-backed S3-compatible object storage, asynchronous parsing and
  import result tracking.
- Completed and failed ADIF source objects are removed from the dedicated
  `myota-adif` bucket 15 days after their terminal processing timestamp. The
  import result and diagnostics remain in PostgreSQL; queued and processing
  imports are not eligible.
- Programme-owned hunter/activator awards, nested conditions, levels, server-side progress, SeaweedFS/S3-backed assets, certificate rendering, participant requests and permanent issuance records are exposed by the same service on port 8004 under `/v1/awards`.
- Object storage separates mutable background artwork (`myota-award-assets`), manager signatures (`myota-award-signatures`), issued PDFs (`myota-certificates`), and ADIF source logs (`myota-adif`). The API chooses an asset bucket from its kind; clients cannot override it. Bucket names are configurable with `MYOTA_AWARD_ASSET_BUCKET`, `MYOTA_AWARD_SIGNATURE_BUCKET`, `MYOTA_CERTIFICATE_BUCKET`, and `MYOTA_ADIF_BUCKET`.
- Upgrade installations from the former shared `myota-awards` bucket with
  `migrate_award_asset_buckets.py` (dry-run first, then `--apply`). It copies,
  verifies and updates metadata without deleting source objects; placeholders
  marked `MISSING` are safely re-pointed without copying. Run with durable
  activity-database and object-store connectivity. Retire the legacy bucket
  only after auditing references and backups.
- Bounded API concurrency, bounded PostgreSQL pools, durable outbox jobs and
  background workers for ADIF, award recalculation, certificate rendering,
  statistics and notifications.

Award administration also supports the Phase 1 `PATCH /v1/awards/{awardId}`
resource for draft edits and lifecycle transitions. The existing submit,
review, publish, and retire action routes remain deprecated compatibility
aliases. See the [Phase 1 API resource update record](https://github.com/myota-platform/myota-docs/blob/main/docs/api-phase1-resource-updates.md).

Phase 3 adds preferred resource/job APIs for activation closure, high-volume
QSO ingestion, correction review, statistics rebuilds, award evaluation and
historical recalculation, issuance, certificate rendering, and artifact
retrieval. Participant mutations are owner-authorized, award recalculation is
version-scoped, and statistics snapshots are replaced deterministically. The
legacy action routes remain deprecated aliases. Phase 4 source changes route
six accepted kinds through the transactional outbox and JetStream; production
cutover is still pending. See the [Phase 3 API record](https://github.com/myota-platform/myota-docs/blob/main/docs/api-phase3-activity-award-jobs.md)
and [Phase 4 work queue record](https://github.com/myota-platform/myota-docs/blob/main/docs/operations/messaging/activity-work-queues.md).

The unit-test adapter remains in-memory for fast contract tests. When
`ACTIVITY_DATABASE_URL` is configured, `activity_repository.py` uses only the
activity-owned relational schema and never rewrites a service-wide JSON state
snapshot.

## Test and run locally

Activation/QSO times, certificate dates, and award draft/publication effective
dates use UTC. Offset-bearing effective dates normalize to the same UTC instant;
legacy unqualified times mean UTC. See the [UTC policy](https://github.com/myota-platform/myota-docs/blob/main/docs/utc-time-policy.md).

Certificate design supports authenticated raw PNG/JPEG `PUT` and content `GET`
at `/v1/awards/assets/{assetId}/content`. Image bytes/dimensions are checked
(20 MiB, 16 million pixels); kind determines the storage bucket. Transient
`POST /v1/awards/previews` renders mock-data PDFs without issuance/storage side
effects, bounded to 64 KiB requests, 30 elements, 150–300 DPI and one render per
pod. `certificate_design.py` shares orientation-aware A4/Letter rendering with
issuance. Drafts preserve signature/manager metadata and custom template text.
See the [designer API and runbook](https://github.com/myota-platform/myota-docs/blob/main/docs/programme-and-award-design.md).
The platform/deploy runtime copies must mirror these owner modules and the
request-body hook; do not replace their unrelated shared HTTP infrastructure.

```bash
python3 -m pip install -r requirements.txt
python3 -m unittest discover -s tests -v
python3 run_activity.py
```

Open <http://127.0.0.1:8004/healthz> for the activity service. Activations and awards intentionally share this port; the gateway exposes the same paths without a second awards service.

The service exposes a real-data `/metrics` endpoint with QSO, activation,
participant, award-progress, import, worker, queue-lag and correction gauges.
When `MYOTA_OTEL_ENABLED=1`, request metrics and traces are exported to the
OpenTelemetry Collector configured by `OTEL_EXPORTER_OTLP_ENDPOINT`.

For the durable local environment, start Colima and use the Compose stack in
`myota-deploy`. It runs the API on port 8004, activity workers, the notification
consumer, the daily ADIF source-retention worker, the dedicated plain-PostgreSQL
`myota_activity` database, NATS and SeaweedFS. Geodata/PostGIS and core
PostgreSQL are separate containers owned by the deployment. Helm deploys three
stateless activity API replicas by default and separately scales workers.

## Activity migrations

The ordered activity schema migrations in `migrations/` are service-owned and
must be synchronized byte-for-byte to `myota-deploy/db/migrations/activity/`;
the deployment runner applies them in numeric order. Migration `003` adds a
retry marker for deleted ADIF sources; migration `004` indexes both completed
and failed imports eligible for cleanup. A failed object-store deletion stays
eligible for retry without deleting the durable import result.

## Architecture

Read the authoritative [architecture](https://github.com/myota-platform/myota-docs/blob/main/docs/architecture.md)
and [repository map](https://github.com/myota-platform/myota-docs/blob/main/docs/repository-map.md).
The geodata deletion worker calls activity APIs for impact, QSO cascade and
award recalculation before removing an entity; activity data stays in
`myota_activity`, never the geo database. See the
[deletion and scaling delivery record](https://github.com/myota-platform/myota-docs/blob/main/docs/geodata-horizontal-scaling-roadmap.md#latest-delivery-and-evidence--7-october-2026).

## Source project

The original `ea7klk/mpota` repository remains untouched; see the
[migration strategy](https://github.com/myota-platform/myota-docs/blob/main/docs/migration-from-mpota.md).
