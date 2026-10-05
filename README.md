# MyOTA Outdoor Activation Platform

MyOTA is a programme-agnostic platform for outdoor activation programmes. MPOTA is represented as a configured programme, not as the platform itself. No rules or charter text are copied from POTA or any other programme: every programme supplies its own configuration, policy, eligibility, awards and public charter.

This repository owns the activity bounded context: activations, normalized QSOs,
ADIF ingestion, programme-owned awards, progress, certificates and execution
statistics. Activity and awards intentionally share one API process and port
(`8004`). Deployment and migration orchestration live in `myota-deploy`; the
versioned activity migration in `migrations/` is copied and applied there.

The platform purpose and the distinction between reusable activity capability
and programme-owned policy are documented in the
[MyOTA charter](https://github.com/myota-platform/myota-docs/blob/main/docs/project-charter.md).
The current production-readiness and participant-experience gaps are tracked
in the [charter gap analysis](https://github.com/myota-platform/myota-docs/blob/main/docs/charter-gap-analysis.md).

## What works now

- Amateur-radio-aware identity: operator/SWL participation, multiple callsigns, one primary callsign, lifecycle and verification fields.
- Shared entity-category catalogue and programme assignments; activity owns programme execution, QSO rules, minimum QSOs, awards, themes and optional OIDC settings.
- Geodata lifecycle: imported candidate → community proposal → approver review → approved entity.
- Provenance-aware imports with adapter metadata for ParkServe, OSM, government GIS and manual proposals.
- Relational, indexed activation/QSO storage with deduplication, idempotency,
  aggregate facts and PostgreSQL `COPY` batch ingestion; the generic JSONB
  state store is not used in durable activity mode.
- The activity migration creates local state, idempotency, outbox,
  consumer-checkpoint and dead-letter tables inside `myota_activity`; these
  infrastructure tables are not read from `myota_core`.
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
- Universal themed frontend with verified/candidate map distinction.
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
retrieval. Jobs use the relational `activity_job` queue, bounded retries, and
the existing background worker; participant mutations are owner-authorized,
award recalculation is version-scoped, and statistics snapshots are replaced
deterministically. The legacy action routes remain deprecated aliases. See the
[Phase 3 activity and award job record](https://github.com/myota-platform/myota-docs/blob/main/docs/api-phase3-activity-award-jobs.md).

The unit-test adapter remains in-memory for fast contract tests. When
`ACTIVITY_DATABASE_URL` is configured, `activity_repository.py` uses only the
activity-owned relational schema and never rewrites a service-wide JSON state
snapshot.

## Run the vertical slice

```bash
python3 -m unittest discover -s tests -v
python3 services/dev_server.py
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

Read [`docs/architecture.md`](docs/architecture.md), [`docs/adr/0001-storage-topology.md`](docs/adr/0001-storage-topology.md), and [`docs/repository-map.md`](docs/repository-map.md). The current bootstrap is kept together to make the vertical slice easy to run; the repository map defines the justified GitHub split once the MyOTA organization is available.

## Source project

The original `ea7klk/mpota` repository remains untouched. Its charter and planned flows are treated as the migration source; see [`docs/migration-from-mpota.md`](docs/migration-from-mpota.md).
