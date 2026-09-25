# MyOTA Outdoor Activation Platform

MyOTA is a programme-agnostic platform for outdoor activation programmes. MPOTA is represented as a configured programme, not as the platform itself. No rules or charter text are copied from POTA or any other programme: every programme supplies its own configuration, policy, eligibility, awards and public charter.

This repository owns the activity bounded context: activations, normalized QSOs,
ADIF ingestion, programme-owned awards, progress, certificates and execution
statistics. Activity and awards intentionally share one API process and port
(`8004`). Deployment and migration orchestration live in `myota-deploy`; the
versioned activity migration in `migrations/` is copied and applied there.

## What works now

- Amateur-radio-aware identity: operator/SWL participation, multiple callsigns, one primary callsign, lifecycle and verification fields.
- Shared entity-category catalogue and programme assignments; activity owns programme execution, QSO rules, minimum QSOs, awards, themes and optional OIDC settings.
- Geodata lifecycle: imported candidate → community proposal → approver review → approved entity.
- Provenance-aware imports with adapter metadata for ParkServe, OSM, government GIS and manual proposals.
- Relational, indexed activation/QSO storage with deduplication, idempotency,
  aggregate facts and PostgreSQL `COPY` batch ingestion; the generic JSONB
  state store is not used in durable activity mode.
- Activation validity windows, location/rule checks, verified callsign inputs,
  normalization, band/mode validation, correction workflows and close-time
  programme rule evaluation.
- ADIF upload safety gate, S3/MinIO object storage, asynchronous parsing and
  import result tracking.
- Programme-owned hunter/activator awards, nested conditions, levels, server-side progress, MinIO/S3-backed assets, certificate rendering, participant requests and permanent issuance records are exposed by the same service on port 8004 under `/v1/awards`.
- Universal themed frontend with verified/candidate map distinction.
- Bounded API concurrency, bounded PostgreSQL pools, durable outbox jobs and
  background workers for ADIF, award recalculation, certificate rendering,
  statistics and notifications.

The unit-test adapter remains in-memory for fast contract tests. When
`CORE_DATABASE_URL` is configured, `activity_repository.py` uses only the
activity-owned relational schema and never rewrites a service-wide JSON state
snapshot.

## Run the vertical slice

```bash
python3 -m unittest discover -s tests -v
python3 services/dev_server.py
```

Open <http://127.0.0.1:8004/healthz> for the activity service. Activations and awards intentionally share this port; the gateway exposes the same paths without a second awards service.

For a containerized PostGIS environment, start Colima and use the Compose stack
in `myota-deploy`. It runs the API on port 8004, activity workers, the
notification consumer, PostgreSQL/PostGIS, NATS and MinIO. Helm deploys three
stateless activity API replicas by default and separately scales workers.

## Architecture

Read [`docs/architecture.md`](docs/architecture.md), [`docs/adr/0001-storage-topology.md`](docs/adr/0001-storage-topology.md), and [`docs/repository-map.md`](docs/repository-map.md). The current bootstrap is kept together to make the vertical slice easy to run; the repository map defines the justified GitHub split once the MyOTA organization is available.

## Source project

The original `ea7klk/mpota` repository remains untouched. Its charter and planned flows are treated as the migration source; see [`docs/migration-from-mpota.md`](docs/migration-from-mpota.md).
