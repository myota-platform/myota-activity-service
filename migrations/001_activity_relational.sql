-- Activity service owns these tables.  The generic service_state table remains
-- available to older services, but activity data is never persisted there.
CREATE TABLE IF NOT EXISTS activity_activation (
  id uuid PRIMARY KEY,
  programme_slug text NOT NULL,
  entity_id text NOT NULL,
  entity_type text,
  jurisdiction_code text,
  operator_id text NOT NULL,
  operator_callsign citext,
  started_at timestamptz NOT NULL,
  ended_at timestamptz,
  validity_expires_at timestamptz,
  status text NOT NULL DEFAULT 'OPEN' CHECK (status IN ('OPEN','CLOSED','CLOSED_INVALID','CANCELLED')),
  location jsonb NOT NULL DEFAULT '{}'::jsonb,
  programme_rules jsonb NOT NULL DEFAULT '{}'::jsonb,
  rule_evaluation jsonb NOT NULL DEFAULT '{}'::jsonb,
  qso_count integer NOT NULL DEFAULT 0,
  unique_callsign_count integer NOT NULL DEFAULT 0,
  unique_entity_count integer NOT NULL DEFAULT 0,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now()
);
ALTER TABLE activity_activation ADD COLUMN IF NOT EXISTS jurisdiction_code text;
CREATE INDEX IF NOT EXISTS activity_activation_programme_idx ON activity_activation (programme_slug, status, started_at DESC);
CREATE INDEX IF NOT EXISTS activity_activation_operator_idx ON activity_activation (operator_id, started_at DESC);
CREATE INDEX IF NOT EXISTS activity_activation_entity_idx ON activity_activation (entity_id, started_at DESC);
CREATE INDEX IF NOT EXISTS activity_activation_jurisdiction_idx ON activity_activation (programme_slug, jurisdiction_code, started_at DESC);
CREATE INDEX IF NOT EXISTS activity_activation_date_idx ON activity_activation (started_at DESC);

CREATE TABLE IF NOT EXISTS activity_qso (
  id uuid PRIMARY KEY,
  activation_id uuid NOT NULL REFERENCES activity_activation(id),
  programme_slug text NOT NULL,
  operator_id text NOT NULL,
  operator_callsign citext,
  hunter_id text,
  hunter_callsign citext,
  worked_callsign citext NOT NULL,
  worked_station_key text NOT NULL,
  worked_entity_id text,
  occurred_at timestamptz NOT NULL,
  band text,
  mode text,
  rst text,
  source text NOT NULL DEFAULT 'manual',
  source_import_id uuid,
  deduplication_key text NOT NULL UNIQUE,
  status text NOT NULL DEFAULT 'VALID' CHECK (status IN ('VALID','CORRECTED','VOID')),
  raw_payload jsonb NOT NULL DEFAULT '{}'::jsonb,
  created_at timestamptz NOT NULL DEFAULT now(),
  corrected_at timestamptz
);
CREATE INDEX IF NOT EXISTS activity_qso_programme_time_idx ON activity_qso (programme_slug, occurred_at DESC);
CREATE INDEX IF NOT EXISTS activity_qso_operator_time_idx ON activity_qso (operator_id, occurred_at DESC);
CREATE INDEX IF NOT EXISTS activity_qso_hunter_time_idx ON activity_qso (hunter_id, occurred_at DESC);
CREATE INDEX IF NOT EXISTS activity_qso_callsign_time_idx ON activity_qso (worked_callsign, occurred_at DESC);
CREATE INDEX IF NOT EXISTS activity_qso_entity_time_idx ON activity_qso (worked_entity_id, occurred_at DESC);
CREATE INDEX IF NOT EXISTS activity_qso_activation_time_idx ON activity_qso (activation_id, occurred_at DESC);
CREATE INDEX IF NOT EXISTS activity_qso_time_brin_idx ON activity_qso USING BRIN (occurred_at);

CREATE TABLE IF NOT EXISTS activity_subject_aggregate (
  programme_slug text NOT NULL,
  subject_id text NOT NULL,
  category text NOT NULL CHECK (category IN ('ACTIVATOR','HUNTER')),
  qso_count bigint NOT NULL DEFAULT 0,
  activation_count bigint NOT NULL DEFAULT 0,
  unique_callsign_count bigint NOT NULL DEFAULT 0,
  unique_entity_count bigint NOT NULL DEFAULT 0,
  last_entity_type text,
  updated_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (programme_slug, subject_id, category)
);
CREATE INDEX IF NOT EXISTS activity_subject_aggregate_qso_idx ON activity_subject_aggregate (programme_slug, category, qso_count DESC);

CREATE TABLE IF NOT EXISTS activity_subject_callsign (
  programme_slug text NOT NULL,
  subject_id text NOT NULL,
  category text NOT NULL,
  callsign citext NOT NULL,
  PRIMARY KEY (programme_slug, subject_id, category, callsign)
);
CREATE TABLE IF NOT EXISTS activity_subject_entity (
  programme_slug text NOT NULL,
  subject_id text NOT NULL,
  category text NOT NULL,
  entity_id text NOT NULL,
  PRIMARY KEY (programme_slug, subject_id, category, entity_id)
);

CREATE TABLE IF NOT EXISTS activity_award_definition (
  id uuid PRIMARY KEY,
  programme_slug text NOT NULL,
  code text NOT NULL,
  version integer NOT NULL DEFAULT 1,
  status text NOT NULL DEFAULT 'DRAFT',
  effective_from timestamptz,
  retired_at timestamptz,
  definition jsonb NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  UNIQUE (programme_slug, code, version)
);
CREATE INDEX IF NOT EXISTS activity_award_public_idx ON activity_award_definition (programme_slug, status, effective_from);

CREATE TABLE IF NOT EXISTS activity_asset (
  id uuid PRIMARY KEY,
  kind text NOT NULL CHECK (kind IN ('BACKGROUND','SIGNATURE')),
  object_key text NOT NULL,
  bucket text NOT NULL,
  media_type text NOT NULL,
  width_px integer NOT NULL,
  height_px integer NOT NULL,
  content_status text NOT NULL DEFAULT 'MISSING',
  metadata jsonb NOT NULL DEFAULT '{}'::jsonb,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS activity_award_request (
  id uuid PRIMARY KEY,
  award_id uuid NOT NULL REFERENCES activity_award_definition(id),
  programme_slug text NOT NULL,
  level_id text NOT NULL,
  subject_id text NOT NULL,
  category text NOT NULL,
  callsign text NOT NULL,
  person_name text NOT NULL,
  facts jsonb NOT NULL DEFAULT '{}'::jsonb,
  status text NOT NULL DEFAULT 'REQUESTED',
  issued_award_id uuid,
  requested_at timestamptz NOT NULL DEFAULT now(),
  issued_at timestamptz,
  UNIQUE (award_id, level_id, subject_id)
);
CREATE INDEX IF NOT EXISTS activity_award_request_subject_idx ON activity_award_request (subject_id, status, requested_at DESC);

CREATE TABLE IF NOT EXISTS activity_award_issuance (
  id uuid PRIMARY KEY,
  request_id uuid NOT NULL REFERENCES activity_award_request(id),
  award_id uuid NOT NULL REFERENCES activity_award_definition(id),
  programme_slug text NOT NULL,
  level_id text NOT NULL,
  subject_id text NOT NULL,
  issuance jsonb NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS activity_award_issuance_subject_idx ON activity_award_issuance (subject_id, created_at DESC);

CREATE TABLE IF NOT EXISTS activity_award_progress (
  award_id uuid NOT NULL REFERENCES activity_award_definition(id),
  award_version integer NOT NULL,
  subject_id text NOT NULL,
  category text NOT NULL,
  facts jsonb NOT NULL DEFAULT '{}'::jsonb,
  evaluation jsonb NOT NULL DEFAULT '{}'::jsonb,
  computed_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (award_id, award_version, subject_id)
);

CREATE TABLE IF NOT EXISTS activity_import (
  id uuid PRIMARY KEY,
  activation_id uuid NOT NULL REFERENCES activity_activation(id),
  filename text NOT NULL,
  object_key text NOT NULL,
  bucket text NOT NULL,
  sha256 text NOT NULL,
  content_size bigint NOT NULL,
  malware_status text NOT NULL DEFAULT 'PENDING',
  status text NOT NULL DEFAULT 'QUEUED',
  records_seen integer NOT NULL DEFAULT 0,
  records_accepted integer NOT NULL DEFAULT 0,
  records_rejected integer NOT NULL DEFAULT 0,
  errors jsonb NOT NULL DEFAULT '[]'::jsonb,
  created_at timestamptz NOT NULL DEFAULT now(),
  started_at timestamptz,
  completed_at timestamptz
);
CREATE INDEX IF NOT EXISTS activity_import_status_idx ON activity_import (status, created_at);

CREATE TABLE IF NOT EXISTS activity_qso_correction (
  id uuid PRIMARY KEY,
  qso_id uuid NOT NULL REFERENCES activity_qso(id),
  requested_by text NOT NULL,
  reason text NOT NULL,
  proposed_values jsonb NOT NULL,
  status text NOT NULL DEFAULT 'PENDING' CHECK (status IN ('PENDING','APPLIED','REJECTED')),
  reviewed_by text,
  review_note text,
  created_at timestamptz NOT NULL DEFAULT now(),
  reviewed_at timestamptz
);
CREATE INDEX IF NOT EXISTS activity_qso_correction_status_idx ON activity_qso_correction (status, created_at);

CREATE TABLE IF NOT EXISTS activity_job (
  id uuid PRIMARY KEY,
  kind text NOT NULL,
  payload jsonb NOT NULL,
  status text NOT NULL DEFAULT 'QUEUED' CHECK (status IN ('QUEUED','RUNNING','SUCCEEDED','FAILED')),
  attempts integer NOT NULL DEFAULT 0,
  idempotency_key text UNIQUE,
  available_at timestamptz NOT NULL DEFAULT now(),
  started_at timestamptz,
  completed_at timestamptz,
  last_error text
);
CREATE INDEX IF NOT EXISTS activity_job_claim_idx ON activity_job (status, available_at, id);

CREATE TABLE IF NOT EXISTS activity_statistic_snapshot (
  id uuid PRIMARY KEY,
  programme_slug text NOT NULL,
  statistic_type text NOT NULL,
  scope_key text NOT NULL,
  period_start timestamptz,
  period_end timestamptz,
  metric_values jsonb NOT NULL,
  algorithm_version text NOT NULL,
  generated_at timestamptz NOT NULL DEFAULT now(),
  UNIQUE (programme_slug, statistic_type, scope_key, period_start, period_end, algorithm_version)
);

CREATE TABLE IF NOT EXISTS activity_notification (
  id uuid PRIMARY KEY,
  recipient_id text NOT NULL,
  notification_type text NOT NULL,
  payload jsonb NOT NULL DEFAULT '{}'::jsonb,
  status text NOT NULL DEFAULT 'QUEUED',
  created_at timestamptz NOT NULL DEFAULT now(),
  delivered_at timestamptz
);
ALTER TABLE activity_notification ADD COLUMN IF NOT EXISTS deduplication_key text;
CREATE UNIQUE INDEX IF NOT EXISTS activity_notification_dedupe_idx ON activity_notification (deduplication_key) WHERE deduplication_key IS NOT NULL;
CREATE INDEX IF NOT EXISTS activity_notification_recipient_idx ON activity_notification (recipient_id, status, created_at DESC);
