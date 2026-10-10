-- Move accepted Activity commands to the transactional outbox. Keep
-- activity_job as the API-visible status/idempotency record; remove only the
-- index and synthetic job rows that existed solely for PostgreSQL polling.
ALTER TABLE activity_job
  ADD COLUMN IF NOT EXISTS created_at timestamptz NOT NULL DEFAULT now(),
  ADD COLUMN IF NOT EXISTS lease_expires_at timestamptz,
  ADD COLUMN IF NOT EXISTS lease_token uuid;

CREATE TABLE IF NOT EXISTS activity_work_dead_letter (
  id uuid PRIMARY KEY,
  work_id uuid,
  work_type text,
  subject text NOT NULL,
  delivery_count integer NOT NULL CHECK (delivery_count > 0),
  error_category text NOT NULL,
  dead_lettered_at timestamptz NOT NULL DEFAULT now(),
  resolved_at timestamptz
);
CREATE INDEX IF NOT EXISTS activity_work_dead_letter_unresolved_idx
  ON activity_work_dead_letter (dead_lettered_at)
  WHERE resolved_at IS NULL;

CREATE TABLE IF NOT EXISTS activity_work_redrive_audit (
  id uuid PRIMARY KEY,
  job_id uuid NOT NULL REFERENCES activity_job(id),
  outbox_event_id uuid NOT NULL,
  actor text NOT NULL CHECK (length(actor) BETWEEN 1 AND 120),
  reason text NOT NULL CHECK (length(reason) BETWEEN 1 AND 500),
  requested_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS activity_work_redrive_job_idx
  ON activity_work_redrive_audit (job_id, requested_at DESC);

DO $$
BEGIN
  -- The replacement status/metrics index is the one-time cutover marker. Once
  -- created, subsequent deployment runs may see legitimate JetStream jobs in
  -- RUNNING state and must not re-run the legacy-drain gate.
  IF to_regclass('public.activity_job_status_kind_idx') IS NULL AND EXISTS (
    SELECT 1 FROM activity_job
    WHERE kind IN (
      'QSO_INGESTION', 'ADIF_IMPORT', 'AWARD_RECALCULATE',
      'AWARD_EVALUATION', 'PDF_RENDER', 'STATISTICS_REBUILD'
    ) AND status = 'RUNNING'
  ) THEN
    RAISE EXCEPTION 'Activity work migration requires all poll-claimed jobs to drain';
  END IF;
END
$$;

-- Translate only unfinished legacy work. The job UUID remains the stable
-- workId; available_at preserves any existing retry delay. The relay publishes
-- these commands once to the dedicated WorkQueue stream.
INSERT INTO outbox_event(
  event_id, event_type, producer, aggregate_type, aggregate_id,
  payload, occurred_at, available_at
)
SELECT
  id,
  CASE kind
    WHEN 'QSO_INGESTION' THEN 'activity.qso-ingestion.v1'
    WHEN 'ADIF_IMPORT' THEN 'activity.adif-import.v1'
    WHEN 'AWARD_RECALCULATE' THEN 'activity.award-recalculate.v1'
    WHEN 'AWARD_EVALUATION' THEN 'activity.award-evaluation.v1'
    WHEN 'PDF_RENDER' THEN 'activity.pdf-render.v1'
    WHEN 'STATISTICS_REBUILD' THEN 'activity.statistics-rebuild.v1'
  END,
  'activity-service', 'activity_job', id::text,
  jsonb_build_object('jobId', id::text), created_at, available_at
FROM activity_job
WHERE status = 'QUEUED'
  AND kind IN (
    'QSO_INGESTION', 'ADIF_IMPORT', 'AWARD_RECALCULATE',
    'AWARD_EVALUATION', 'PDF_RENDER', 'STATISTICS_REBUILD'
  )
ON CONFLICT (event_id) DO NOTHING;

-- Notification rows are already delivered to the in-app database projection;
-- the removed worker made no provider call. Preserve the notification, correct
-- its delivery state, then delete only its internal synthetic job records.
UPDATE activity_notification
SET status = 'DELIVERED', delivered_at = COALESCE(delivered_at, created_at)
WHERE status = 'QUEUED';
ALTER TABLE activity_notification ALTER COLUMN status SET DEFAULT 'DELIVERED';
DELETE FROM activity_job WHERE kind = 'NOTIFICATION_SEND';

DROP INDEX IF EXISTS activity_job_claim_idx;
CREATE INDEX IF NOT EXISTS activity_job_status_kind_idx
  ON activity_job (kind, status, available_at);
