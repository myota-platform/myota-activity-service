-- Preserve JetStream coordinates for reviewed replay of poisoned notifications.
ALTER TABLE dead_letter_event
  ADD COLUMN IF NOT EXISTS stream_name text,
  ADD COLUMN IF NOT EXISTS stream_sequence bigint,
  ADD COLUMN IF NOT EXISTS subject text;

CREATE INDEX IF NOT EXISTS activity_consumer_dead_letter_replay_idx
  ON dead_letter_event (stream_name, stream_sequence)
  WHERE resolved_at IS NULL AND stream_sequence IS NOT NULL;

CREATE TABLE IF NOT EXISTS activity_notification_redrive (
  id uuid PRIMARY KEY,
  event_id uuid NOT NULL REFERENCES dead_letter_event(event_id),
  actor text NOT NULL CHECK (length(actor) BETWEEN 1 AND 120),
  reason text NOT NULL CHECK (length(reason) BETWEEN 1 AND 500),
  status text NOT NULL CHECK (status IN ('PENDING', 'PUBLISHED')),
  requested_at timestamptz NOT NULL DEFAULT now(),
  published_at timestamptz
);

CREATE INDEX IF NOT EXISTS activity_notification_redrive_event_idx
  ON activity_notification_redrive (event_id, requested_at DESC);
