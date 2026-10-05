-- ADIF source objects expire after processing, while import results remain in
-- the activity database for participant history and troubleshooting.
ALTER TABLE activity_import
  ADD COLUMN IF NOT EXISTS source_deleted_at timestamptz;

CREATE INDEX IF NOT EXISTS activity_import_adif_retention_idx
  ON activity_import (completed_at, id)
  WHERE status = 'COMPLETED' AND source_deleted_at IS NULL;
