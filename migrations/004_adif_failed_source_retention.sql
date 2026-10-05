-- Include terminal failures in the same source-object retention policy.
DROP INDEX IF EXISTS activity_import_adif_retention_idx;

CREATE INDEX activity_import_adif_retention_idx
  ON activity_import (completed_at, id)
  WHERE status IN ('COMPLETED', 'FAILED') AND source_deleted_at IS NULL;
