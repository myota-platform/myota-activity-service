-- Include terminal failures in the same source-object retention policy. The
-- migration runner reapplies SQL files, so rebuild the existing index only if
-- it does not already cover FAILED rows.
DO $$
DECLARE
  current_predicate text;
BEGIN
  SELECT pg_get_expr(indpred, indrelid)
    INTO current_predicate
    FROM pg_index
   WHERE indexrelid = to_regclass('public.activity_import_adif_retention_idx');

  IF current_predicate IS NULL OR current_predicate NOT ILIKE '%FAILED%' THEN
    DROP INDEX IF EXISTS public.activity_import_adif_retention_idx;
    CREATE INDEX activity_import_adif_retention_idx
      ON activity_import (completed_at, id)
      WHERE status IN ('COMPLETED', 'FAILED') AND source_deleted_at IS NULL;
  END IF;
END $$;
