-- Entity deletion is orchestrated by the global administrator workflow. These
-- indexes make the impact check and QSO cascade bounded by the affected entity
-- instead of requiring a scan of the complete activity history.
CREATE INDEX IF NOT EXISTS activity_qso_worked_entity_valid_idx
  ON activity_qso (worked_entity_id, occurred_at DESC)
  WHERE status <> 'VOID';
CREATE INDEX IF NOT EXISTS activity_qso_activation_valid_idx
  ON activity_qso (activation_id, occurred_at DESC)
  WHERE status <> 'VOID';

COMMENT ON TABLE activity_qso IS
  'Service-owned QSOs. Global entity deletion removes linked valid rows, rebuilds aggregates, and queues award recalculation.';
