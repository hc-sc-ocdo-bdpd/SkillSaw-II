-- -------------------------------------------------------------------
-- Plans (servers + filepaths)
-- Includes your current and the commented example
-- -------------------------------------------------------------------
INSERT INTO ingestion_plans (server_name, filepath, enabled, notes)
VALUES
  ('APP02/HC-SC/GC/CA', 'csb\\imsd\\hcdir3.nsf', 1, 'HC directory')

ON DUPLICATE KEY UPDATE
  enabled = VALUES(enabled),
  notes   = VALUES(notes);

-- Fetch plan IDs so we can insert plan views
-- (Replace 1/2 below if your auto-increment differs)
-- You can check with: SELECT id, server_name, filepath FROM ingestion_plans;

-- -------------------------------------------------------------------
-- Views for csb\imsd\hcdir3.nsf  (assume id = 1)
-- -------------------------------------------------------------------
INSERT INTO ingestion_plan_views (plan_id, canon_name, priority, enabled)
VALUES
  (1, 'Person By Surname', 10, 1)

ON DUPLICATE KEY UPDATE
  priority = VALUES(priority),
  enabled  = VALUES(enabled);

-- -------------------------------------------------------------------
-- Views for cfob\dpfa\sap\sapaccess.nsf (assume id = 2)
-- -------------------------------------------------------------------
INSERT INTO ingestion_plan_views (plan_id, canon_name, priority, enabled)
VALUES
  (2, 'All Employees HC Export', 10, 1),
  (2, 'GEDS Update M365', 20, 1)
ON DUPLICATE KEY UPDATE
  priority = VALUES(priority),
  enabled  = VALUES(enabled);

-- Optional: a one-off regex override if a database uses a quirky view name:
-- UPDATE ingestion_plan_views
-- SET regex_override = '(?i)^all\\s+hc\\s+empl.*export$'
-- WHERE plan_id = 2 AND canon_name = 'All Employees HC Export';
