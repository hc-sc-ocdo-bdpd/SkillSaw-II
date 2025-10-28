-- Ensure plans exist (paths use backslashes; double-escape in SQL)
INSERT INTO ingestion_plans (server_name, filepath, enabled, notes)
VALUES
  ('APP02/HC-SC/GC/CA', 'csb\\imsd\\hcdir3.nsf', 1, 'HC directory'),
  ('APP02/HC-SC/GC/CA', 'cfob\\dpfa\\sap\\sapaccess.nsf', 1, 'SAP access')
ON DUPLICATE KEY UPDATE enabled=VALUES(enabled), notes=VALUES(notes);

-- --- HC DIR plan: add canon rows with concrete overrides (exact names seen in your log) ---

-- Person By Surname  →  English / Anglais\2. Employees, alphabetically
INSERT INTO ingestion_plan_views (plan_id, canon_name, priority, enabled, regex_override)
SELECT ip.id, 'Person By Surname', 10, 1, 'English / Anglais\\2. Employees, alphabetically'
FROM ingestion_plans ip
WHERE ip.server_name='APP02/HC-SC/GC/CA' AND ip.filepath='csb\\imsd\\hcdir3.nsf'
ON DUPLICATE KEY UPDATE priority=VALUES(priority), enabled=VALUES(enabled), regex_override=VALUES(regex_override);

-- Person By Organization  →  English / Anglais\1. Employees by Region, by Branch
INSERT INTO ingestion_plan_views (plan_id, canon_name, priority, enabled, regex_override)
SELECT ip.id, 'Person By Organization', 20, 1, 'English / Anglais\\1. Employees by Region, by Branch'
FROM ingestion_plans ip
WHERE ip.server_name='APP02/HC-SC/GC/CA' AND ip.filepath='csb\\imsd\\hcdir3.nsf'
ON DUPLICATE KEY UPDATE priority=VALUES(priority), enabled=VALUES(enabled), regex_override=VALUES(regex_override);

-- Organizational Structure  →  Organization Structure
INSERT INTO ingestion_plan_views (plan_id, canon_name, priority, enabled, regex_override)
SELECT ip.id, 'Organizational Structure', 30, 1, 'Organization Structure'
FROM ingestion_plans ip
WHERE ip.server_name='APP02/HC-SC/GC/CA' AND ip.filepath='csb\\imsd\\hcdir3.nsf'
ON DUPLICATE KEY UPDATE priority=VALUES(priority), enabled=VALUES(enabled), regex_override=VALUES(regex_override);

-- All Employees HC Export  →  All Employees HC Export
INSERT INTO ingestion_plan_views (plan_id, canon_name, priority, enabled, regex_override)
SELECT ip.id, 'All Employees HC Export', 40, 1, 'All Employees HC Export'
FROM ingestion_plans ip
WHERE ip.server_name='APP02/HC-SC/GC/CA' AND ip.filepath='csb\\imsd\\hcdir3.nsf'
ON DUPLICATE KEY UPDATE priority=VALUES(priority), enabled=VALUES(enabled), regex_override=VALUES(regex_override);

-- All Employees PHAC Export  →  All Employees PHAC Export
INSERT INTO ingestion_plan_views (plan_id, canon_name, priority, enabled, regex_override)
SELECT ip.id, 'All Employees PHAC Export', 50, 1, 'All Employees PHAC Export'
FROM ingestion_plans ip
WHERE ip.server_name='APP02/HC-SC/GC/CA' AND ip.filepath='csb\\imsd\\hcdir3.nsf'
ON DUPLICATE KEY UPDATE priority=VALUES(priority), enabled=VALUES(enabled), regex_override=VALUES(regex_override);

-- GEDS Update M365  →  GEDS\UPDATE M365
INSERT INTO ingestion_plan_views (plan_id, canon_name, priority, enabled, regex_override)
SELECT ip.id, 'GEDS Update M365', 60, 1, 'GEDS\\UPDATE M365'
FROM ingestion_plans ip
WHERE ip.server_name='APP02/HC-SC/GC/CA' AND ip.filepath='csb\\imsd\\hcdir3.nsf'
ON DUPLICATE KEY UPDATE priority=VALUES(priority), enabled=VALUES(enabled), regex_override=VALUES(regex_override);

-- (Optional) If you don’t want to touch the SAP DB yet, disable its plan:
-- UPDATE ingestion_plans
-- SET enabled=0
-- WHERE server_name='APP02/HC-SC/GC/CA' AND filepath='cfob\\dpfa\\sap\\sapaccess.nsf';

-- Verify what the loader will read for HC DIR
SELECT ipv.plan_id, ipv.canon_name, ipv.priority, ipv.enabled, ipv.regex_override
FROM ingestion_plan_views ipv
JOIN ingestion_plans ip ON ip.id=ipv.plan_id
WHERE ip.server_name='APP02/HC-SC/GC/CA' AND ip.filepath='csb\\imsd\\hcdir3.nsf'
ORDER BY ipv.priority, ipv.canon_name;
