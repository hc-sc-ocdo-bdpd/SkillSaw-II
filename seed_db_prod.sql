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

-- ========= Targeted performance indexes =========

-- Plans listing by enabled + ordering by server/path
CREATE INDEX idx_plans_enabled_server_path
  ON ingestion_plans (enabled, server_name, filepath);

-- Views lookup for a plan with enabled filter + ordering by priority, name
CREATE INDEX idx_plan_views_plan_enabled_prio_name
  ON ingestion_plan_views (plan_id, enabled, priority, canon_name);

-- Incremental/doc scans per source ordered by modified_at
CREATE INDEX idx_documents_source_modified
  ON documents (source_id, modified_at);

-- Fast existence checks for item_values as used by _select_existing_item_value(...)
-- (prefix on v_string because it's VARCHAR(1024))
CREATE INDEX idx_item_values_match
  ON item_values (
    item_id, val_kind, attachment_id, v_bool, v_datetime, v_number, v_string(191)
  );

-- Speed SELECT id FROM attachments WHERE unid=? AND filename<=>? AND sha256=?
-- (keeps existing UNIQUE(sha256, unid, filename) for dedup)
CREATE INDEX idx_attachments_unid_sha_file
  ON attachments (unid, sha256, filename);

-- (Optional) If you filter documents by form per source frequently:
-- CREATE INDEX idx_documents_source_form
--   ON documents (source_id, form);

-- (Optional) If you often fetch all values for an item across docs:
-- CREATE INDEX idx_doc_item_values_item
--   ON doc_item_values (item_id);


INSERT INTO notes_extract_norm.items
(id, name, name_lc, notes_filter)
VALUES(1, 'ETI_SUK', 'eti_suk', NULL);
INSERT INTO notes_extract_norm.items
(id, name, name_lc, notes_filter)
VALUES(2, 'ETI_PUK', 'eti_puk', NULL);
INSERT INTO notes_extract_norm.items
(id, name, name_lc, notes_filter)
VALUES(3, 'ETI_DisplayName', 'eti_displayname', NULL);
INSERT INTO notes_extract_norm.items
(id, name, name_lc, notes_filter)
VALUES(4, 'ETI_preferredName', 'eti_preferredname', NULL);
INSERT INTO notes_extract_norm.items
(id, name, name_lc, notes_filter)
VALUES(5, 'ETI_lastName', 'eti_lastname', NULL);
INSERT INTO notes_extract_norm.items
(id, name, name_lc, notes_filter)
VALUES(6, 'ETI_Blackberry', 'eti_blackberry', NULL);
INSERT INTO notes_extract_norm.items
(id, name, name_lc, notes_filter)
VALUES(7, 'ETI_BlackberryOS', 'eti_blackberryos', NULL);
INSERT INTO notes_extract_norm.items
(id, name, name_lc, notes_filter)
VALUES(8, 'Litigation', 'litigation', NULL);
INSERT INTO notes_extract_norm.items
(id, name, name_lc, notes_filter)
VALUES(9, 'M365', 'm365', 1);
INSERT INTO notes_extract_norm.items
(id, name, name_lc, notes_filter)
VALUES(10, 'M365_Address', 'm365_address', 1);
INSERT INTO notes_extract_norm.items
(id, name, name_lc, notes_filter)
VALUES(11, 'EMPLISTID', 'emplistid', NULL);
INSERT INTO notes_extract_norm.items
(id, name, name_lc, notes_filter)
VALUES(12, 'EXISTSINNAB', 'existsinnab', NULL);
INSERT INTO notes_extract_norm.items
(id, name, name_lc, notes_filter)
VALUES(13, 'Key', 'key', NULL);
INSERT INTO notes_extract_norm.items
(id, name, name_lc, notes_filter)
VALUES(14, 'InternetChg', 'internetchg', NULL);
INSERT INTO notes_extract_norm.items
(id, name, name_lc, notes_filter)
VALUES(15, 'ShortName', 'shortname', 1);
INSERT INTO notes_extract_norm.items
(id, name, name_lc, notes_filter)
VALUES(16, 'ORGKEY', 'orgkey', 1);
INSERT INTO notes_extract_norm.items
(id, name, name_lc, notes_filter)
VALUES(17, 'BuildingNo', 'buildingno', 1);
INSERT INTO notes_extract_norm.items
(id, name, name_lc, notes_filter)
VALUES(18, 'MNSGroupCode', 'mnsgroupcode', NULL);
INSERT INTO notes_extract_norm.items
(id, name, name_lc, notes_filter)
VALUES(19, 'tmpFloor', 'tmpfloor', 1);
INSERT INTO notes_extract_norm.items
(id, name, name_lc, notes_filter)
VALUES(20, 'FLevel1Name', 'flevel1name', 1);
INSERT INTO notes_extract_norm.items
(id, name, name_lc, notes_filter)
VALUES(21, 'FLevel2Name', 'flevel2name', 1);
INSERT INTO notes_extract_norm.items
(id, name, name_lc, notes_filter)
VALUES(22, 'FLevel3Name', 'flevel3name', 1);
INSERT INTO notes_extract_norm.items
(id, name, name_lc, notes_filter)
VALUES(23, 'FLevel4Name', 'flevel4name', 1);
INSERT INTO notes_extract_norm.items
(id, name, name_lc, notes_filter)
VALUES(24, 'FLevel5Name', 'flevel5name', 1);
INSERT INTO notes_extract_norm.items
(id, name, name_lc, notes_filter)
VALUES(25, 'FLevel6Name', 'flevel6name', 1);
INSERT INTO notes_extract_norm.items
(id, name, name_lc, notes_filter)
VALUES(26, 'FLevel7Name', 'flevel7name', 1);
INSERT INTO notes_extract_norm.items
(id, name, name_lc, notes_filter)
VALUES(27, 'GUAPIREQUEST', 'guapirequest', NULL);
INSERT INTO notes_extract_norm.items
(id, name, name_lc, notes_filter)
VALUES(28, 'GEDSUPDATE', 'gedsupdate', NULL);
INSERT INTO notes_extract_norm.items
(id, name, name_lc, notes_filter)
VALUES(29, 'VIP', 'vip', 1);
INSERT INTO notes_extract_norm.items
(id, name, name_lc, notes_filter)
VALUES(30, 'addressupdatedate', 'addressupdatedate', NULL);
INSERT INTO notes_extract_norm.items
(id, name, name_lc, notes_filter)
VALUES(31, 'GEDS_DN', 'geds_dn', NULL);
INSERT INTO notes_extract_norm.items
(id, name, name_lc, notes_filter)
VALUES(32, 'Form', 'form', NULL);
INSERT INTO notes_extract_norm.items
(id, name, name_lc, notes_filter)
VALUES(33, '$ConflictAction', '$conflictaction', NULL);
INSERT INTO notes_extract_norm.items
(id, name, name_lc, notes_filter)
VALUES(34, 'EForm', 'eform', NULL);
INSERT INTO notes_extract_norm.items
(id, name, name_lc, notes_filter)
VALUES(35, 'FForm', 'fform', NULL);
INSERT INTO notes_extract_norm.items
(id, name, name_lc, notes_filter)
VALUES(36, 'Availability', 'availability', NULL);
INSERT INTO notes_extract_norm.items
(id, name, name_lc, notes_filter)
VALUES(37, 'ReturnDate', 'returndate', NULL);
INSERT INTO notes_extract_norm.items
(id, name, name_lc, notes_filter)
VALUES(38, 'LinkID', 'linkid', NULL);
INSERT INTO notes_extract_norm.items
(id, name, name_lc, notes_filter)
VALUES(39, 'CurParentId', 'curparentid', NULL);
INSERT INTO notes_extract_norm.items
(id, name, name_lc, notes_filter)
VALUES(40, 'NewParentId', 'newparentid', NULL);
INSERT INTO notes_extract_norm.items
(id, name, name_lc, notes_filter)
VALUES(41, 'PublicityPreference', 'publicitypreference', NULL);
INSERT INTO notes_extract_norm.items
(id, name, name_lc, notes_filter)
VALUES(42, 'LanguagePreference_input', 'languagepreference_input', NULL);
INSERT INTO notes_extract_norm.items
(id, name, name_lc, notes_filter)
VALUES(43, 'Manager', 'manager', 1);
INSERT INTO notes_extract_norm.items
(id, name, name_lc, notes_filter)
VALUES(44, 'EADisplay', 'eadisplay', 1);
INSERT INTO notes_extract_norm.items
(id, name, name_lc, notes_filter)
VALUES(45, 'BuildingF', 'buildingf', 1);
INSERT INTO notes_extract_norm.items
(id, name, name_lc, notes_filter)
VALUES(46, 'ADDRESS', 'address', 1);
INSERT INTO notes_extract_norm.items
(id, name, name_lc, notes_filter)
VALUES(47, 'ADDRESSF', 'addressf', 1);
INSERT INTO notes_extract_norm.items
(id, name, name_lc, notes_filter)
VALUES(48, 'City', 'city', 1);
INSERT INTO notes_extract_norm.items
(id, name, name_lc, notes_filter)
VALUES(49, 'CITYF', 'cityf', NULL);
INSERT INTO notes_extract_norm.items
(id, name, name_lc, notes_filter)
VALUES(50, 'TeleWorkCity', 'teleworkcity', 1);
INSERT INTO notes_extract_norm.items
(id, name, name_lc, notes_filter)
VALUES(51, 'Province', 'province', 1);
INSERT INTO notes_extract_norm.items
(id, name, name_lc, notes_filter)
VALUES(52, 'PROVINCEF', 'provincef', 1);
INSERT INTO notes_extract_norm.items
(id, name, name_lc, notes_filter)
VALUES(53, 'Country', 'country', 1);
INSERT INTO notes_extract_norm.items
(id, name, name_lc, notes_filter)
VALUES(54, 'COUNTRYF', 'countryf', 1);
INSERT INTO notes_extract_norm.items
(id, name, name_lc, notes_filter)
VALUES(55, 'POBOX', 'pobox', 1);
INSERT INTO notes_extract_norm.items
(id, name, name_lc, notes_filter)
VALUES(56, 'POBOXF', 'poboxf', 1);
INSERT INTO notes_extract_norm.items
(id, name, name_lc, notes_filter)
VALUES(57, 'AddressChangeFlag', 'addresschangeflag', 1);
INSERT INTO notes_extract_norm.items
(id, name, name_lc, notes_filter)
VALUES(58, 'BuildingF_Alt', 'buildingf_alt', 1);
INSERT INTO notes_extract_norm.items
(id, name, name_lc, notes_filter)
VALUES(59, 'ADDRESS_Alt', 'address_alt', 1);
INSERT INTO notes_extract_norm.items
(id, name, name_lc, notes_filter)
VALUES(60, 'ADDRESSF_Alt', 'addressf_alt', 1);
INSERT INTO notes_extract_norm.items
(id, name, name_lc, notes_filter)
VALUES(61, 'CITY_Alt', 'city_alt', 1);
INSERT INTO notes_extract_norm.items
(id, name, name_lc, notes_filter)
VALUES(62, 'CITYF_Alt', 'cityf_alt', 1);
INSERT INTO notes_extract_norm.items
(id, name, name_lc, notes_filter)
VALUES(63, 'TeleWorkCity_Alt', 'teleworkcity_alt', 1);
INSERT INTO notes_extract_norm.items
(id, name, name_lc, notes_filter)
VALUES(64, 'Province_Alt', 'province_alt', 1);
INSERT INTO notes_extract_norm.items
(id, name, name_lc, notes_filter)
VALUES(65, 'ProvinceF_Alt', 'provincef_alt', 1);
INSERT INTO notes_extract_norm.items
(id, name, name_lc, notes_filter)
VALUES(66, 'Country_Alt', 'country_alt', 1);
INSERT INTO notes_extract_norm.items
(id, name, name_lc, notes_filter)
VALUES(67, 'CountryF_Alt', 'countryf_alt', 1);
INSERT INTO notes_extract_norm.items
(id, name, name_lc, notes_filter)
VALUES(68, 'PostalCode_Alt', 'postalcode_alt', 1);
INSERT INTO notes_extract_norm.items
(id, name, name_lc, notes_filter)
VALUES(69, 'POBOX_Alt', 'pobox_alt', 1);
INSERT INTO notes_extract_norm.items
(id, name, name_lc, notes_filter)
VALUES(70, 'POBOXF_Alt', 'poboxf_alt', 1);
INSERT INTO notes_extract_norm.items
(id, name, name_lc, notes_filter)
VALUES(71, 'AddressChangeFlag_Alt', 'addresschangeflag_alt', 1);
INSERT INTO notes_extract_norm.items
(id, name, name_lc, notes_filter)
VALUES(72, 'EADisplay_Alt', 'eadisplay_alt', NULL);
INSERT INTO notes_extract_norm.items
(id, name, name_lc, notes_filter)
VALUES(73, 'ELevel0', 'elevel0', 1);
INSERT INTO notes_extract_norm.items
(id, name, name_lc, notes_filter)
VALUES(74, 'ELevel0Name', 'elevel0name', 1);
INSERT INTO notes_extract_norm.items
(id, name, name_lc, notes_filter)
VALUES(75, 'FLevel0', 'flevel0', 1);
INSERT INTO notes_extract_norm.items
(id, name, name_lc, notes_filter)
VALUES(76, 'FLevel0Name', 'flevel0name', 1);
INSERT INTO notes_extract_norm.items
(id, name, name_lc, notes_filter)
VALUES(77, 'SecurityHide', 'securityhide', NULL);
INSERT INTO notes_extract_norm.items
(id, name, name_lc, notes_filter)
VALUES(78, 'WExp', 'wexp', NULL);
INSERT INTO notes_extract_norm.items
(id, name, name_lc, notes_filter)
VALUES(79, 'WDate', 'wdate', NULL);
INSERT INTO notes_extract_norm.items
(id, name, name_lc, notes_filter)
VALUES(80, 'YExp', 'yexp', NULL);
INSERT INTO notes_extract_norm.items
(id, name, name_lc, notes_filter)
VALUES(81, 'YDate', 'ydate', NULL);
INSERT INTO notes_extract_norm.items
(id, name, name_lc, notes_filter)
VALUES(82, 'R', 'r', NULL);
INSERT INTO notes_extract_norm.items
(id, name, name_lc, notes_filter)
VALUES(83, 'EmailNamed', 'emailnamed', 1);
INSERT INTO notes_extract_norm.items
(id, name, name_lc, notes_filter)
VALUES(84, 'x400Address', 'x400address', 1);
INSERT INTO notes_extract_norm.items
(id, name, name_lc, notes_filter)
VALUES(85, 'EmailAdditionalAddress', 'emailadditionaladdress', 1);
INSERT INTO notes_extract_norm.items
(id, name, name_lc, notes_filter)
VALUES(86, 'ETI_Address', 'eti_address', 1);
INSERT INTO notes_extract_norm.items
(id, name, name_lc, notes_filter)
VALUES(87, 'Owner', 'owner', NULL);
INSERT INTO notes_extract_norm.items
(id, name, name_lc, notes_filter)
VALUES(88, 'NewPerson', 'newperson', 1);
INSERT INTO notes_extract_norm.items
(id, name, name_lc, notes_filter)
VALUES(89, 'BuildingUNID', 'buildingunid', 1);
INSERT INTO notes_extract_norm.items
(id, name, name_lc, notes_filter)
VALUES(90, 'NonEmailFlag', 'nonemailflag', NULL);
INSERT INTO notes_extract_norm.items
(id, name, name_lc, notes_filter)
VALUES(91, 'EMPUNIQUE', 'empunique', NULL);
INSERT INTO notes_extract_norm.items
(id, name, name_lc, notes_filter)
VALUES(92, 'Status', 'status', NULL);
INSERT INTO notes_extract_norm.items
(id, name, name_lc, notes_filter)
VALUES(93, 'Transfer', 'transfer', NULL);
INSERT INTO notes_extract_norm.items
(id, name, name_lc, notes_filter)
VALUES(94, 'Override1stName', 'override1stname', NULL);
INSERT INTO notes_extract_norm.items
(id, name, name_lc, notes_filter)
VALUES(95, 'DeleteFromGEDS', 'deletefromgeds', NULL);
INSERT INTO notes_extract_norm.items
(id, name, name_lc, notes_filter)
VALUES(96, 'RequiresUpdate', 'requiresupdate', NULL);
INSERT INTO notes_extract_norm.items
(id, name, name_lc, notes_filter)
VALUES(97, 'LastModified', 'lastmodified', 1);
INSERT INTO notes_extract_norm.items
(id, name, name_lc, notes_filter)
VALUES(98, 'DocAuthors', 'docauthors', NULL);
INSERT INTO notes_extract_norm.items
(id, name, name_lc, notes_filter)
VALUES(99, 'docStatus', 'docstatus', NULL);
INSERT INTO notes_extract_norm.items
(id, name, name_lc, notes_filter)
VALUES(100, 'SuperUser', 'superuser', 1);
INSERT INTO notes_extract_norm.items
(id, name, name_lc, notes_filter)
VALUES(101, 'Untitled_Section', 'untitled_section', NULL);
INSERT INTO notes_extract_norm.items
(id, name, name_lc, notes_filter)
VALUES(102, 'Comment', 'comment', NULL);
INSERT INTO notes_extract_norm.items
(id, name, name_lc, notes_filter)
VALUES(103, 'MainId', 'mainid', NULL);
INSERT INTO notes_extract_norm.items
(id, name, name_lc, notes_filter)
VALUES(104, 'ParentFlag', 'parentflag', NULL);
INSERT INTO notes_extract_norm.items
(id, name, name_lc, notes_filter)
VALUES(105, 'Parent', 'parent', NULL);
INSERT INTO notes_extract_norm.items
(id, name, name_lc, notes_filter)
VALUES(106, 'LastModification', 'lastmodification', NULL);
INSERT INTO notes_extract_norm.items
(id, name, name_lc, notes_filter)
VALUES(107, 'FirstName', 'firstname', 1);
INSERT INTO notes_extract_norm.items
(id, name, name_lc, notes_filter)
VALUES(108, 'OPNArea', 'opnarea', 1);
INSERT INTO notes_extract_norm.items
(id, name, name_lc, notes_filter)
VALUES(109, 'OfficePhoneNumber', 'officephonenumber', 1);
INSERT INTO notes_extract_norm.items
(id, name, name_lc, notes_filter)
VALUES(110, 'OfficePhoneNumberExt', 'officephonenumberext', 1);
INSERT INTO notes_extract_norm.items
(id, name, name_lc, notes_filter)
VALUES(111, 'MiddleInitial', 'middleinitial', 1);
INSERT INTO notes_extract_norm.items
(id, name, name_lc, notes_filter)
VALUES(112, 'PNSArea', 'pnsarea', 1);
INSERT INTO notes_extract_norm.items
(id, name, name_lc, notes_filter)
VALUES(113, 'OfficePhoneNumberSecure', 'officephonenumbersecure', 1);
INSERT INTO notes_extract_norm.items
(id, name, name_lc, notes_filter)
VALUES(114, 'OfficePhoneNumberSecExt', 'officephonenumbersecext', 1);
INSERT INTO notes_extract_norm.items
(id, name, name_lc, notes_filter)
VALUES(115, 'LastName', 'lastname', 1);
INSERT INTO notes_extract_norm.items
(id, name, name_lc, notes_filter)
VALUES(116, 'CellArea', 'cellarea', 1);
INSERT INTO notes_extract_norm.items
(id, name, name_lc, notes_filter)
VALUES(117, 'Cellular', 'cellular', 1);
INSERT INTO notes_extract_norm.items
(id, name, name_lc, notes_filter)
VALUES(118, 'CellularExt', 'cellularext', 1);
INSERT INTO notes_extract_norm.items
(id, name, name_lc, notes_filter)
VALUES(119, 'FullName', 'fullname', 1);
INSERT INTO notes_extract_norm.items
(id, name, name_lc, notes_filter)
VALUES(120, 'PagerArea', 'pagerarea', 1);
INSERT INTO notes_extract_norm.items
(id, name, name_lc, notes_filter)
VALUES(121, 'Pager', 'pager', 1);
INSERT INTO notes_extract_norm.items
(id, name, name_lc, notes_filter)
VALUES(122, 'PagerExt', 'pagerext', 1);
INSERT INTO notes_extract_norm.items
(id, name, name_lc, notes_filter)
VALUES(123, 'PREFIXENG', 'prefixeng', 1);
INSERT INTO notes_extract_norm.items
(id, name, name_lc, notes_filter)
VALUES(124, 'PREFIXFRE', 'prefixfre', 1);
INSERT INTO notes_extract_norm.items
(id, name, name_lc, notes_filter)
VALUES(125, 'FaxArea', 'faxarea', 1);
INSERT INTO notes_extract_norm.items
(id, name, name_lc, notes_filter)
VALUES(126, 'OfficeFAXPhoneNumber', 'officefaxphonenumber', 1);
INSERT INTO notes_extract_norm.items
(id, name, name_lc, notes_filter)
VALUES(127, 'SUFFIXENG', 'suffixeng', 1);
INSERT INTO notes_extract_norm.items
(id, name, name_lc, notes_filter)
VALUES(128, 'SUFFIXFRE', 'suffixfre', 1);
INSERT INTO notes_extract_norm.items
(id, name, name_lc, notes_filter)
VALUES(129, 'FaxSArea', 'faxsarea', 1);
INSERT INTO notes_extract_norm.items
(id, name, name_lc, notes_filter)
VALUES(130, 'FaxS', 'faxs', 1);
INSERT INTO notes_extract_norm.items
(id, name, name_lc, notes_filter)
VALUES(131, 'LanguagePreference', 'languagepreference', 1);
INSERT INTO notes_extract_norm.items
(id, name, name_lc, notes_filter)
VALUES(132, 'TDDArea', 'tddarea', 1);
INSERT INTO notes_extract_norm.items
(id, name, name_lc, notes_filter)
VALUES(133, 'TDDNumber', 'tddnumber', 1);
INSERT INTO notes_extract_norm.items
(id, name, name_lc, notes_filter)
VALUES(134, 'EJobTitle', 'ejobtitle', 1);
INSERT INTO notes_extract_norm.items
(id, name, name_lc, notes_filter)
VALUES(135, 'FJobTitle', 'fjobtitle', 1);
INSERT INTO notes_extract_norm.items
(id, name, name_lc, notes_filter)
VALUES(136, 'ExecutiveAssistant', 'executiveassistant', 1);
INSERT INTO notes_extract_norm.items
(id, name, name_lc, notes_filter)
VALUES(137, 'EAPhone', 'eaphone', 1);
INSERT INTO notes_extract_norm.items
(id, name, name_lc, notes_filter)
VALUES(138, 'EExpertise', 'eexpertise', 1);
INSERT INTO notes_extract_norm.items
(id, name, name_lc, notes_filter)
VALUES(139, 'FExpertise', 'fexpertise', 1);
INSERT INTO notes_extract_norm.items
(id, name, name_lc, notes_filter)
VALUES(140, 'BuildingE', 'buildinge', 1);
INSERT INTO notes_extract_norm.items
(id, name, name_lc, notes_filter)
VALUES(141, 'Floor', 'floor', 1);
INSERT INTO notes_extract_norm.items
(id, name, name_lc, notes_filter)
VALUES(142, 'Room', 'room', 1);
INSERT INTO notes_extract_norm.items
(id, name, name_lc, notes_filter)
VALUES(143, 'ValdidatPostalCode', 'valdidatpostalcode', 1);
INSERT INTO notes_extract_norm.items
(id, name, name_lc, notes_filter)
VALUES(144, 'PostalCode', 'postalcode', 1);
INSERT INTO notes_extract_norm.items
(id, name, name_lc, notes_filter)
VALUES(145, 'MAILSTOP', 'mailstop', 1);
INSERT INTO notes_extract_norm.items
(id, name, name_lc, notes_filter)
VALUES(146, 'ALBuilding', 'albuilding', 1);
INSERT INTO notes_extract_norm.items
(id, name, name_lc, notes_filter)
VALUES(147, 'ALFloor', 'alfloor', 1);
INSERT INTO notes_extract_norm.items
(id, name, name_lc, notes_filter)
VALUES(148, 'ALSection', 'alsection', 1);
INSERT INTO notes_extract_norm.items
(id, name, name_lc, notes_filter)
VALUES(149, 'ManSaskRoom', 'mansaskroom', 1);
INSERT INTO notes_extract_norm.items
(id, name, name_lc, notes_filter)
VALUES(150, 'ALManSask', 'almansask', 1);
INSERT INTO notes_extract_norm.items
(id, name, name_lc, notes_filter)
VALUES(151, 'ManSaskFloor', 'mansaskfloor', 1);
INSERT INTO notes_extract_norm.items
(id, name, name_lc, notes_filter)
VALUES(152, 'RegionF', 'regionf', 1);
INSERT INTO notes_extract_norm.items
(id, name, name_lc, notes_filter)
VALUES(153, 'Region', 'region', 1);
INSERT INTO notes_extract_norm.items
(id, name, name_lc, notes_filter)
VALUES(154, 'ReportingRegionF', 'reportingregionf', 1);
INSERT INTO notes_extract_norm.items
(id, name, name_lc, notes_filter)
VALUES(155, 'ReportingRegion', 'reportingregion', 1);
INSERT INTO notes_extract_norm.items
(id, name, name_lc, notes_filter)
VALUES(156, 'BuildingE_Alt', 'buildinge_alt', 1);
INSERT INTO notes_extract_norm.items
(id, name, name_lc, notes_filter)
VALUES(157, 'Floor_Alt', 'floor_alt', 1);
INSERT INTO notes_extract_norm.items
(id, name, name_lc, notes_filter)
VALUES(158, 'Room_Alt', 'room_alt', 1);
INSERT INTO notes_extract_norm.items
(id, name, name_lc, notes_filter)
VALUES(159, 'ValdidatPostalCode_Alt', 'valdidatpostalcode_alt', 1);
INSERT INTO notes_extract_norm.items
(id, name, name_lc, notes_filter)
VALUES(160, 'OPNArea_Alt', 'opnarea_alt', 1);
INSERT INTO notes_extract_norm.items
(id, name, name_lc, notes_filter)
VALUES(161, 'OfficePhoneNumber_Alt', 'officephonenumber_alt', 1);
INSERT INTO notes_extract_norm.items
(id, name, name_lc, notes_filter)
VALUES(162, 'OfficePhoneNumberExt_Alt', 'officephonenumberext_alt', 1);
INSERT INTO notes_extract_norm.items
(id, name, name_lc, notes_filter)
VALUES(163, 'FaxArea_Alt', 'faxarea_alt', 1);
INSERT INTO notes_extract_norm.items
(id, name, name_lc, notes_filter)
VALUES(164, 'OfficeFAXPhoneNumber_Alt', 'officefaxphonenumber_alt', 1);
INSERT INTO notes_extract_norm.items
(id, name, name_lc, notes_filter)
VALUES(165, 'MailStop_Alt', 'mailstop_alt', 1);
INSERT INTO notes_extract_norm.items
(id, name, name_lc, notes_filter)
VALUES(166, 'ALBuilding_Alt', 'albuilding_alt', 1);
INSERT INTO notes_extract_norm.items
(id, name, name_lc, notes_filter)
VALUES(167, 'ALFloor_Alt', 'alfloor_alt', 1);
INSERT INTO notes_extract_norm.items
(id, name, name_lc, notes_filter)
VALUES(168, 'ALSection_Alt', 'alsection_alt', 1);
INSERT INTO notes_extract_norm.items
(id, name, name_lc, notes_filter)
VALUES(169, 'FaxSArea_Alt', 'faxsarea_alt', 1);
INSERT INTO notes_extract_norm.items
(id, name, name_lc, notes_filter)
VALUES(170, 'FaxS_Alt', 'faxs_alt', 1);
INSERT INTO notes_extract_norm.items
(id, name, name_lc, notes_filter)
VALUES(171, 'ALManSask_Alt', 'almansask_alt', 1);
INSERT INTO notes_extract_norm.items
(id, name, name_lc, notes_filter)
VALUES(172, 'ManSaskRoom_Alt', 'mansaskroom_alt', 1);
INSERT INTO notes_extract_norm.items
(id, name, name_lc, notes_filter)
VALUES(173, 'ManSaskFloor_Alt', 'mansaskfloor_alt', 1);
INSERT INTO notes_extract_norm.items
(id, name, name_lc, notes_filter)
VALUES(174, 'RegionF_Alt', 'regionf_alt', 1);
INSERT INTO notes_extract_norm.items
(id, name, name_lc, notes_filter)
VALUES(175, 'Region_Alt', 'region_alt', 1);
INSERT INTO notes_extract_norm.items
(id, name, name_lc, notes_filter)
VALUES(176, 'ReportingRegionF_Alt', 'reportingregionf_alt', 1);
INSERT INTO notes_extract_norm.items
(id, name, name_lc, notes_filter)
VALUES(177, 'ReportingRegion_Alt', 'reportingregion_alt', 1);
INSERT INTO notes_extract_norm.items
(id, name, name_lc, notes_filter)
VALUES(178, 'ExecutiveAssistant_Alt', 'executiveassistant_alt', 1);
INSERT INTO notes_extract_norm.items
(id, name, name_lc, notes_filter)
VALUES(179, 'EAPhone_Alt', 'eaphone_alt', 1);
INSERT INTO notes_extract_norm.items
(id, name, name_lc, notes_filter)
VALUES(180, 'ELevel1Name', 'elevel1name', 1);
INSERT INTO notes_extract_norm.items
(id, name, name_lc, notes_filter)
VALUES(181, 'ELevel1', 'elevel1', 1);
INSERT INTO notes_extract_norm.items
(id, name, name_lc, notes_filter)
VALUES(182, 'FLevel1', 'flevel1', 1);
INSERT INTO notes_extract_norm.items
(id, name, name_lc, notes_filter)
VALUES(183, 'ELevel2Name', 'elevel2name', 1);
INSERT INTO notes_extract_norm.items
(id, name, name_lc, notes_filter)
VALUES(184, 'ELevel2', 'elevel2', 1);
INSERT INTO notes_extract_norm.items
(id, name, name_lc, notes_filter)
VALUES(185, 'FLevel2', 'flevel2', 1);
INSERT INTO notes_extract_norm.items
(id, name, name_lc, notes_filter)
VALUES(186, 'ELevel3Name', 'elevel3name', 1);
INSERT INTO notes_extract_norm.items
(id, name, name_lc, notes_filter)
VALUES(187, 'ELevel3', 'elevel3', 1);
INSERT INTO notes_extract_norm.items
(id, name, name_lc, notes_filter)
VALUES(188, 'FLevel3', 'flevel3', 1);
INSERT INTO notes_extract_norm.items
(id, name, name_lc, notes_filter)
VALUES(189, 'ELevel4Name', 'elevel4name', 1);
INSERT INTO notes_extract_norm.items
(id, name, name_lc, notes_filter)
VALUES(190, 'ELevel4', 'elevel4', 1);
INSERT INTO notes_extract_norm.items
(id, name, name_lc, notes_filter)
VALUES(191, 'FLevel4', 'flevel4', 1);
INSERT INTO notes_extract_norm.items
(id, name, name_lc, notes_filter)
VALUES(192, 'ELevel5Name', 'elevel5name', 1);
INSERT INTO notes_extract_norm.items
(id, name, name_lc, notes_filter)
VALUES(193, 'ELevel5', 'elevel5', 1);
INSERT INTO notes_extract_norm.items
(id, name, name_lc, notes_filter)
VALUES(194, 'FLevel5', 'flevel5', 1);
INSERT INTO notes_extract_norm.items
(id, name, name_lc, notes_filter)
VALUES(195, 'ELevel6Name', 'elevel6name', 1);
INSERT INTO notes_extract_norm.items
(id, name, name_lc, notes_filter)
VALUES(196, 'ELevel6', 'elevel6', 1);
INSERT INTO notes_extract_norm.items
(id, name, name_lc, notes_filter)
VALUES(197, 'FLevel6', 'flevel6', 1);
INSERT INTO notes_extract_norm.items
(id, name, name_lc, notes_filter)
VALUES(198, 'ELevel7Name', 'elevel7name', 1);
INSERT INTO notes_extract_norm.items
(id, name, name_lc, notes_filter)
VALUES(199, 'ELevel7', 'elevel7', 1);
INSERT INTO notes_extract_norm.items
(id, name, name_lc, notes_filter)
VALUES(200, 'FLevel7', 'flevel7', 1);
