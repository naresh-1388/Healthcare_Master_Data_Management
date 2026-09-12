-- ============================================================
-- Healthcare_Master_Data_Management - Cross-layer audit / reconciliation
--
-- Run these after a pipeline batch completes to confirm row counts are
-- consistent across layers (a sudden drop usually means a DQ rule is
-- rejecting more than expected, or an upstream table is stale).
-- Replace :batch_id with the batch to check.
-- ============================================================

-- 1. Row counts at every layer, per HCP/HCO table
SELECT 'RAW' AS layer, table_name, row_count FROM (
    SELECT 'hcp_name' AS table_name, COUNT(*) AS row_count FROM HMDM_DEV.RAW.hcp_name WHERE "_BATCH_ID" = :batch_id
    UNION ALL
    SELECT 'hco_name', COUNT(*) FROM HMDM_DEV.RAW.hco_name WHERE "_BATCH_ID" = :batch_id
);

-- 2. DQ pass/reject ratio per table for the batch (flags a rule that is
--    rejecting an unusually high share of records)
SELECT
    source_table,
    rule_name,
    passed_count,
    rejected_count,
    ROUND(100.0 * rejected_count / NULLIF(passed_count + rejected_count, 0), 2) AS reject_pct
FROM HMDM_DEV.UTIL.ctl_dqm_log_tbl
WHERE batch_id = :batch_id
ORDER BY reject_pct DESC;

-- 3. Batches stuck mid-pipeline (started but never reached MDM egress)
SELECT batch_id, source_system_name, table_name,
       raw_ingestion_status, stdz_status, canonical_status,
       dq_status, ingress_status, egress_status, created_at
FROM HMDM_DEV.UTIL.ctl_batch_log_tbl
WHERE raw_ingestion_status = 'Y'
  AND egress_status != 'Y'
  AND created_at < DATEADD('hour', -6, CURRENT_TIMESTAMP())
ORDER BY created_at;

-- 4. HCP records present in MDM but missing from every Master egress group
--    (would indicate an egress gap for a subset of mastered HCPs)
SELECT hcp."Source_ID"
FROM HMDM_DEV.MDM.HCP hcp
LEFT JOIN HMDM_DEV.MASTER.HCP master ON hcp."Source_ID" = master."Source_ID"
WHERE master."Source_ID" IS NULL;
