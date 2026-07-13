-- ============================================================================
-- AI-Powered ZeroOps — explicit schema DDL (Unity Catalog layout)
-- ============================================================================
-- Not required to run this project: every table below is also created
-- automatically the first time a notebook writes to it (saveAsTable(), or
-- MERGE INTO against an existing one), and the catalog + schemas + volumes are
-- created by ensure_catalog_and_schemas() the first time 00_setup_environment
-- runs. This script exists so the schema is explicit, versionable, and
-- reviewable in one place, and so you can grant another principal access
-- (see the GRANT section below) without touching a notebook.
--
-- Layout: one Unity Catalog (default name "zeroops") with five schemas:
--   bronze   raw ingested data + a "landing" Volume for input CSVs
--   silver   cleaned/joined/deduped data (+ quarantine, + unmapped columns)
--   gold     business aggregations
--   ops      ZeroOps metadata (incidents, AI analysis/fix, validation results,
--            PR history, notifications, guardrail audit log) + an "artifacts"
--            Volume for the simulated git repo, generated fix code, and PR files
--   sandbox  per-incident validation clones, created dynamically at runtime
--            by 07_sandbox_validation.py (no static table DDL for these --
--            see the note at the bottom of this file)
--
-- Run this once in a SQL Editor / SQL Warehouse, or as a %sql cell in any
-- notebook, before running 01_bronze_ingestion.py for the first time. Safe to
-- re-run: every statement is IF NOT EXISTS.
--
-- If CREATE CATALOG fails with a permissions error, ask a workspace admin for
-- catalog-creation rights, or point the `catalog_name` widget in
-- 00_setup_environment.py at a catalog you already have CREATE SCHEMA rights
-- on, and replace `zeroops` below with that catalog's name.
-- ============================================================================

CREATE CATALOG IF NOT EXISTS zeroops
COMMENT 'AI-Powered ZeroOps for Databricks ETL Pipelines';

CREATE SCHEMA IF NOT EXISTS zeroops.bronze  COMMENT 'Raw ingested data';
CREATE SCHEMA IF NOT EXISTS zeroops.silver  COMMENT 'Cleaned, joined, deduplicated data';
CREATE SCHEMA IF NOT EXISTS zeroops.gold    COMMENT 'Business aggregations';
CREATE SCHEMA IF NOT EXISTS zeroops.ops     COMMENT 'ZeroOps metadata: incidents, AI analysis/fix, validation, PR history, notifications, guardrail log';
CREATE SCHEMA IF NOT EXISTS zeroops.sandbox COMMENT 'Per-incident validation clones (notebook 07), safe to drop/recreate at will';

-- ----------------------------------------------------------------------------
-- Unity Catalog Volumes — file storage (input CSVs, simulated git repo,
-- generated fix code, PR artifacts). Used instead of DBFS FileStore, since
-- Community/Free Edition workspaces increasingly restrict direct dbfs:/ root
-- access; Volumes are governed the same way as tables (GRANT-able, auditable).
-- ----------------------------------------------------------------------------

CREATE VOLUME IF NOT EXISTS zeroops.bronze.landing
COMMENT 'Input CSVs land here: sales_batch1.csv, sales_batch2_bad.csv, store_lookup.csv';

CREATE VOLUME IF NOT EXISTS zeroops.ops.artifacts
COMMENT 'Simulated git repo, generated fix code, PR diffs/descriptions, reference prompts';

-- ----------------------------------------------------------------------------
-- GRANT permissions
-- ----------------------------------------------------------------------------
-- On a single-user Community/Free Edition workspace this section is usually
-- unnecessary -- you own everything you just created. It matters the moment
-- this project is shared with a team or run in a paid workspace where other
-- principals (users, groups, or service principals) need access.
--
-- Volume privileges in Unity Catalog: READ VOLUME (list/read files) and
-- WRITE VOLUME (create/modify/delete files). Like tables, a principal also
-- needs USE CATALOG on the catalog and USE SCHEMA on the schema to even see
-- the volume, in addition to the volume-level grant itself.
--
-- Replace `analyst_group` / `zeroops_pipeline_sp` with a real Databricks
-- account group, user email, or service principal application ID.

-- Read-only access to the input-data volume (e.g. for a BI/analyst group that
-- should be able to inspect raw source files but never modify them):
-- GRANT USE CATALOG   ON CATALOG zeroops                TO `analyst_group`;
-- GRANT USE SCHEMA    ON SCHEMA  zeroops.bronze          TO `analyst_group`;
-- GRANT READ VOLUME   ON VOLUME  zeroops.bronze.landing  TO `analyst_group`;

-- Read/write access to the artifacts volume for a service principal running
-- this pipeline on a schedule (e.g. via Databricks Workflows):
-- GRANT USE CATALOG   ON CATALOG zeroops                  TO `zeroops_pipeline_sp`;
-- GRANT USE SCHEMA    ON SCHEMA  zeroops.ops               TO `zeroops_pipeline_sp`;
-- GRANT READ VOLUME   ON VOLUME  zeroops.ops.artifacts     TO `zeroops_pipeline_sp`;
-- GRANT WRITE VOLUME  ON VOLUME  zeroops.ops.artifacts     TO `zeroops_pipeline_sp`;

-- Table-level equivalent, if that same principal also needs to query results:
-- GRANT USE SCHEMA  ON SCHEMA zeroops.ops TO `zeroops_pipeline_sp`;
-- GRANT SELECT      ON SCHEMA zeroops.ops TO `zeroops_pipeline_sp`;

-- ----------------------------------------------------------------------------
-- bronze schema
-- ----------------------------------------------------------------------------

-- Written with mergeSchema=true by 01, so new upstream columns (e.g.
-- discount_code in sales_batch2_bad.csv) attach here automatically even
-- though they're not listed below.
CREATE TABLE IF NOT EXISTS zeroops.bronze.sales (
    order_id        INT,
    order_date      STRING,
    store_id        INT,
    product_id      STRING,
    product_name    STRING,
    quantity        STRING,       -- kept as STRING in Bronze on purpose: batch2 has "N/A"
    unit_price      DOUBLE,
    customer_id     STRING,
    _ingestion_time TIMESTAMP,
    _source_file    STRING
) USING DELTA
COMMENT 'Raw ingested sales batches, one row per source CSV row';

-- ----------------------------------------------------------------------------
-- silver schema
-- ----------------------------------------------------------------------------

-- Cast, joined with store_lookup, deduplicated on order_id via MERGE INTO.
CREATE TABLE IF NOT EXISTS zeroops.silver.sales (
    order_id        INT,
    order_date      STRING,
    store_id        INT,
    product_id      STRING,
    product_name    STRING,
    quantity        INT,
    unit_price      DOUBLE,
    customer_id     STRING,
    _ingestion_time TIMESTAMP,
    store_code      STRING,
    region          STRING
) USING DELTA
COMMENT 'Cleaned, deduplicated, region-enriched sales data';

-- NOT written automatically by the current notebooks (07's apply_fix() filters
-- bad rows out rather than routing them here) -- this is what 06's generated
-- CAST_INVALID_INPUT fix template targets if a human applies that generated
-- code for real. Created here so the table exists ahead of that manual step.
CREATE TABLE IF NOT EXISTS zeroops.silver.sales_quarantine (
    order_id           INT,
    order_date         STRING,
    store_id           INT,
    product_id         STRING,
    product_name       STRING,
    quantity           STRING,
    unit_price         DOUBLE,
    customer_id        STRING,
    _ingestion_time    TIMESTAMP,
    quarantine_reason  STRING
) USING DELTA
COMMENT 'Rows that failed a data-quality check, held for manual review';

-- NOT written automatically either -- target of 06's generated SCHEMA_DRIFT
-- fix template, for capturing upstream columns not in the expected schema.
CREATE TABLE IF NOT EXISTS zeroops.silver.unmapped_columns (
    order_id      INT,
    _source_file  STRING
    -- additional columns are appended dynamically (mergeSchema=true) by the
    -- generated fix, since the whole point is columns not known in advance
) USING DELTA
COMMENT 'Upstream columns not in the expected Bronze schema, held for triage';

-- ----------------------------------------------------------------------------
-- gold schema
-- ----------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS zeroops.gold.daily_sales (
    order_date     STRING,
    region         STRING,
    total_revenue  DOUBLE,
    total_units    BIGINT,
    order_count    BIGINT
) USING DELTA
COMMENT 'Daily revenue/units/orders by region';

CREATE TABLE IF NOT EXISTS zeroops.gold.monthly_revenue (
    year_month     STRING,
    region         STRING,
    total_revenue  DOUBLE,
    order_count    BIGINT
) USING DELTA
COMMENT 'Monthly revenue/orders by region';

-- ----------------------------------------------------------------------------
-- ops schema — ZeroOps metadata, not part of the medallion data layers
-- ----------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS zeroops.ops.run_history (
    pipeline_name  STRING,
    source_file    STRING,
    row_count      BIGINT,
    recorded_at    TIMESTAMP
) USING DELTA
COMMENT 'Per-run row counts, used by 04 to detect run-over-run anomalies';

CREATE TABLE IF NOT EXISTS zeroops.ops.incident_log (
    incident_id     STRING,
    detected_at     TIMESTAMP,
    pipeline_name   STRING,
    source_file     STRING,
    error_type      STRING,       -- SCHEMA_DRIFT | CAST_INVALID_INPUT | NULL_SPIKE |
                                   -- DUPLICATE_SPIKE | ROW_COUNT_ANOMALY
    description     STRING,
    affected_table  STRING,
    metrics_json    STRING,
    status          STRING        -- OPEN | ...
) USING DELTA
COMMENT 'Every incident raised by the pipeline monitor';

CREATE TABLE IF NOT EXISTS zeroops.ops.ai_analysis (
    incident_id             STRING,
    error_type              STRING,
    root_cause               STRING,
    business_impact          STRING,
    confidence                DOUBLE,
    requires_human_review     BOOLEAN,   -- confidence-gate guardrail outcome
    generated_by              STRING,    -- e.g. 'databricks_llm:databricks-claude-sonnet-4',
                                          -- 'openai:gpt-4o-mini', or 'rule_based_simulation_v1'
    generated_at              TIMESTAMP
) USING DELTA
COMMENT 'Root cause + business impact per incident, from LLM or rule engine';

CREATE TABLE IF NOT EXISTS zeroops.ops.ai_fix (
    incident_id                  STRING,
    error_type                   STRING,
    fix_summary                  STRING,
    generated_code                STRING,
    guardrail_status               STRING,   -- PASSED | BLOCKED
    guardrail_violations_json      STRING,
    generated_by                   STRING,
    generated_at                   TIMESTAMP
) USING DELTA
COMMENT 'Generated PySpark fix code per incident, plus its guardrail verdict';

CREATE TABLE IF NOT EXISTS zeroops.ops.validation_results (
    incident_id             STRING,
    error_type              STRING,
    before_metrics_json      STRING,
    after_metrics_json       STRING,
    validation_table          STRING,   -- points into the sandbox schema, e.g.
                                         -- zeroops.sandbox.sales_validation_<id>
    validation_narrative       STRING,
    semantic_similarity         DOUBLE,  -- cosine similarity vs CANONICAL_METRIC_CONTRACT (07)
    requires_semantic_review     BOOLEAN, -- True if similarity fell below policy threshold
    status                        STRING,  -- VALIDATION_PASSED | VALIDATION_FAILED (mechanical only)
    validated_at                   TIMESTAMP
) USING DELTA
COMMENT 'Before/after sandbox comparison results for each generated fix, plus semantic-drift check';

CREATE TABLE IF NOT EXISTS zeroops.ops.github_pr_history (
    incident_id              STRING,
    error_type               STRING,
    branch_name               STRING,
    fix_filename                STRING,
    pr_description_path          STRING,
    requires_human_review          BOOLEAN,
    requires_semantic_review        BOOLEAN,
    status                            STRING,   -- SIMULATED_PR_READY
    created_at                        TIMESTAMP
) USING DELTA
COMMENT 'Local git branch/commit/diff record for each proposed fix';

CREATE TABLE IF NOT EXISTS zeroops.ops.notifications (
    incident_id  STRING,
    message      STRING,
    channel      STRING,     -- 'slack_simulated'
    sent_at      TIMESTAMP
) USING DELTA
COMMENT 'Simulated Slack/Teams alert history';

CREATE TABLE IF NOT EXISTS zeroops.ops.guardrail_log (
    policy       STRING,   -- confidence_gate | dangerous_code_pattern | table_scope |
                            -- semantic_drift_check | human_approval_required
    subject_id   STRING,   -- usually an incident_id
    stage        STRING,   -- root_cause_analysis | fix_generation | sandbox_validation | pr_creation
    decision     STRING,   -- ALLOW | BLOCK
    reason       STRING,
    logged_at    TIMESTAMP
) USING DELTA
COMMENT 'Every guardrail/policy decision, pass or fail, for audit';

-- ============================================================================
-- Verify
-- ============================================================================
-- SHOW SCHEMAS IN zeroops;
-- SHOW VOLUMES IN zeroops.bronze;
-- SHOW VOLUMES IN zeroops.ops;
-- SHOW TABLES IN zeroops.bronze;
-- SHOW TABLES IN zeroops.silver;
-- SHOW TABLES IN zeroops.gold;
-- SHOW TABLES IN zeroops.ops;
-- SHOW TABLES IN zeroops.sandbox;


-- ============================================================================
-- SANDBOX SCHEMA — how it's actually used
-- ============================================================================
-- 07_sandbox_validation.py clones data into per-incident tables named
--   zeroops.sandbox.sales_validation_<incident_short_id>
-- at runtime, so their exact names can't be pre-declared here -- there's one
-- per incident, created the first time that incident is validated. The
-- `sandbox` schema itself is created above by CREATE SCHEMA IF NOT EXISTS, so
-- you never need to touch this file to add a new sandbox table; it just
-- appears the next time 07 runs.
--
-- To wipe every sandbox experiment and start clean without touching any real
-- data (bronze/silver/gold/ops are untouched by this):
--
--   DROP SCHEMA IF EXISTS zeroops.sandbox CASCADE;
--   CREATE SCHEMA IF NOT EXISTS zeroops.sandbox;
--
-- To fully reset the whole project (drops everything, including real data):
--
--   DROP CATALOG IF EXISTS zeroops CASCADE;
