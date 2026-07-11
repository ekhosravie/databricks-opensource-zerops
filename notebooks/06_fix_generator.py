# Databricks notebook source
# MAGIC %md
# MAGIC # 06 — Fix Generator
# MAGIC
# MAGIC Prompts an LLM (prompt design in `prompts/code_fix.txt`) with the incident, root
# MAGIC cause, and the offending pipeline code, and asks for a corrected PySpark snippet.
# MAGIC
# MAGIC Same three backends as notebook 05, selected with `llm_backend`:
# MAGIC - `databricks_llm` (default) — free, Databricks-hosted `system.ai` model
# MAGIC - `openai` — real OpenAI API, needs internet + paid key
# MAGIC - `rule_engine` — deterministic fixed templates keyed by `error_type`
# MAGIC
# MAGIC If the LLM backend fails or returns something unusable, this notebook
# MAGIC automatically falls back to the rule-based template for that `error_type`, so it
# MAGIC always produces a runnable fix.

# COMMAND ----------

# MAGIC %run ./00_setup_environment

# COMMAND ----------

dbutils.widgets.dropdown("llm_backend", "databricks_llm",
                          ["databricks_llm", "openai", "rule_engine"], "AI backend")
LLM_BACKEND = dbutils.widgets.get("llm_backend")
log(f"Using AI backend: {LLM_BACKEND}")

CODE_FIX_PROMPT_TEMPLATE = """You are a senior data engineer fixing a PySpark data pipeline bug.

Incident type: {error_type}
Root cause: {root_cause}

Pipeline code that produced this incident:
---
{pipeline_code}
---

Write a corrected PySpark code snippet that fixes the root cause above. Requirements:
- Do not change unrelated logic.
- Prefer explicit validation/quarantine over silently dropping or nulling data.
- The snippet must be runnable given a DataFrame variable named `bronze_df_input`, and
  must assign its final result to a variable named `clean_df`.
- If you need a side table (e.g. for quarantined or unmapped rows), it MUST be named
  under the `{allowed_prefix}` catalog -- for example `{quarantine_table}` for
  quarantined rows or `{unmapped_table}` for unmapped columns. Do not invent a table
  name outside that catalog.
- Add a one-line comment above the fix explaining what changed and why.

Respond with the corrected code only, no markdown fences, no explanation outside code comments.
"""

# Same short pipeline snippet used in notebook 05 -- duplicated here since each notebook
# only shares state with 00_setup_environment via %run, not with each other.
PIPELINE_CODE_CONTEXT = '''\
casted_df = bronze_df.withColumn("quantity", F.col("quantity").cast(IntegerType()))
selected_df = casted_df.select(
    "order_id", "order_date", "store_id", "product_id",
    "product_name", "quantity", "unit_price", "customer_id", "_ingestion_time",
)
joined_df = selected_df.join(F.broadcast(store_lookup), on="store_id", how="left")
'''

# COMMAND ----------

FIX_TEMPLATES = {
    "CAST_INVALID_INPUT": {
        "summary": "Quarantine non-numeric quantity values instead of silently nulling them.",
        "code": '''# Fix for CAST_INVALID_INPUT
# Instead of a plain cast (which turns bad values into silent NULLs), validate the
# string first and route failures to a quarantine table for manual review.
from pyspark.sql import functions as F

valid_mask = F.col("quantity").rlike("^[0-9]+$")

clean_df = casted_df_input.filter(valid_mask) \\
    .withColumn("quantity", F.col("quantity").cast("int"))

quarantined_df = casted_df_input.filter(~valid_mask) \\
    .withColumn("quarantine_reason", F.lit("quantity not numeric"))

quarantined_df.write.format("delta").mode("append") \\
    .saveAsTable("{quarantine_table}")
''',
    },
    "SCHEMA_DRIFT": {
        "summary": "Capture unmapped columns instead of dropping them in the select().",
        "code": '''# Fix for SCHEMA_DRIFT
# Instead of a fixed select() list that silently drops new columns, explicitly
# capture anything unexpected into a side table for review, and keep it out of
# the main Silver contract until it's been triaged.
from pyspark.sql import functions as F

expected_cols = {expected_cols}
actual_cols = set(bronze_df_input.columns) - {{"_ingestion_time", "_source_file"}}
new_cols = sorted(actual_cols - expected_cols)

if new_cols:
    unmapped_df = bronze_df_input.select("order_id", "_source_file", *new_cols)
    unmapped_df.write.format("delta").mode("append").option("mergeSchema", "true") \\
        .saveAsTable("{unmapped_table}")

clean_df = bronze_df_input.select(*sorted(expected_cols))
''',
    },
    "NULL_SPIKE": {
        "summary": "Flag rows with missing customer_id rather than passing them through silently.",
        "code": '''# Fix for NULL_SPIKE (customer_id)
from pyspark.sql import functions as F

clean_df = casted_df_input.withColumn(
    "customer_id_missing",
    F.when((F.col("customer_id").isNull()) | (F.col("customer_id") == ""), True).otherwise(False),
)
''',
    },
    "DUPLICATE_SPIKE": {
        "summary": "De-duplicate Bronze on (order_id, _source_file) before it ever reaches Silver.",
        "code": '''# Fix for DUPLICATE_SPIKE
from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy(F.col("_ingestion_time").desc())
clean_df = bronze_df_input.withColumn("_rn", F.row_number().over(w)) \\
    .filter(F.col("_rn") == 1).drop("_rn")
''',
    },
    "ROW_COUNT_ANOMALY": {
        "summary": "No direct fix — this incident is a symptom; investigate the paired incident instead.",
        "code": '''# ROW_COUNT_ANOMALY has no standalone fix.
# Check other OPEN incidents from the same monitor run (same detected_at) for the
# underlying cause (schema drift, duplicate spike, etc.) and apply that fix instead.
clean_df = None
''',
    },
}

# COMMAND ----------

def generate_fix_with_rule_engine(analysis: dict) -> dict:
    template = FIX_TEMPLATES.get(analysis["error_type"])
    if template is None:
        code = "# No fix template available for this error_type."
        summary = "Manual fix required."
    else:
        code = template["code"].format(
            expected_cols=set(CONFIG["expected_bronze_schema"]),
            quarantine_table=CONFIG["tables"]["silver_quarantine"],
            unmapped_table=CONFIG["tables"]["silver_unmapped_columns"],
        )
        summary = template["summary"]
    return {"fix_summary": summary, "generated_code": code, "generated_by": "rule_based_simulation_v1"}


def generate_fix_with_llm(analysis: dict, backend: str) -> dict:
    prompt = CODE_FIX_PROMPT_TEMPLATE.format(
        error_type=analysis["error_type"],
        root_cause=analysis.get("root_cause", "(not available)"),
        pipeline_code=PIPELINE_CODE_CONTEXT,
        allowed_prefix=CONFIG["policies"]["allowed_table_prefix"],
        quarantine_table=CONFIG["tables"]["silver_quarantine"],
        unmapped_table=CONFIG["tables"]["silver_unmapped_columns"],
    )
    raw = query_llm(prompt, backend=backend)
    code = strip_code_fences(raw)
    if not code.strip():
        raise ValueError("LLM returned empty code.")
    model_name = (CONFIG["llm"]["databricks_model"] if backend == "databricks_llm"
                  else CONFIG["llm"]["openai_model"])
    first_comment = next((l for l in code.split("\n") if l.strip().startswith("#")), "")
    summary = first_comment.lstrip("#").strip() or f"LLM-generated fix for {analysis['error_type']}"
    return {"fix_summary": summary, "generated_code": code, "generated_by": f"{backend}:{model_name}"}


def generate_fix(analysis: dict, backend: str) -> dict:
    if backend == "rule_engine":
        result = generate_fix_with_rule_engine(analysis)
    else:
        try:
            result = generate_fix_with_llm(analysis, backend)
        except Exception as e:
            log(f"WARNING: '{backend}' backend failed generating a fix for incident "
                f"{analysis['incident_id']} ({e}); falling back to rule engine.")
            result = generate_fix_with_rule_engine(analysis)

    # Guardrails / policy enforcement: scan the generated code before it's allowed
    # anywhere near a file write or a git commit (notebook 08). Applies regardless of
    # which backend produced the code -- LLM output and rule-engine templates both
    # go through the same gate.
    verdict = enforce_fix_guardrails(analysis["incident_id"], result["generated_code"])
    if verdict["status"] == "BLOCKED":
        log(f"BLOCKED fix for incident {analysis['incident_id']}: "
            f"dangerous_patterns={verdict['dangerous_hits']}, "
            f"scope_violations={verdict['scope_violations']}")

    return {
        "incident_id": analysis["incident_id"],
        "error_type": analysis["error_type"],
        "fix_summary": result["fix_summary"],
        "generated_code": result["generated_code"],
        "guardrail_status": verdict["status"],
        "guardrail_violations_json": json.dumps({
            "dangerous_hits": verdict["dangerous_hits"],
            "scope_violations": verdict["scope_violations"],
        }),
        "generated_by": result["generated_by"],
        "generated_at": datetime.datetime.utcnow(),
    }

# COMMAND ----------

analyzed = (
    spark.table(CONFIG["tables"]["ai_analysis"])
    .select("incident_id", "error_type", "root_cause")
    .collect()
)
existing_fixes = set()
if table_exists(CONFIG["tables"]["ai_fix"]):
    existing_fixes = {
        r.incident_id for r in spark.table(CONFIG["tables"]["ai_fix"]).select("incident_id").collect()
    }

to_fix = [row.asDict() for row in analyzed if row.incident_id not in existing_fixes]
fixes = [generate_fix(a, LLM_BACKEND) for a in to_fix]
log(f"Generated {len(fixes)} fix(es) using backend='{LLM_BACKEND}' for incidents without one yet.")

# COMMAND ----------

if fixes:
    fix_df = spark.createDataFrame(fixes)
    fix_df.write.format("delta").mode("append").option("mergeSchema", "true") \
        .saveAsTable(CONFIG["tables"]["ai_fix"])

    # Only fixes that passed the guardrail scan are written to disk. A BLOCKED fix
    # stays recorded in ai_fix (for audit) but notebook 08 will never see a file for
    # it, so it can never be committed or PR'd.
    for fix in fixes:
        if fix["guardrail_status"] == "BLOCKED":
            log(f"Skipping file write for BLOCKED fix (incident {fix['incident_id']}).")
            continue
        out_path = f"{CONFIG['paths']['generated_fixes']}/fix_{fix['incident_id']}.py"
        with open(out_path, "w") as f:
            f.write(f"# Fix for incident {fix['incident_id']} ({fix['error_type']})\n")
            f.write(f"# {fix['fix_summary']}\n\n")
            f.write(fix["generated_code"])
        log(f"Wrote generated fix file: {out_path}")

    display(fix_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Notes
# MAGIC - LLM-generated code is written to `{{generated_fixes}}/fix_{{incident_id}}.py`
# MAGIC   (a Unity Catalog Volume path, e.g. `/Volumes/zeroops/ops/artifacts/generated_fixes/...`)
# MAGIC   for notebook 08 to pick up as a real commit — same as the rule-engine path, so
# MAGIC   08 doesn't need to know which backend produced the file.
# MAGIC - `strip_code_fences()` (in `00_setup_environment`) removes any ``` wrapper the
# MAGIC   model adds despite being told not to.
# MAGIC - This notebook never `exec()`s the generated code itself — that only happens
# MAGIC   (in a controlled, mapped way) in notebook 07's sandbox validation gate.
# MAGIC - **Guardrails applied here:** every generated fix — LLM or rule-engine — is
# MAGIC   scanned by `enforce_fix_guardrails()` for banned code patterns (`DROP TABLE`,
# MAGIC   `os.system`, `dbutils.secrets`, etc.) and for writes outside the `zeroops.`
# MAGIC   table prefix. A fix that fails either check is recorded with
# MAGIC   `guardrail_status = BLOCKED` and is never written to disk, so it can never
# MAGIC   reach the git commit step in notebook 08. Every check (pass or fail) is logged
# MAGIC   to `zeroops.guardrail_log`.
