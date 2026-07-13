# Databricks notebook source
# MAGIC %md
# MAGIC # 07 — Sandbox Validation
# MAGIC Clones Silver into a validation copy, applies the fix that corresponds to each
# MAGIC incident's `error_type`, and compares row count / schema / null% / duplicate%
# MAGIC before and after.
# MAGIC
# MAGIC **Safety note:** this notebook does **not** `exec()` the AI-generated code string
# MAGIC from notebook 06 directly. Running dynamically generated code from an LLM (or a
# MAGIC rule engine) without isolation is a real risk even in a portfolio project. Instead,
# MAGIC `error_type` is mapped to a small set of hand-vetted transformation functions that
# MAGIC mirror what the generated code does. In a production system, the generated diff
# MAGIC would run inside an isolated sandbox/container *before* this mapping step —
# MAGIC this notebook demonstrates the validation gate, not code execution isolation.
# MAGIC
# MAGIC **The pass/fail decision below is always the deterministic metric comparison —
# MAGIC never the LLM.** On top of that, this notebook optionally asks an LLM (same
# MAGIC `llm_backend` widget as 05/06) to write a one-paragraph plain-English summary of
# MAGIC the before/after metrics, purely for the dashboard narrative. If that call fails,
# MAGIC the numeric result is unaffected — only the narrative text falls back to a
# MAGIC templated sentence.
# MAGIC
# MAGIC Each incident's validation clone is written to its own table under the `sandbox`
# MAGIC schema (e.g. `zeroops.sandbox.sales_validation_<incident_short_id>`), keeping
# MAGIC these throwaway experiments completely separate from the real `silver` schema.
# MAGIC
# MAGIC ### Mechanical validation vs. semantic drift
# MAGIC The metric checks above (row count, null %, duplicate %) catch **mechanical**
# MAGIC breakage — but they can't catch a fix that's mechanically clean and still
# MAGIC silently changes what a number *means* (e.g. quietly narrowing which rows count
# MAGIC toward revenue). To catch that class of problem, this notebook also embeds the
# MAGIC fix's code against a plain-English description of the metric's intended
# MAGIC contract (`CANONICAL_METRIC_CONTRACT` below) using a free Databricks-hosted
# MAGIC embedding model, and flags a low cosine similarity as `requires_semantic_review`.
# MAGIC This is a heuristic, not a proof of correctness — but it's a real, automated
# MAGIC check for exactly the gap that mechanical validation can't see, and it fails
# MAGIC safe (flags for review) if the embedding call itself errors.

# COMMAND ----------

# MAGIC %run ./00_setup_environment

# COMMAND ----------

dbutils.widgets.dropdown("llm_backend", "databricks_llm",
                          ["databricks_llm", "openai", "rule_engine"], "AI backend")
LLM_BACKEND = dbutils.widgets.get("llm_backend")
log(f"Using AI backend for validation narrative: {LLM_BACKEND}")

# COMMAND ----------

# The intended, human-approved meaning of the metrics this pipeline produces. A fix
# whose embedding drifts far from this description is flagged for semantic review,
# regardless of how clean it looks mechanically. Update this any time the actual
# business definition of these metrics legitimately changes.
CANONICAL_METRIC_CONTRACT = (
    "Revenue is computed as quantity multiplied by unit_price, summed per region "
    "per day in gold.daily_sales and per region per month in gold.monthly_revenue. "
    "Every row with a validly parsed positive integer quantity counts toward these "
    "totals. unit_price is taken as-is from Bronze. No row is excluded from revenue "
    "based on customer_id, discount_code, or any column other than quantity validity."
)

# COMMAND ----------

def apply_fix(bronze_df_input, error_type: str):
    """Vetted transformations mirroring the templates generated in notebook 06."""
    if error_type == "CAST_INVALID_INPUT":
        valid_mask = F.col("quantity").rlike("^[0-9]+$")
        clean_df = bronze_df_input.filter(valid_mask).withColumn("quantity", F.col("quantity").cast("int"))
        return clean_df
    elif error_type == "SCHEMA_DRIFT":
        expected_cols = set(CONFIG["expected_bronze_schema"])
        return bronze_df_input.select(*[c for c in bronze_df_input.columns if c in expected_cols])
    elif error_type == "DUPLICATE_SPIKE":
        from pyspark.sql.window import Window
        w = Window.partitionBy("order_id").orderBy(F.col("_ingestion_time").desc())
        return bronze_df_input.withColumn("_rn", F.row_number().over(w)).filter(F.col("_rn") == 1).drop("_rn")
    else:
        return bronze_df_input


VALIDATION_NARRATIVE_PROMPT = """You are a data engineer summarizing a sandbox validation result \
for a non-technical stakeholder dashboard.

Error type: {error_type}
Before-fix metrics: {before_metrics}
After-fix metrics: {after_metrics}
Deterministic result: {status}

Write exactly one short paragraph (2-3 sentences) in plain, non-technical language \
summarizing whether the fix worked and why. Do not include numbers verbatim; describe \
the change qualitatively (e.g. "nulls dropped from noticeable to zero"). Respond with \
plain text only, no markdown, no preamble.
"""


def generate_validation_narrative(error_type, before, after, status, backend):
    if backend == "rule_engine":
        return (f"Deterministic check: {status.replace('_', ' ').lower()} for {error_type}. "
                f"Row count went from {before['row_count']} to {after['row_count']}.")
    try:
        prompt = VALIDATION_NARRATIVE_PROMPT.format(
            error_type=error_type, before_metrics=before, after_metrics=after, status=status,
        )
        return query_llm(prompt, backend=backend).strip()
    except Exception as e:
        log(f"WARNING: narrative generation via '{backend}' failed ({e}); using a templated sentence.")
        return (f"Deterministic check: {status.replace('_', ' ').lower()} for {error_type}. "
                f"Row count went from {before['row_count']} to {after['row_count']}.")


def compute_metrics(df):
    total = df.count()
    null_qty = df.filter(F.col("quantity").isNull()).count() if "quantity" in df.columns else 0
    dup = (
        df.groupBy("order_id").count().filter(F.col("count") > 1)
        .agg(F.sum(F.col("count") - 1)).collect()[0][0] or 0
    )
    return {
        "row_count": total,
        "null_quantity_pct": round(null_qty / max(total, 1), 4),
        "duplicate_pct": round(dup / max(total, 1), 4),
        "columns": sorted(df.columns),
    }

# COMMAND ----------

pending_fixes = (
    spark.table(CONFIG["tables"]["ai_fix"])
    .filter(F.col("guardrail_status") == "PASSED")
    .join(
        spark.table(CONFIG["tables"]["incident_log"]).select("incident_id", "source_file"),
        on="incident_id",
    )
    .collect()
)

blocked_count = (
    spark.table(CONFIG["tables"]["ai_fix"]).filter(F.col("guardrail_status") == "BLOCKED").count()
    if "guardrail_status" in spark.table(CONFIG["tables"]["ai_fix"]).columns else 0
)
if blocked_count:
    log(f"{blocked_count} fix(es) excluded from validation because guardrail_status = BLOCKED.")

results = []
for row in pending_fixes:
    d = row.asDict()
    bronze_batch = spark.table(CONFIG["tables"]["bronze"]).filter(F.col("_source_file") == d["source_file"])

    before_metrics = compute_metrics(bronze_batch)
    fixed_df = apply_fix(bronze_batch, d["error_type"])
    after_metrics = compute_metrics(fixed_df)

    validation_name = f"{CONFIG['sandbox_schema']}.sales_validation_{d['incident_id'][:8]}"
    fixed_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true") \
        .saveAsTable(validation_name)

    # This pass/fail decision is the deterministic, authoritative gate. It never
    # depends on an LLM call -- the LLM below only narrates this already-decided result.
    passed = (
        after_metrics["null_quantity_pct"] <= CONFIG["thresholds"]["null_pct"]
        and after_metrics["duplicate_pct"] <= CONFIG["thresholds"]["duplicate_pct"]
    )
    status = "VALIDATION_PASSED" if passed else "VALIDATION_FAILED"

    narrative = generate_validation_narrative(
        d["error_type"], before_metrics, after_metrics, status, LLM_BACKEND
    )

    # Semantic drift check: does this fix's code still mean what the metric contract
    # says it should mean? This is independent of the mechanical pass/fail above --
    # a fix can pass every mechanical check and still fail this one, and vice versa.
    if LLM_BACKEND == "rule_engine":
        # No embedding call in rule-engine mode -- nothing to compare semantically
        # against, so this is skipped rather than guessed at.
        semantic = {"similarity": None, "drifted": False, "error": "skipped (rule_engine backend)"}
    else:
        semantic = check_semantic_drift(CANONICAL_METRIC_CONTRACT, d["generated_code"])

    log_guardrail_decision(
        "semantic_drift_check", d["incident_id"], "sandbox_validation",
        "BLOCK" if semantic["drifted"] else "ALLOW",
        f"similarity={semantic['similarity']}, "
        f"threshold={CONFIG['policies']['min_semantic_similarity']}"
        + (f", error={semantic['error']}" if semantic.get("error") else ""),
    )

    results.append({
        "incident_id": d["incident_id"],
        "error_type": d["error_type"],
        "before_metrics_json": json.dumps(before_metrics),
        "after_metrics_json": json.dumps(after_metrics),
        "validation_table": validation_name,
        "validation_narrative": narrative,
        "semantic_similarity": semantic["similarity"],
        "requires_semantic_review": semantic["drifted"],
        "status": status,
        "validated_at": datetime.datetime.utcnow(),
    })
    log(f"[{d['error_type']}] before={before_metrics} after={after_metrics} -> {status} "
        f"(semantic_similarity={semantic['similarity']}, "
        f"requires_semantic_review={semantic['drifted']})")

# COMMAND ----------

if results:
    already_validated = set()
    if table_exists(CONFIG["tables"]["validation_results"]):
        already_validated = {
            r.incident_id for r in spark.table(CONFIG["tables"]["validation_results"])
            .select("incident_id").collect()
        }
    new_results = [r for r in results if r["incident_id"] not in already_validated]

    if new_results:
        result_df = spark.createDataFrame(new_results)
        result_df.write.format("delta").mode("append").option("mergeSchema", "true") \
            .saveAsTable(CONFIG["tables"]["validation_results"])
        display(result_df)
    else:
        log("All incidents already have a validation result on record.")
