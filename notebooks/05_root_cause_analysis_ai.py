# Databricks notebook source
# MAGIC %md
# MAGIC # 05 — Root Cause Analysis
# MAGIC
# MAGIC This notebook sends the incident + a snippet of the offending pipeline code to an
# MAGIC LLM (prompt design in `prompts/root_cause.txt`) and gets back a root cause,
# MAGIC business impact, and confidence score.
# MAGIC
# MAGIC Three interchangeable backends, selected with the `llm_backend` widget:
# MAGIC - **`databricks_llm`** (default) — a Databricks Free Edition `system.ai` model
# MAGIC   (e.g. `databricks-claude-sonnet-4`), queried via this workspace's own
# MAGIC   `/serving-endpoints/...` url using the notebook's own token. No secrets, no
# MAGIC   external internet call, free under fair-use quotas.
# MAGIC - **`openai`** — the real OpenAI API. Needs actual outbound internet and a paid
# MAGIC   API key (`dbutils.secrets` scope `zeroops`/`openai_api_key`, or an
# MAGIC   `OPENAI_API_KEY` env var). Opt-in, since not every workspace/network allows
# MAGIC   general internet egress.
# MAGIC - **`rule_engine`** — a deterministic fallback with no model call at all. Used
# MAGIC   automatically if the chosen LLM backend errors (quota, network, parsing), so
# MAGIC   this notebook always produces a result either way.
# MAGIC
# MAGIC The output schema (`ai_analysis` table) is identical across all three, so
# MAGIC notebook 07 and the dashboard don't care which one produced a given row.

# COMMAND ----------

# MAGIC %run ./00_setup_environment

# COMMAND ----------

dbutils.widgets.dropdown("llm_backend", "databricks_llm",
                          ["databricks_llm", "openai", "rule_engine"], "AI backend")
LLM_BACKEND = dbutils.widgets.get("llm_backend")
log(f"Using AI backend: {LLM_BACKEND}")

# COMMAND ----------

# A short, representative snippet of the actual buggy code from 02_silver_transformation.py,
# used as pipeline context in the LLM prompt. Kept short deliberately -- enough for the model
# to reason about the exact operation, not a full-file dump.
PIPELINE_CODE_CONTEXT = '''\
casted_df = bronze_df.withColumn("quantity", F.col("quantity").cast(IntegerType()))
selected_df = casted_df.select(
    "order_id", "order_date", "store_id", "product_id",
    "product_name", "quantity", "unit_price", "customer_id", "_ingestion_time",
)
joined_df = selected_df.join(F.broadcast(store_lookup), on="store_id", how="left")
'''

ROOT_CAUSE_PROMPT_TEMPLATE = """You are a senior data engineer performing root cause analysis on a data pipeline incident.

Incident type: {error_type}
Incident description: {description}

Pipeline code that produced this incident:
---
{pipeline_code}
---

Respond with a JSON object only, no markdown fences, no preamble, with exactly these keys:
{{"root_cause": "<one paragraph, technical>", "business_impact": "<one paragraph, plain language>", "confidence": <float 0-1>}}

Be specific: name the exact column, operation, or assumption in the code responsible.
"""

# COMMAND ----------

# ---------------------------------------------------------------------------
# RULE ENGINE — stands in for the LLM call.
# Each entry: root_cause / business_impact are Python format-strings filled in
# with the incident's `metrics_json`, plus a fixed confidence score.
# ---------------------------------------------------------------------------
RULE_ENGINE = {
    "SCHEMA_DRIFT": {
        "root_cause": (
            "The upstream export started sending column(s) {new_columns} that are not part "
            "of the Bronze table's expected schema ({missing_columns} were expected but "
            "absent). The Silver transformation's explicit `.select(...)` list drops any "
            "column not named there, so the new field is silently discarded rather than "
            "raising an error."
        ),
        "business_impact": (
            "Any business value carried in the new column (e.g. a promo/discount code) is "
            "lost between Bronze and Silver with no error surfaced — reporting will look "
            "correct while quietly missing information analysts may expect to see."
        ),
        "confidence": 0.93,
    },
    "CAST_INVALID_INPUT": {
        "root_cause": (
            "Column '{column}' contains non-numeric values (e.g. \"N/A\") that fail a plain "
            "`.cast(\"int\")`. Spark's cast does not raise on failure — it silently returns "
            "NULL, so {null_pct:.1%} of rows now have a NULL quantity instead of an error "
            "or a quarantined record."
        ),
        "business_impact": (
            "Gold-layer revenue aggregations under-count sales for every row with a NULL "
            "quantity, since `quantity * unit_price` becomes NULL and is excluded from "
            "SUM(). This understates daily/monthly revenue without any visible failure."
        ),
        "confidence": 0.95,
    },
    "NULL_SPIKE": {
        "root_cause": (
            "Column '{column}' has {null_pct:.1%} nulls/blanks, above the "
            f"{CONFIG['thresholds']['null_pct']:.0%} threshold, most likely from an "
            "upstream anonymization or export change that stopped populating this field "
            "for a subset of rows."
        ),
        "business_impact": (
            "Any customer-level analysis (repeat-purchase rate, loyalty segmentation) "
            "silently loses coverage for the affected rows."
        ),
        "confidence": 0.88,
    },
    "DUPLICATE_SPIKE": {
        "root_cause": (
            "{duplicate_rows} order_id(s) were re-ingested from a prior batch, most likely "
            "an upstream retry without idempotency. The current pipeline does de-duplicate "
            "on order_id at the Silver MERGE step, so downstream Gold numbers are protected, "
            "but Bronze is accumulating redundant raw rows."
        ),
        "business_impact": (
            "No revenue impact today (MERGE handles it), but Bronze storage and Bronze-layer "
            "row-count-based monitors will trend upward unnecessarily and mask the *next* "
            "genuine anomaly if left unaddressed."
        ),
        "confidence": 0.90,
    },
    "ROW_COUNT_ANOMALY": {
        "root_cause": (
            "Row count moved from {previous} to {current} between runs, beyond the "
            f"{CONFIG['thresholds']['row_count_change_pct']:.0%} threshold. This is usually "
            "a symptom rather than a root cause — check the other incidents raised in the "
            "same monitor run for the underlying reason."
        ),
        "business_impact": (
            "Unexplained volume swings undermine trust in day-over-day reporting until "
            "the cause is confirmed."
        ),
        "confidence": 0.75,
    },
}

# COMMAND ----------

def analyze_with_rule_engine(incident: dict) -> dict:
    metrics = json.loads(incident["metrics_json"])
    rule = RULE_ENGINE.get(incident["error_type"])
    if rule is None:
        root_cause = "No rule matched this error_type; manual investigation required."
        business_impact = "Unknown — not covered by the current rule engine."
        confidence = 0.0
    else:
        try:
            root_cause = rule["root_cause"].format(**metrics)
            business_impact = rule["business_impact"].format(**metrics)
        except KeyError:
            root_cause = rule["root_cause"]
            business_impact = rule["business_impact"]
        confidence = rule["confidence"]

    return {
        "root_cause": root_cause,
        "business_impact": business_impact,
        "confidence": confidence,
        "generated_by": "rule_based_simulation_v1",
    }


def analyze_with_llm(incident: dict, backend: str) -> dict:
    # Data-minimization guardrail: redact PII-like values before they leave this
    # process in an LLM prompt, even though these descriptions rarely carry raw PII.
    safe_description = redact_pii(incident["description"])
    prompt = ROOT_CAUSE_PROMPT_TEMPLATE.format(
        error_type=incident["error_type"],
        description=safe_description,
        pipeline_code=PIPELINE_CODE_CONTEXT,
    )
    raw = query_llm(prompt, backend=backend)
    parsed = parse_json_loose(raw)
    model_name = (CONFIG["llm"]["databricks_model"] if backend == "databricks_llm"
                  else CONFIG["llm"]["openai_model"])
    return {
        "root_cause": parsed["root_cause"],
        "business_impact": parsed["business_impact"],
        "confidence": float(parsed["confidence"]),
        "generated_by": f"{backend}:{model_name}",
    }


def analyze_incident(incident: dict, backend: str) -> dict:
    if backend == "rule_engine":
        result = analyze_with_rule_engine(incident)
    else:
        try:
            result = analyze_with_llm(incident, backend)
        except Exception as e:
            log(f"WARNING: '{backend}' backend failed for incident "
                f"{incident['incident_id']} ({e}); falling back to rule engine.")
            result = analyze_with_rule_engine(incident)

    # Policy enforcement: confidence gate. This doesn't block anything here -- the
    # fix is still generated and validated either way -- it just decides whether
    # notebook 08 may treat the eventual PR as routine or must flag it for a human.
    passed_gate = confidence_gate_passed(result["confidence"])
    log_guardrail_decision(
        "confidence_gate", incident["incident_id"], "root_cause_analysis",
        "ALLOW" if passed_gate else "BLOCK",
        f"confidence={result['confidence']:.2f}, "
        f"threshold={CONFIG['policies']['min_confidence_for_auto_pr']:.2f}",
    )

    return {
        "incident_id": incident["incident_id"],
        "error_type": incident["error_type"],
        "root_cause": result["root_cause"],
        "business_impact": result["business_impact"],
        "confidence": result["confidence"],
        "requires_human_review": not passed_gate,
        "generated_by": result["generated_by"],
        "generated_at": datetime.datetime.utcnow(),
    }

# COMMAND ----------

open_incidents = (
    spark.table(CONFIG["tables"]["incident_log"])
    .filter(F.col("status") == "OPEN")
    .collect()
)

analyses = [analyze_incident(row.asDict(), LLM_BACKEND) for row in open_incidents]
log(f"Analyzed {len(analyses)} open incident(s) using backend='{LLM_BACKEND}'.")

if analyses:
    analysis_df = spark.createDataFrame(analyses)
    analysis_df.write.format("delta").mode("append").option("mergeSchema", "true") \
        .saveAsTable(CONFIG["tables"]["ai_analysis"])
    display(analysis_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Notes
# MAGIC - `databricks_llm` calls `query_databricks_llm()` in `00_setup_environment`, which
# MAGIC   hits `{workspace_url}/serving-endpoints/{model}/invocations` — this workspace's
# MAGIC   own url, not an external domain.
# MAGIC - `openai` calls the real `https://api.openai.com/v1/chat/completions` and needs
# MAGIC   both outbound internet and a paid key.
# MAGIC - Either backend falling back to `rule_engine` is expected behavior, not an error —
# MAGIC   check the printed WARNING log line to see why a given incident fell back.
# MAGIC - **Guardrails applied here:** the incident description is passed through
# MAGIC   `redact_pii()` before it enters any LLM prompt, and every incident's confidence
# MAGIC   score is checked against the `min_confidence_for_auto_pr` policy — the result is
# MAGIC   stored as `requires_human_review` and both the check and its outcome are logged
# MAGIC   to `zeroops.guardrail_log`.
