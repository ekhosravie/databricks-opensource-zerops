# Databricks notebook source
# MAGIC %md
# MAGIC # 10 — Incident Dashboard
# MAGIC A unified view across every stage of the ZeroOps lifecycle: detection → AI
# MAGIC analysis → AI fix → sandbox validation → simulated PR → simulated notification.
# MAGIC
# MAGIC Tip: in the Databricks UI, click the chart icon under any `display()` result
# MAGIC to turn these into a pinned dashboard.

# COMMAND ----------

# MAGIC %run ./00_setup_environment

# COMMAND ----------

# MAGIC %md ## Open incidents

# COMMAND ----------

display(
    spark.table(CONFIG["tables"]["incident_log"])
    .filter(F.col("status") == "OPEN")
    .orderBy(F.col("detected_at").desc())
)

# COMMAND ----------

# MAGIC %md ## Incidents by error type

# COMMAND ----------

display(
    spark.table(CONFIG["tables"]["incident_log"])
    .groupBy("error_type").count()
    .orderBy(F.col("count").desc())
)

# COMMAND ----------

# MAGIC %md ## Full lifecycle view (incident → root cause → fix → validation → PR)

# COMMAND ----------

incident = spark.table(CONFIG["tables"]["incident_log"])
analysis = spark.table(CONFIG["tables"]["ai_analysis"])
fix = spark.table(CONFIG["tables"]["ai_fix"])
validation = spark.table(CONFIG["tables"]["validation_results"])

lifecycle = (
    incident.alias("i")
    .join(analysis.alias("a"), "incident_id", "left")
    .join(
        fix.select("incident_id", "fix_summary", "generated_by", "guardrail_status").alias("f"),
        "incident_id", "left",
    )
    .join(
        validation.select("incident_id", "status", "validation_narrative")
        .withColumnRenamed("status", "validation_status").alias("v"),
        "incident_id", "left",
    )
    .select(
        "i.incident_id", "i.detected_at", "i.error_type", "i.description",
        "a.root_cause", "a.confidence", "a.requires_human_review", "a.generated_by",
        "f.fix_summary", "f.guardrail_status",
        "v.validation_status", "v.validation_narrative", "i.status",
    )
    .orderBy(F.col("i.detected_at").desc())
)

display(lifecycle)

# COMMAND ----------

# MAGIC %md ## Simulated GitHub PRs

# COMMAND ----------

if table_exists(CONFIG["tables"]["github_pr_history"]):
    display(spark.table(CONFIG["tables"]["github_pr_history"]).orderBy(F.col("created_at").desc()))
else:
    print("No PRs simulated yet — run notebook 08 first.")

# COMMAND ----------

# MAGIC %md ## Gold-layer business metrics (for context alongside incidents)

# COMMAND ----------

display(spark.table(CONFIG["tables"]["gold_daily"]).orderBy("order_date", "region"))

# COMMAND ----------

# MAGIC %md ## Guardrails & policy enforcement audit trail

# COMMAND ----------

if table_exists(CONFIG["tables"]["guardrail_log"]):
    display(
        spark.table(CONFIG["tables"]["guardrail_log"]).orderBy(F.col("logged_at").desc())
    )
else:
    print("No guardrail decisions logged yet — run notebooks 05/06/08 first.")

# COMMAND ----------

# MAGIC %md ## Guardrail decisions by policy and outcome

# COMMAND ----------

if table_exists(CONFIG["tables"]["guardrail_log"]):
    display(
        spark.table(CONFIG["tables"]["guardrail_log"])
        .groupBy("policy", "decision").count()
        .orderBy("policy", "decision")
    )
