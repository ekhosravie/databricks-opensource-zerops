# Databricks notebook source
# MAGIC %md
# MAGIC # 09 — Notification (simulated)
# MAGIC Production version POSTs a formatted message to a Slack or Teams webhook.
# MAGIC Community Edition has no outbound internet access, so this notebook writes the
# MAGIC same message to a Delta table and prints it, so you can see exactly what would
# MAGIC have been sent.

# COMMAND ----------

# MAGIC %run ./00_setup_environment

# COMMAND ----------

incidents = spark.table(CONFIG["tables"]["incident_log"]).alias("i")
analysis = spark.table(CONFIG["tables"]["ai_analysis"]).alias("a")
validation = spark.table(CONFIG["tables"]["validation_results"]).alias("v")
pr_history = spark.table(CONFIG["tables"]["github_pr_history"]).alias("p") if table_exists(CONFIG["tables"]["github_pr_history"]) else None

joined = (
    incidents
    .join(analysis, "incident_id", "left")
    .join(validation, ["incident_id", "error_type"], "left")
)
if pr_history is not None:
    joined = joined.join(pr_history, ["incident_id", "error_type"], "left")

already_notified = set()
if table_exists(CONFIG["tables"]["notifications"]):
    already_notified = {
        r.incident_id for r in spark.table(CONFIG["tables"]["notifications"])
        .select("incident_id").collect()
    }

rows_to_notify = [r.asDict() for r in joined.collect() if r["incident_id"] not in already_notified]
log(f"{len(rows_to_notify)} incident(s) pending notification.")

# COMMAND ----------

def format_message(row: dict) -> str:
    confidence = row.get("confidence")
    confidence_str = f"{confidence:.0%}" if confidence is not None else "n/a"
    status = row.get("status") or "AWAITING_ANALYSIS"
    review_flag = "\n⚠️ REQUIRES HUMAN REVIEW (low AI confidence)" if row.get("requires_human_review") else ""
    semantic_flag = "\n🧭 REQUIRES SEMANTIC REVIEW (metric definition may have drifted)" if row.get("requires_semantic_review") else ""
    return (
        f"🚨 *ZeroOps Alert* — {row['error_type']}\n"
        f"Pipeline: {row['pipeline_name']} (batch `{row['source_file']}`)\n"
        f"Description: {row['description']}\n"
        f"AI confidence: {confidence_str}\n"
        f"Validation status: {status}"
        f"{review_flag}"
        f"{semantic_flag}\n"
        f"Incident ID: {row['incident_id']}"
    )

# COMMAND ----------

notification_rows = []
for row in rows_to_notify:
    msg = format_message(row)
    print(msg)
    print("-" * 60)
    notification_rows.append({
        "incident_id": row["incident_id"],
        "message": msg,
        "channel": "slack_simulated",
        "sent_at": datetime.datetime.utcnow(),
    })

if notification_rows:
    notif_df = spark.createDataFrame(notification_rows)
    notif_df.write.format("delta").mode("append").option("mergeSchema", "true") \
        .saveAsTable(CONFIG["tables"]["notifications"])
    log(f"Recorded {len(notification_rows)} simulated notification(s).")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Production reference (commented out — requires internet)

# COMMAND ----------

# import requests
#
# SLACK_WEBHOOK_URL = dbutils.secrets.get("zeroops", "slack_webhook_url")
#
# def send_slack_notification(message: str) -> None:
#     response = requests.post(SLACK_WEBHOOK_URL, json={"text": message}, timeout=10)
#     response.raise_for_status()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Notes
# MAGIC - **Guardrails applied here:** the alert surfaces `requires_human_review` (set in
# MAGIC   notebook 05's confidence-gate policy) directly in the message text, so whoever
# MAGIC   reads the alert — human or downstream automation — sees the same trust signal
# MAGIC   that gated the PR description in notebook 08.
