# Databricks notebook source
# MAGIC %md
# MAGIC # 04 — Pipeline Monitor
# MAGIC Compares the current Bronze/Silver run against the last recorded run and flags:
# MAGIC - row-count swings beyond threshold
# MAGIC - schema drift (unexpected columns in Bronze)
# MAGIC - null spikes in key columns (`quantity`, `customer_id`)
# MAGIC - duplicate `order_id` spikes
# MAGIC
# MAGIC Any flagged condition writes an `OPEN` row into `incident_log`.

# COMMAND ----------

# MAGIC %run ./00_setup_environment

# COMMAND ----------

dbutils.widgets.text("source_file_filter", "sales_batch1.csv", "Which Bronze batch to evaluate")
SOURCE_FILTER = dbutils.widgets.get("source_file_filter")

bronze_df = spark.table(CONFIG["tables"]["bronze"]).filter(F.col("_source_file") == SOURCE_FILTER)
current_row_count = bronze_df.count()
current_columns = set(bronze_df.columns) - {"_ingestion_time", "_source_file"}

log(f"Evaluating batch '{SOURCE_FILTER}': {current_row_count} rows, columns={sorted(current_columns)}")

# COMMAND ----------

# ---- Metric 1: row count vs previous run for this pipeline ----
run_history_schema = StructType([
    StructField("pipeline_name", StringType()),
    StructField("source_file", StringType()),
    StructField("row_count", LongType()),
    StructField("recorded_at", TimestampType()),
])

if table_exists(CONFIG["tables"]["run_history"]):
    prev_runs = (
        spark.table(CONFIG["tables"]["run_history"])
        .filter(F.col("pipeline_name") == "bronze_sales")
        .orderBy(F.col("recorded_at").desc())
    )
    prev_row = prev_runs.limit(1).collect()
    prev_row_count = prev_row[0]["row_count"] if prev_row else None
else:
    prev_row_count = None

if prev_row_count:
    row_count_change_pct = abs(current_row_count - prev_row_count) / max(prev_row_count, 1)
else:
    row_count_change_pct = 0.0

log(f"Previous run row count: {prev_row_count}, change pct: {row_count_change_pct:.2%}")

# COMMAND ----------

# ---- Metric 2: schema drift vs expected baseline schema ----
expected_columns = set(CONFIG["expected_bronze_schema"])
new_columns = current_columns - expected_columns
missing_columns = expected_columns - current_columns

# COMMAND ----------

# ---- Metric 3: null spike in key columns ----
null_metrics = {}
for c in ["quantity", "customer_id"]:
    if c in bronze_df.columns:
        null_count = bronze_df.filter(F.col(c).isNull() | (F.col(c) == "")).count()
        null_metrics[c] = null_count / max(current_row_count, 1)

# COMMAND ----------

# ---- Metric 4: duplicate order_id spike ----
dup_count = (
    bronze_df.groupBy("order_id").count()
    .filter(F.col("count") > 1)
    .agg(F.sum(F.col("count") - 1))
    .collect()[0][0]
) or 0
duplicate_pct = dup_count / max(current_row_count, 1)

log(f"Nulls: {null_metrics}, duplicate_pct: {duplicate_pct:.2%}, "
    f"new_columns: {new_columns}, missing_columns: {missing_columns}")

# COMMAND ----------

incidents = []
t = CONFIG["thresholds"]

if new_columns or missing_columns:
    incidents.append({
        "error_type": "SCHEMA_DRIFT",
        "description": f"New columns detected: {sorted(new_columns) or 'none'}; "
                        f"missing expected columns: {sorted(missing_columns) or 'none'}.",
        "metrics": {"new_columns": sorted(new_columns), "missing_columns": sorted(missing_columns)},
    })

for col_name, pct in null_metrics.items():
    if pct > t["null_pct"]:
        incidents.append({
            "error_type": "NULL_SPIKE" if col_name != "quantity" else "CAST_INVALID_INPUT",
            "description": f"Column '{col_name}' has {pct:.2%} nulls/blanks, "
                            f"above the {t['null_pct']:.0%} threshold.",
            "metrics": {"column": col_name, "null_pct": round(pct, 4)},
        })

if duplicate_pct > t["duplicate_pct"]:
    incidents.append({
        "error_type": "DUPLICATE_SPIKE",
        "description": f"{duplicate_pct:.2%} of order_ids are duplicated, "
                        f"above the {t['duplicate_pct']:.0%} threshold.",
        "metrics": {"duplicate_pct": round(duplicate_pct, 4), "duplicate_rows": dup_count},
    })

if row_count_change_pct > t["row_count_change_pct"]:
    incidents.append({
        "error_type": "ROW_COUNT_ANOMALY",
        "description": f"Row count changed by {row_count_change_pct:.2%} vs previous run "
                        f"({prev_row_count} -> {current_row_count}).",
        "metrics": {"previous": prev_row_count, "current": current_row_count},
    })

log(f"{len(incidents)} incident(s) detected." if incidents else "No incidents detected — pipeline healthy.")

# COMMAND ----------

if incidents:
    incident_rows = []
    now = datetime.datetime.utcnow()
    for inc in incidents:
        incident_rows.append({
            "incident_id": new_id(),
            "detected_at": now,
            "pipeline_name": "bronze_sales",
            "source_file": SOURCE_FILTER,
            "error_type": inc["error_type"],
            "description": inc["description"],
            "affected_table": CONFIG["tables"]["bronze"],
            "metrics_json": json.dumps(inc["metrics"]),
            "status": "OPEN",
        })

    incident_df = spark.createDataFrame(incident_rows)
    incident_df.write.format("delta").mode("append").option("mergeSchema", "true") \
        .saveAsTable(CONFIG["tables"]["incident_log"])
    log(f"Wrote {len(incident_rows)} incident(s) to {CONFIG['tables']['incident_log']}.")

# COMMAND ----------

history_row = spark.createDataFrame([{
    "pipeline_name": "bronze_sales",
    "source_file": SOURCE_FILTER,
    "row_count": current_row_count,
    "recorded_at": datetime.datetime.utcnow(),
}])
history_row.write.format("delta").mode("append").option("mergeSchema", "true") \
    .saveAsTable(CONFIG["tables"]["run_history"])
log("Run history updated for next comparison.")

# COMMAND ----------

if table_exists(CONFIG["tables"]["incident_log"]):
    display(spark.table(CONFIG["tables"]["incident_log"]).filter(F.col("status") == "OPEN"))
