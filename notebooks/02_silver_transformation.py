# Databricks notebook source
# MAGIC %md
# MAGIC # 02 — Silver Transformation
# MAGIC Casts, joins against `store_lookup`, and deduplicates on `order_id`.
# MAGIC
# MAGIC ⚠️ **This notebook intentionally contains two realistic bugs**, on purpose,
# MAGIC so the monitor/AI/fix steps downstream have something real to catch:
# MAGIC 1. `quantity` is cast with a plain `.cast("int")` — Spark casts that fail
# MAGIC    (e.g. `"N/A"`) become `NULL` silently instead of raising an error.
# MAGIC 2. Only the expected columns are selected — a new upstream column like
# MAGIC    `discount_code` is silently dropped instead of being flagged.

# COMMAND ----------

# MAGIC %run ./00_setup_environment

# COMMAND ----------

dbutils.widgets.text("source_file_filter", "sales_batch1.csv", "Which Bronze batch to process")
SOURCE_FILTER = dbutils.widgets.get("source_file_filter")

bronze_df = spark.table(CONFIG["tables"]["bronze"]).filter(F.col("_source_file") == SOURCE_FILTER)
store_lookup = (
    spark.read.option("header", "true").option("inferSchema", "true")
    .csv(f"{CONFIG['paths']['data']}/store_lookup.csv")
)

log(f"Bronze rows for {SOURCE_FILTER}: {bronze_df.count()}")

# COMMAND ----------

# --- BUG #1: silent cast. "N/A" -> NULL instead of a raised/quarantined error. ---
casted_df = bronze_df.withColumn("quantity", F.col("quantity").cast(IntegerType()))

# --- BUG #2: silent column drop. Only known columns survive the select(). ---
selected_df = casted_df.select(
    "order_id", "order_date", "store_id", "product_id",
    "product_name", "quantity", "unit_price", "customer_id",
    "_ingestion_time",
)

joined_df = selected_df.join(F.broadcast(store_lookup), on="store_id", how="left")

# COMMAND ----------

# Deduplicate on order_id, keeping the most recently ingested record.
window = __import__("pyspark.sql.window", fromlist=["Window"]).Window
w = window.partitionBy("order_id").orderBy(F.col("_ingestion_time").desc())
deduped_df = (
    joined_df
    .withColumn("_rn", F.row_number().over(w))
    .filter(F.col("_rn") == 1)
    .drop("_rn")
)

log(f"Rows after cast+join: {joined_df.count()}, after de-dup: {deduped_df.count()}")

# COMMAND ----------

if table_exists(CONFIG["tables"]["silver"]):
    (
        deduped_df.createOrReplaceTempView("silver_updates")
    )
    spark.sql(f"""
        MERGE INTO {CONFIG['tables']['silver']} AS target
        USING silver_updates AS source
        ON target.order_id = source.order_id
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)
    log(f"MERGE INTO {CONFIG['tables']['silver']} complete (idempotent on order_id).")
else:
    deduped_df.write.format("delta").mode("overwrite").saveAsTable(CONFIG["tables"]["silver"])
    log(f"Created {CONFIG['tables']['silver']} for the first time.")

display(spark.table(CONFIG["tables"]["silver"]).limit(10))
