# Databricks notebook source
# MAGIC %md
# MAGIC # 01 — Bronze Ingestion
# MAGIC Reads a raw CSV batch from DBFS and appends it to the Bronze Delta table,
# MAGIC preserving whatever columns actually show up in the file (so schema drift is
# MAGIC visible here rather than silently dropped).

# COMMAND ----------

# MAGIC %run ./00_setup_environment

# COMMAND ----------

dbutils.widgets.text("batch_file", "sales_batch1.csv", "CSV file name under /data")
BATCH_FILE = dbutils.widgets.get("batch_file")
SOURCE_PATH = f"{CONFIG['paths']['data']}/{BATCH_FILE}"

log(f"Reading batch: {SOURCE_PATH}")

# COMMAND ----------

raw_df = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(SOURCE_PATH)
)

log(f"Raw batch schema: {raw_df.schema.simpleString()}")
log(f"Raw batch row count: {raw_df.count()}")
display(raw_df.limit(10))

# COMMAND ----------

bronze_df = (
    raw_df
    .withColumn("_ingestion_time", F.current_timestamp())
    .withColumn("_source_file", F.lit(BATCH_FILE))
)

(
    bronze_df.write
    .format("delta")
    .mode("append")
    .option("mergeSchema", "true")   # allow new columns (e.g. discount_code) to land in Bronze
    .saveAsTable(CONFIG["tables"]["bronze"])
)

log(f"Appended {bronze_df.count()} rows into {CONFIG['tables']['bronze']} "
    f"(mergeSchema=true, so upstream schema drift is captured, not rejected).")

# COMMAND ----------

# MAGIC %md
# MAGIC Run this notebook twice with the widget set to:
# MAGIC 1. `sales_batch1.csv` — clean baseline batch
# MAGIC 2. `sales_batch2_bad.csv` — batch with an extra `discount_code` column, some
# MAGIC    `quantity = "N/A"` values, missing `customer_id`s, and 15 duplicated `order_id`s
# MAGIC    re-sent from batch 1 — this is what notebook 04 will catch.

# COMMAND ----------

display(spark.table(CONFIG["tables"]["bronze"]).groupBy("_source_file").count())
