# Databricks notebook source
# MAGIC %md
# MAGIC # 03 — Gold Pipeline
# MAGIC Business-level aggregations recomputed from Silver: daily sales by region and
# MAGIC monthly revenue by region.

# COMMAND ----------

# MAGIC %run ./00_setup_environment

# COMMAND ----------

silver_df = spark.table(CONFIG["tables"]["silver"])

daily_sales = (
    silver_df
    .withColumn("revenue", F.col("quantity") * F.col("unit_price"))
    .groupBy("order_date", "region")
    .agg(
        F.sum("revenue").alias("total_revenue"),
        F.sum("quantity").alias("total_units"),
        F.count("order_id").alias("order_count"),
    )
    .orderBy("order_date", "region")
)

monthly_revenue = (
    silver_df
    .withColumn("revenue", F.col("quantity") * F.col("unit_price"))
    .withColumn("year_month", F.date_format(F.col("order_date"), "yyyy-MM"))
    .groupBy("year_month", "region")
    .agg(
        F.sum("revenue").alias("total_revenue"),
        F.count("order_id").alias("order_count"),
    )
    .orderBy("year_month", "region")
)

# COMMAND ----------

daily_sales.write.format("delta").mode("overwrite").saveAsTable(CONFIG["tables"]["gold_daily"])
monthly_revenue.write.format("delta").mode("overwrite").saveAsTable(CONFIG["tables"]["gold_monthly"])

log(f"Wrote {daily_sales.count()} rows to {CONFIG['tables']['gold_daily']}")
log(f"Wrote {monthly_revenue.count()} rows to {CONFIG['tables']['gold_monthly']}")

display(spark.table(CONFIG["tables"]["gold_daily"]))
