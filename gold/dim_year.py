# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# DBTITLE 1,Dim Year - Overview
# MAGIC %md
# MAGIC # Dimension: School Year
# MAGIC
# MAGIC **Purpose:** Time dimension for school years
# MAGIC
# MAGIC **Grain:** One row per school year
# MAGIC
# MAGIC **Source:** Extract unique years from silver tables
# MAGIC
# MAGIC **Output:** `workspace.gold.dim_year`
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Attributes
# MAGIC * **year_key** - Surrogate key (auto-generated)
# MAGIC * **school_year** - Business key (e.g., "2023-24")
# MAGIC * **start_calendar_year** - Calendar year when school year starts (e.g., 2023)
# MAGIC * **end_calendar_year** - Calendar year when school year ends (e.g., 2024)
# MAGIC * **fiscal_year** - State fiscal year (typically matches end_calendar_year)
# MAGIC * **sort_order** - Integer for ordering (e.g., 2024 for "2023-24")

# COMMAND ----------

# DBTITLE 1,Build dim_year
from pyspark.sql.functions import col, lit, substring, monotonically_increasing_id

print("=== Building Dim_Year ===")

# Gather all unique years from enrollment (most comprehensive)
enroll = spark.table("workspace.silver.enrollment_by_grade")
years = enroll.select('school_year').distinct()

print(f"Unique school years: {years.count()}")

# Parse school_year format (e.g., "2023-24")
dim_year = years.withColumn(
    'start_calendar_year',
    substring(col('school_year'), 1, 4).cast('int')
).withColumn(
    'end_calendar_year',
    substring(col('school_year'), 6, 2).cast('int') + 2000
).withColumn(
    'fiscal_year',
    substring(col('school_year'), 6, 2).cast('int') + 2000
).withColumn(
    'sort_order',
    substring(col('school_year'), 6, 2).cast('int') + 2000
).withColumn(
    'year_key',
    monotonically_increasing_id()
).select(
    'year_key',
    'school_year',
    'start_calendar_year',
    'end_calendar_year',
    'fiscal_year',
    'sort_order'
).orderBy('sort_order')

print(f"\nDim_Year built: {dim_year.count()} rows")
display(dim_year)

# COMMAND ----------

# DBTITLE 1,Write to gold
dim_year.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("workspace.gold.dim_year")
print(f"✓ workspace.gold.dim_year: {spark.table('workspace.gold.dim_year').count():,} rows")

# COMMAND ----------


