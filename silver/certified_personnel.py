# Databricks notebook source
# DBTITLE 1,Certified Personnel Silver Layer - Overview
# MAGIC %md
# MAGIC # Certified Personnel Silver Layer Pipeline
# MAGIC
# MAGIC **Purpose:** Clean and consolidate certified personnel (teachers/staff) data from bronze layer
# MAGIC
# MAGIC **Source Table:**
# MAGIC * `workspace.bronze.certified_personnel` (2,484,756 rows, 2010-2025)
# MAGIC
# MAGIC **Output:** `workspace.silver.certified_personnel` (14 columns)
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Key Transformations
# MAGIC
# MAGIC ### 1. Duplicate Removal
# MAGIC * **Problem:** ~230K duplicate records in 2022-24 (double row counts vs other years)
# MAGIC * **Cause:** Same year data downloaded on multiple dates
# MAGIC * **Solution:** DropDuplicates on business key
# MAGIC * **Result:** All duplicates have identical measures
# MAGIC
# MAGIC ### 2. Institution Key
# MAGIC * **`institution_key`** = composite join key
# MAGIC * **Logic:** `district_code_institution_number`
# MAGIC
# MAGIC ### 3. Data Quality
# MAGIC * Cast MEASURE to integer
# MAGIC * Standardize column names to snake_case
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Schema
# MAGIC
# MAGIC | Column | Type | Description |
# MAGIC |--------|------|-------------|
# MAGIC | school_year | string | Academic year |
# MAGIC | district_code | string | District code |
# MAGIC | district_name | string | District name |
# MAGIC | institution_number | string | Institution number |
# MAGIC | institution_name | string | School name |
# MAGIC | grades_served | string | Grade range |
# MAGIC | data_category | string | Category |
# MAGIC | data_sub_category | string | Subcategory value |
# MAGIC | employee_type | string | Role |
# MAGIC | staff_count | int | Number of staff |
# MAGIC | institution_key | string | Composite join key |
# MAGIC | source_year | string | Source file year |
# MAGIC | source_file | string | Source filename |
# MAGIC | report_name | string | Report identifier |

# COMMAND ----------

# DBTITLE 1,Build silver table
from pyspark.sql.functions import col, concat_ws, count as cnt
from pyspark.sql.types import IntegerType

cert_bronze = spark.table("workspace.bronze.certified_personnel")
print(f"Bronze: {cert_bronze.count():,} rows")

cert_deduped = cert_bronze.dropDuplicates(['LONG_SCHOOL_YEAR', 'SCHOOL_DSTRCT_CD', 'INSTN_NUMBER', 'DATA_CATEGORY', 'DATA_SUB_CATEGORY', 'EMPLOYEE_TYPE'])
print(f"After dedup: {cert_deduped.count():,} rows")

cert_silver = cert_deduped.select(
    col('LONG_SCHOOL_YEAR').alias('school_year'),
    col('SCHOOL_DSTRCT_CD').alias('district_code'),
    col('SCHOOL_DSTRCT_NM').alias('district_name'),
    col('INSTN_NUMBER').alias('institution_number'),
    col('INSTN_NAME').alias('institution_name'),
    col('GRADES_SERVED_DESC').alias('grades_served'),
    col('DATA_CATEGORY').alias('data_category'),
    col('DATA_SUB_CATEGORY').alias('data_sub_category'),
    col('EMPLOYEE_TYPE').alias('employee_type'),
    col('MEASURE').cast(IntegerType()).alias('staff_count'),
    concat_ws('_', col('SCHOOL_DSTRCT_CD'), col('INSTN_NUMBER')).alias('institution_key'),
    col('source_year'), col('source_file'), col('#RPT_NAME').alias('report_name')
)

display(cert_silver.limit(5))

# COMMAND ----------

# DBTITLE 1,Write to silver
cert_silver.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("workspace.silver.certified_personnel")
print(f"✓ workspace.silver.certified_personnel: {spark.table('workspace.silver.certified_personnel').count():,} rows")
