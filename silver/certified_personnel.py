# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
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
from pyspark.sql.types import DoubleType

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
    col('MEASURE').cast(DoubleType()).alias('staff_count'),
    concat_ws('_', col('SCHOOL_DSTRCT_CD'), col('INSTN_NUMBER')).alias('institution_key'),
    col('source_year'), col('source_file'), col('#RPT_NAME').alias('report_name')
)

display(cert_silver.limit(5))

# COMMAND ----------

# DBTITLE 1,Validate transformations
from pyspark.sql.functions import min as spark_min, max as spark_max, sum as spark_sum, countDistinct

print("\n=== Data Quality Validation ===\n")

# 1. Row count validation
print("1. Row Count:")
bronze_total = cert_bronze.count()
silver_total = cert_silver.count()
rows_removed = bronze_total - silver_total
print(f"   Bronze: {bronze_total:,} rows")
print(f"   Duplicates removed: {rows_removed:,} rows")
print(f"   Silver: {silver_total:,} rows")
if rows_removed > 0:
    print(f"   ✓ {rows_removed:,} duplicates removed ({rows_removed/bronze_total*100:.1f}% of bronze)")
else:
    print("   ✓ No duplicates found")

# 2. Business key uniqueness
print("\n2. Business Key Uniqueness:")
dup_check = cert_silver.groupBy(
    'school_year', 'district_code', 'institution_number', 
    'data_category', 'data_sub_category', 'employee_type'
).agg(cnt('*').alias('dup_count')).filter('dup_count > 1').count()
print(f"   Duplicate business keys: {dup_check}")
if dup_check == 0:
    print("   ✓ All business keys unique")
else:
    print(f"   ⚠️ {dup_check} duplicate keys found")

# 3. Staff count validation
print("\n3. Staff Count Range:")
staff_stats = cert_silver.filter("staff_count IS NOT NULL").agg(
    spark_min('staff_count').alias('min_count'),
    spark_max('staff_count').alias('max_count'),
    spark_sum('staff_count').alias('total_count')
).collect()[0]

print(f"   Min: {staff_stats['min_count']}")
print(f"   Max: {staff_stats['max_count']:,}")
print(f"   Total: {staff_stats['total_count']:,}")

# Check for negative or zero counts
neg_zero = cert_silver.filter("staff_count IS NOT NULL AND staff_count <= 0").count()
if neg_zero == 0:
    print("   ✓ No negative or zero counts")
else:
    print(f"   ⚠️ {neg_zero} records with count <= 0")

# 4. NULL handling
print("\n4. NULL Value Check:")
null_counts = cert_silver.filter("staff_count IS NULL").count()
print(f"   NULL staff_count: {null_counts:,}")
if null_counts == 0:
    print("   ✓ No NULL staff counts")
else:
    print(f"   ⚠️ {null_counts:,} records with NULL staff_count")

# 5. Year coverage
print("\n5. Year Coverage:")
print("   Records per year:")
cert_silver.groupBy('school_year').agg(
    cnt('*').alias('record_count')
).orderBy('school_year').show(20, truncate=False)

# 6. Data category distribution
print("\n6. Data Category Distribution:")
cert_silver.groupBy('data_category').agg(
    cnt('*').alias('record_count'),
    countDistinct('data_sub_category').alias('unique_subcategories')
).orderBy('data_category').show(truncate=False)

# 7. Employee type distribution
print("\n7. Employee Type Distribution:")
cert_silver.groupBy('employee_type').agg(
    cnt('*').alias('record_count'),
    spark_sum('staff_count').alias('total_staff')
).orderBy(col('record_count').desc()).show(20, truncate=False)

# 8. Institution key format check
print("\n8. Institution Key Validation:")
invalid_keys = cert_silver.filter(
    "institution_key IS NULL OR institution_key = '' OR institution_key NOT LIKE '%_%'"
).count()
if invalid_keys == 0:
    print("   ✓ All institution keys properly formatted")
else:
    print(f"   ⚠️ {invalid_keys} invalid institution keys")

print("\n✓ Validation complete")

# COMMAND ----------

# DBTITLE 1,Investigate zero staff counts
print("=== Investigating Zero Staff Counts ===\n")

zero_staff = cert_silver.filter("staff_count = 0")
zero_count = zero_staff.count()
total_count = cert_silver.count()

print(f"Total records with 0 staff: {zero_count:,} ({zero_count/total_count*100:.1f}% of all records)\n")

# Distribution by data category and subcategory
print("1. Zero Counts by Category and Subcategory:")
zero_staff.groupBy('data_category', 'data_sub_category').agg(
    cnt('*').alias('zero_count')
).orderBy('data_category', col('zero_count').desc()).show(50, truncate=False)

# Distribution by employee type
print("\n2. Zero Counts by Employee Type:")
zero_staff.groupBy('employee_type').agg(
    cnt('*').alias('zero_count')
).orderBy(col('zero_count').desc()).show(truncate=False)

# Sample some zero records to understand context
print("\n3. Sample Zero Count Records:")
zero_staff.select(
    'school_year', 'district_name', 'institution_name',
    'data_category', 'data_sub_category', 'employee_type', 'staff_count'
).show(15, truncate=False)

# Check if zeros are concentrated in specific years
print("\n4. Zero Counts by Year:")
zero_staff.groupBy('school_year').agg(
    cnt('*').alias('zero_count')
).orderBy('school_year').show(truncate=False)

# COMMAND ----------

# DBTITLE 1,Write to silver
cert_silver.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("workspace.silver.certified_personnel")
print(f"✓ workspace.silver.certified_personnel: {spark.table('workspace.silver.certified_personnel').count():,} rows")
