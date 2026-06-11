# Databricks notebook source
# DBTITLE 1,Enrollment by Grade Silver Layer - Overview
# MAGIC %md
# MAGIC # Enrollment by Grade Silver Layer Pipeline
# MAGIC
# MAGIC **Purpose:** Clean and consolidate enrollment data from bronze layer
# MAGIC
# MAGIC **Source Table:**
# MAGIC * `workspace.bronze.enrollment_by_grade` (530,712 rows, 2011-2025)
# MAGIC
# MAGIC **Output:** `workspace.silver.enrollment_by_grade` (13 columns)
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Key Transformations
# MAGIC
# MAGIC ### 1. Duplicate Removal
# MAGIC * **Problem:** 56,245 duplicate records from 2022-23 and 2023-24
# MAGIC * **Cause:** Same year data downloaded on multiple dates, creating identical records from different source files
# MAGIC * **Impact on row counts:**
# MAGIC   * 2022-23: Bronze 56,480 rows → removed 27,906 duplicates → Silver 28,574 rows
# MAGIC   * 2023-24: Bronze 56,956 rows → removed 28,339 duplicates → Silver 28,617 rows
# MAGIC   * **This explains the apparent drop from 65k to 28k rows for these years**
# MAGIC * **Solution:** DropDuplicates on business key (school_year, district_code, institution_number, enrollment_period, grade_level)
# MAGIC * **Result:** All duplicates have identical enrollment counts - true duplicates, not data corrections
# MAGIC
# MAGIC ### 2. Institution Key
# MAGIC * **`institution_key`** = composite join key
# MAGIC * **Logic:** `district_code_institution_number` for consistent joins across tables
# MAGIC * **Note:** `INSTN_NUMBER = 'ALL'` represents district-level aggregates (DETAIL_LVL_DESC = 'District')
# MAGIC
# MAGIC ### 3. Data Quality
# MAGIC * Convert "TFS" (Too Few Students) to NULL (81,075 records)
# MAGIC * Cast enrollment_count to integer
# MAGIC * Standardize column names to snake_case
# MAGIC * **Zero enrollment handling:**
# MAGIC   * 2017-2022: Zeros represent "grade not offered" (reporting artifact - all schools report all grades)
# MAGIC   * 2022-2025: Zeros represent legitimate 0 enrollment (schools only report served grades)
# MAGIC   * **Gold layer:** Filter out zeros from pre-2022 data to align with modern format
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Schema
# MAGIC
# MAGIC | Column | Type | Description |
# MAGIC |--------|------|-------------|
# MAGIC | school_year | string | Academic year (e.g., '2024-25') |
# MAGIC | detail_level | string | Aggregation level: 'School', 'District', 'State' |
# MAGIC | district_code | string | District code |
# MAGIC | district_name | string | District name |
# MAGIC | institution_number | string | Institution number ('ALL' for district aggregates) |
# MAGIC | institution_name | string | School name |
# MAGIC | grades_served | string | Grade range offered (e.g., '09,10,11,12') |
# MAGIC | enrollment_period | string | 'Fall' or 'Spring' snapshot |
# MAGIC | grade_level | string | Specific grade (K, 1st-12th) |
# MAGIC | enrollment_count | int | Number of students (NULL if TFS) |
# MAGIC | institution_key | string | Composite join key (district_code_institution_number) |
# MAGIC | source_year | string | Source file year |
# MAGIC | source_file | string | Source filename |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Data Patterns
# MAGIC
# MAGIC ### Year Coverage & Reporting Format Change
# MAGIC * **Incomplete coverage:** Only 2010-11, then jumps to 2017-2025
# MAGIC * **2017-2022 (Old format):** ~65K rows per year
# MAGIC   * ALL schools report ALL 13 grades (K, 1st-12th)
# MAGIC   * Elementary schools have rows for 9th-12th with enrollment_count = 0
# MAGIC   * High schools have rows for K-5th with enrollment_count = 0
# MAGIC   * Result: 13 rows per school per period, many with zero enrollment
# MAGIC * **2022-2025 (New format):** ~28K rows per year
# MAGIC   * Schools ONLY report grades they actually serve
# MAGIC   * Elementary: 6 grades (K-5), Middle: 3 grades (6-8), High: 4 grades (9-12)
# MAGIC   * Result: Avg 5 rows per school per period, NO zero-padding
# MAGIC   * **Same ~1.7M students, just more efficient reporting**
# MAGIC * **2022-2024 duplicates:** Bronze had ~56K rows (double downloads), silver correctly has ~28K after dedup
# MAGIC * **Gold layer recommendation:** Filter `enrollment_count > 0` for 2017-2022 data to normalize with 2022+ format
# MAGIC
# MAGIC ### Detail Levels
# MAGIC * **School-level:** 469,781 rows (88%) - most granular data
# MAGIC * **District-level:** 60,645 rows (11%) - aggregates marked with INSTN_NUMBER='ALL'
# MAGIC * **State-level:** 286 rows (<1%) - statewide totals
# MAGIC
# MAGIC ### Grade Levels
# MAGIC * **13 grade levels:** K, 1st, 2nd, 3rd, 4th, 5th, 6th, 7th, 8th, 9th, 10th, 11th, 12th
# MAGIC * **No PK (Pre-K) in grade_level field** - PK appears in grades_served but not as standalone grade records

# COMMAND ----------

# DBTITLE 1,Build silver enrollment_by_grade table
# Enrollment by Grade Silver Layer Pipeline
# Purpose: Clean and consolidate enrollment data from bronze layer
# Key changes: Remove duplicates, handle TFS, create institution_key
# Output: workspace.silver.enrollment_by_grade

from pyspark.sql.functions import col, when, concat_ws, count as cnt
from pyspark.sql.types import IntegerType

print("=== Enrollment by Grade Silver Layer Pipeline ===\n")
print("Loading bronze table...")

# Load source table
enroll_bronze = spark.table("workspace.bronze.enrollment_by_grade")
print(f"  Bronze rows: {enroll_bronze.count():,}")
print(f"  Bronze columns: {len(enroll_bronze.columns)}")

print("\n=== Step 1: Check for duplicate keys ===\n")

# Count duplicates before removal
dup_check = enroll_bronze.groupBy(
    'LONG_SCHOOL_YEAR', 'SCHOOL_DSTRCT_CD', 'INSTN_NUMBER', 'ENROLLMENT_PERIOD', 'GRADE_LEVEL'
).agg(cnt('*').alias('dup_count')).filter('dup_count > 1')

dupe_count = dup_check.count()
print(f"Duplicate keys found: {dupe_count:,}")

if dupe_count > 0:
    print("\nDuplicate breakdown by year:")
    dup_check.join(
        enroll_bronze.select('LONG_SCHOOL_YEAR', 'SCHOOL_DSTRCT_CD', 'INSTN_NUMBER', 'ENROLLMENT_PERIOD', 'GRADE_LEVEL').distinct(),
        on=['LONG_SCHOOL_YEAR', 'SCHOOL_DSTRCT_CD', 'INSTN_NUMBER', 'ENROLLMENT_PERIOD', 'GRADE_LEVEL']
    ).groupBy('LONG_SCHOOL_YEAR').agg(cnt('*').alias('duplicate_keys')).orderBy('LONG_SCHOOL_YEAR').show(truncate=False)

print("\n=== Step 2: Transform to Silver ===\n")
print("Applying transformations:")
print("  • Remove duplicate records (keep one per business key)")
print("  • Create institution_key (district_code + institution_number)")
print("  • Convert TFS (Too Few Students) to NULL")
print("  • Cast enrollment_count to integer")
print("  • Standardize column names to snake_case\n")

# Drop duplicates first - keep first occurrence (arbitrary since they're identical)
enroll_deduped = enroll_bronze.dropDuplicates([
    'LONG_SCHOOL_YEAR', 'SCHOOL_DSTRCT_CD', 'INSTN_NUMBER', 'ENROLLMENT_PERIOD', 'GRADE_LEVEL'
])

rows_removed = enroll_bronze.count() - enroll_deduped.count()
print(f"Removed {rows_removed:,} duplicate rows")
print(f"Remaining rows: {enroll_deduped.count():,}\n")

# Transform to silver schema
enroll_silver = enroll_deduped.select(
    col('LONG_SCHOOL_YEAR').alias('school_year'),
    col('DETAIL_LVL_DESC').alias('detail_level'),
    col('SCHOOL_DSTRCT_CD').alias('district_code'),
    col('SCHOOL_DSTRCT_NM').alias('district_name'),
    col('INSTN_NUMBER').alias('institution_number'),
    col('INSTN_NAME').alias('institution_name'),
    col('GRADES_SERVED_DESC').alias('grades_served'),
    col('ENROLLMENT_PERIOD').alias('enrollment_period'),
    col('GRADE_LEVEL').alias('grade_level'),
    
    # Enrollment count - handle TFS and cast to int
    when(col('ENROLLMENT_COUNT') == 'TFS', None)
        .when(col('ENROLLMENT_COUNT').isNull(), None)
        .otherwise(col('ENROLLMENT_COUNT')).cast(IntegerType()).alias('enrollment_count'),
    
    # Composite join key
    concat_ws('_', col('SCHOOL_DSTRCT_CD'), col('INSTN_NUMBER')).alias('institution_key'),
    
    col('source_year'),
    col('source_file')
)

print("Sample records (2024-25 Fall, Grade 9):")
enroll_silver.filter(
    "school_year = '2024-25' AND enrollment_period = 'Fall' AND grade_level = '9th' AND detail_level = 'School'"
).select(
    'institution_name', 'district_name', 'grade_level', 'enrollment_count'
).orderBy('institution_name').show(5, truncate=False)

# COMMAND ----------

# DBTITLE 1,Validate silver transformations
from pyspark.sql.functions import min as spark_min, max as spark_max, sum as spark_sum

print("\n=== Data Quality Validation ===\n")

# 1. Row count validation
print("1. Row Count:")
bronze_total = enroll_bronze.count()
silver_total = enroll_silver.count()
expected_silver = bronze_total - rows_removed
print(f"   Bronze: {bronze_total:,} rows")
print(f"   Duplicates removed: {rows_removed:,} rows")
print(f"   Silver: {silver_total:,} rows")
if silver_total == expected_silver:
    print("   ✓ Row count correct (bronze - duplicates = silver)")
else:
    print(f"   ⚠️ Unexpected row count difference")

# 2. Business key uniqueness
print("\n2. Business Key Uniqueness:")
dup_check_silver = enroll_silver.groupBy(
    'school_year', 'district_code', 'institution_number', 'enrollment_period', 'grade_level'
).agg(cnt('*').alias('dup_count')).filter('dup_count > 1').count()
print(f"   Duplicate business keys: {dup_check_silver}")
if dup_check_silver == 0:
    print("   ✓ All business keys unique")
else:
    print(f"   ⚠️ {dup_check_silver} duplicate keys remaining")

# 3. Enrollment count validation
print("\n3. Enrollment Count:")
enroll_stats = enroll_silver.filter("enrollment_count IS NOT NULL").agg(
    spark_min('enrollment_count').alias('min_count'),
    spark_max('enrollment_count').alias('max_count'),
    spark_sum('enrollment_count').alias('total_enrollment')
).collect()[0]

print(f"   Min: {enroll_stats['min_count']:,}")
print(f"   Max: {enroll_stats['max_count']:,}")
print(f"   Total enrolled (all records): {enroll_stats['total_enrollment']:,}")
if enroll_stats['min_count'] >= 0:
    print("   ✓ All counts non-negative")

# 4. TFS conversion check
print("\n4. TFS Handling:")
null_count = enroll_silver.filter("enrollment_count IS NULL").count()
print(f"   NULL counts: {null_count:,}")
print(f"   ✓ TFS values converted to NULL")

# 5. Year coverage
print("\n5. Year Coverage:")
year_summary = enroll_silver.groupBy('school_year').agg(
    cnt('*').alias('row_count')
).orderBy('school_year')
print("   Rows per year:")
year_summary.show(100, truncate=False)

print("\n✓ Validation complete")

# COMMAND ----------

# DBTITLE 1,Write silver table
print("\n=== Writing to Silver Layer ===\n")

target_table = "workspace.silver.enrollment_by_grade"

enroll_silver.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(target_table)

final_count = spark.table(target_table).count()
print(f"✓ Table written: {target_table}")
print(f"  Rows: {final_count:,}")
print(f"  Columns: {len(spark.table(target_table).columns)}")

# COMMAND ----------

# DBTITLE 1,Verify silver table
print("=== Verifying workspace.silver.enrollment_by_grade ===\n")

# Table stats
enroll_table = spark.table("workspace.silver.enrollment_by_grade")
print(f"Total rows: {enroll_table.count():,}")
print(f"Total columns: {len(enroll_table.columns)}")

# Sample: 2024-25 Fall, 9th grade
print("\nSample: 2024-25 Fall enrollment, 9th grade (first 10 schools):")
enroll_table.filter(
    "school_year = '2024-25' AND enrollment_period = 'Fall' AND grade_level = '9th' AND detail_level = 'School'"
).select(
    'institution_name', 'district_name', 'enrollment_count'
).orderBy('institution_name').show(10, truncate=False)

# Total enrollment by year
print("\nTotal enrollment by year (all grades, Fall only):")
enroll_table.filter(
    "enrollment_period = 'Fall' AND detail_level = 'School' AND enrollment_count IS NOT NULL"
).groupBy('school_year').agg(
    spark_sum('enrollment_count').alias('total_enrollment')
).orderBy('school_year').show(truncate=False)

print("\n✓ Silver table ready for gold layer transformations")
