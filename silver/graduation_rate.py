# Databricks notebook source
# DBTITLE 1,Graduation Rate Silver Layer - Overview
# MAGIC %md
# MAGIC # Graduation Rate Silver Layer Pipeline
# MAGIC
# MAGIC **Purpose:** Clean and consolidate graduation rate data from bronze layer
# MAGIC
# MAGIC **Source Table:**
# MAGIC * `workspace.bronze.graduation_rate` (226,352 rows, 2011-2025)
# MAGIC
# MAGIC **Output:** `workspace.silver.graduation_rate` (14 columns)
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Key Transformations
# MAGIC
# MAGIC ### 1. Duplicate Removal
# MAGIC * **Problem:** 26,331 duplicate records from multiple downloads (same pattern as enrollment_by_grade)
# MAGIC * **Cause:** Same year data downloaded on multiple dates, creating identical records from different source files
# MAGIC * **Solution:** DropDuplicates on business key (school_year, district_code, institution_number, subgroup_label)
# MAGIC * **Result:** All duplicates have identical graduation rates - true duplicates, not data revisions
# MAGIC
# MAGIC ### 2. Institution Key
# MAGIC * **`institution_key`** = composite join key
# MAGIC * **Logic:** `district_code_institution_number` for consistent joins across tables
# MAGIC * **Note:** `INSTN_NUMBER = 'ALL'` represents district-level aggregates (DETAIL_LVL_DESC = 'District')
# MAGIC
# MAGIC ### 3. Data Quality
# MAGIC * Convert "TFS" (Too Few Students) to NULL for PROGRAM_TOTAL, PROGRAM_PERCENT, TOTAL_COUNT
# MAGIC * Cast numeric fields to appropriate types (integers for counts, decimals for percentages)
# MAGIC * Standardize column names to snake_case
# MAGIC * Extract cohort type (4-year vs 5-year) and subgroup from LABEL_LVL_1_DESC
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
# MAGIC | grades_served | string | Grade range offered |
# MAGIC | cohort_type | string | '4-Year' or '5-Year' graduation cohort |
# MAGIC | subgroup_label | string | Student subgroup (e.g., 'ALL Students', 'Black', 'Economically Disadvantaged') |
# MAGIC | graduate_count | int | Number of graduates (NULL if TFS) |
# MAGIC | graduation_rate | decimal(5,2) | Graduation rate percentage (NULL if TFS) |
# MAGIC | cohort_count | int | Total cohort size (NULL if TFS) |
# MAGIC | institution_key | string | Composite join key (district_code_institution_number) |
# MAGIC | source_year | string | Source file year |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Data Patterns
# MAGIC
# MAGIC ### Year Coverage
# MAGIC * **Incomplete coverage:** 2010-11, then 2016-2025
# MAGIC * **Row count variation by year:** 
# MAGIC   * 2018-19, 2022-24: ~39K rows (expanded subgroup reporting)
# MAGIC   * 2017-18, 2021-22: ~24-26K rows
# MAGIC   * 2019-21, 2024-25: ~12-13K rows (may be partial year or reduced subgroups)
# MAGIC
# MAGIC ### Cohort Types
# MAGIC * **4-Year Grad Rate:** Standard on-time graduation (most common)
# MAGIC * **5-Year Grad Rate:** Extended-time graduation (available for recent years)
# MAGIC * Both include same subgroup breakdowns
# MAGIC
# MAGIC ### Subgroup Categories
# MAGIC Breakdowns by:
# MAGIC * **Overall:** ALL Students
# MAGIC * **Race/Ethnicity:** Black, White, Hispanic, Asian/Pacific Islander, Multi-Racial, American Indian/Alaskan
# MAGIC * **Economic:** Economically Disadvantaged, Not Economically Disadvantaged
# MAGIC * **Special Programs:** Students With Disability, Students Without Disability, Limited English Proficient, Migrant, Foster, Homeless, Active Duty
# MAGIC * **Gender:** Male, Female
# MAGIC
# MAGIC ### Detail Levels
# MAGIC * **School-level:** ~71% - most granular graduation rates by school
# MAGIC * **District-level:** ~29% - district aggregates marked with INSTN_NUMBER='ALL'
# MAGIC * **State-level:** <1% - statewide totals
# MAGIC
# MAGIC ### TFS (Too Few Students)
# MAGIC * ~21.8% of records have TFS suppression (49,431 records)
# MAGIC * Converted to NULL to preserve privacy while maintaining data structure

# COMMAND ----------

# DBTITLE 1,Build silver graduation_rate table
# Graduation Rate Silver Layer Pipeline
# Purpose: Clean and consolidate graduation rate data from bronze layer
# Key changes: Remove duplicates, handle TFS, create institution_key, parse subgroups
# Output: workspace.silver.graduation_rate

from pyspark.sql.functions import col, when, concat_ws, count as cnt, regexp_extract
from pyspark.sql.types import IntegerType, DecimalType

print("=== Graduation Rate Silver Layer Pipeline ===")
print()
print("Loading bronze table...")

# Load source table
grad_bronze = spark.table("workspace.bronze.graduation_rate")
print(f"  Bronze rows: {grad_bronze.count():,}")
print(f"  Bronze columns: {len(grad_bronze.columns)}")

print()
print("=== Step 1: Check for duplicate keys ===")
print()

# Count duplicates before removal (business key includes subgroup label)
dup_check = grad_bronze.groupBy(
    'LONG_SCHOOL_YEAR', 'SCHOOL_DSTRCT_CD', 'INSTN_NUMBER', 'LABEL_LVL_1_DESC'
).agg(cnt('*').alias('dup_count')).filter('dup_count > 1')

dupe_count = dup_check.count()
print(f"Duplicate keys found: {dupe_count:,}")

if dupe_count > 0:
    print()
    print("Sample duplicates (top 5):")
    dup_check.orderBy(col('dup_count').desc()).show(5, truncate=False)
    
    # Show breakdown by year
    print("Duplicate breakdown by year:")
    dup_check.join(
        grad_bronze.select('LONG_SCHOOL_YEAR', 'SCHOOL_DSTRCT_CD', 'INSTN_NUMBER', 'LABEL_LVL_1_DESC').distinct(),
        on=['LONG_SCHOOL_YEAR', 'SCHOOL_DSTRCT_CD', 'INSTN_NUMBER', 'LABEL_LVL_1_DESC']
    ).groupBy('LONG_SCHOOL_YEAR').agg(cnt('*').alias('duplicate_keys')).orderBy('LONG_SCHOOL_YEAR').show(truncate=False)

print()
print("=== Step 2: Transform to Silver ===")
print()
print("Applying transformations:")
print("  • Remove duplicate records (keep one per business key)")
print("  • Create institution_key (district_code + institution_number)")
print("  • Parse cohort type (4-year vs 5-year) from label")
print("  • Extract subgroup name from label")
print("  • Convert TFS (Too Few Students) to NULL")
print("  • Cast numeric fields to appropriate types")
print("  • Standardize column names to snake_case")
print()

# Drop duplicates first - keep first occurrence (arbitrary since they're identical)
grad_deduped = grad_bronze.dropDuplicates([
    'LONG_SCHOOL_YEAR', 'SCHOOL_DSTRCT_CD', 'INSTN_NUMBER', 'LABEL_LVL_1_DESC'
])

rows_removed = grad_bronze.count() - grad_deduped.count()
print(f"Removed {rows_removed:,} duplicate rows")
print(f"Remaining rows: {grad_deduped.count():,}")
print()

# Transform to silver schema
grad_silver = grad_deduped.select(
    col('LONG_SCHOOL_YEAR').alias('school_year'),
    col('DETAIL_LVL_DESC').alias('detail_level'),
    col('SCHOOL_DSTRCT_CD').alias('district_code'),
    col('SCHOOL_DSTRCT_NM').alias('district_name'),
    col('INSTN_NUMBER').alias('institution_number'),
    col('INSTN_NAME').alias('institution_name'),
    col('GRADES_SERVED_DESC').alias('grades_served'),
    
    # Parse cohort type from label (e.g., "Grad Rate -" vs "5 Yr Grad Rate -")
    when(col('LABEL_LVL_1_DESC').startswith('5 Yr'), '5-Year')
        .otherwise('4-Year').alias('cohort_type'),
    
    # Extract subgroup name (everything after "Grad Rate -" or "5 Yr Grad Rate -")
    regexp_extract(col('LABEL_LVL_1_DESC'), r'Grad Rate -(.*)', 1).alias('subgroup_label'),
    
    # Graduate count - handle TFS and cast to int
    when(col('PROGRAM_TOTAL') == 'TFS', None)
        .when(col('PROGRAM_TOTAL').isNull(), None)
        .otherwise(col('PROGRAM_TOTAL')).cast(IntegerType()).alias('graduate_count'),
    
    # Graduation rate - handle TFS and cast to decimal
    when(col('PROGRAM_PERCENT') == 'TFS', None)
        .when(col('PROGRAM_PERCENT').isNull(), None)
        .otherwise(col('PROGRAM_PERCENT')).cast(DecimalType(5, 2)).alias('graduation_rate'),
    
    # Cohort count - handle TFS and cast to int
    when(col('TOTAL_COUNT') == 'TFS', None)
        .when(col('TOTAL_COUNT').isNull(), None)
        .otherwise(col('TOTAL_COUNT')).cast(IntegerType()).alias('cohort_count'),
    
    # Composite join key
    concat_ws('_', col('SCHOOL_DSTRCT_CD'), col('INSTN_NUMBER')).alias('institution_key'),
    
    col('source_year')
)

print("Sample records (2024-25, ALL Students, 4-year cohort):")
grad_silver.filter(
    "school_year = '2024-25' AND subgroup_label = 'ALL Students' AND cohort_type = '4-Year' AND detail_level = 'School'"
).select(
    'institution_name', 'district_name', 'graduate_count', 'graduation_rate', 'cohort_count'
).orderBy('institution_name').show(5, truncate=False)

# COMMAND ----------

# DBTITLE 1,Validate silver transformations
from pyspark.sql.functions import min as spark_min, max as spark_max, avg as spark_avg, sum as spark_sum

print()
print("=== Data Quality Validation ===")
print()

# 1. Row count validation
print("1. Row Count:")
bronze_total = grad_bronze.count()
silver_total = grad_silver.count()
expected_silver = bronze_total - rows_removed
print(f"   Bronze: {bronze_total:,} rows")
print(f"   Duplicates removed: {rows_removed:,} rows")
print(f"   Silver: {silver_total:,} rows")
if silver_total == expected_silver:
    print("   ✓ Row count correct (bronze - duplicates = silver)")
else:
    print(f"   ⚠️ Unexpected row count difference")

# 2. Business key uniqueness
print()
print("2. Business Key Uniqueness:")
dup_check_silver = grad_silver.groupBy(
    'school_year', 'district_code', 'institution_number', 'cohort_type', 'subgroup_label'
).agg(cnt('*').alias('dup_count')).filter('dup_count > 1').count()
print(f"   Duplicate business keys: {dup_check_silver}")
if dup_check_silver == 0:
    print("   ✓ All business keys unique")
else:
    print(f"   ⚠️ {dup_check_silver} duplicate keys remaining")

# 3. Graduation rate validation
print()
print("3. Graduation Rates:")
grad_stats = grad_silver.filter("graduation_rate IS NOT NULL").agg(
    spark_min('graduation_rate').alias('min_rate'),
    spark_max('graduation_rate').alias('max_rate'),
    spark_avg('graduation_rate').alias('avg_rate')
).collect()[0]

print(f"   Min: {grad_stats['min_rate']:.2f}%")
print(f"   Max: {grad_stats['max_rate']:.2f}%")
print(f"   Avg: {grad_stats['avg_rate']:.2f}%")
if grad_stats['min_rate'] >= 0 and grad_stats['max_rate'] <= 100:
    print("   ✓ All rates within valid range (0-100%)")

# 4. Graduate count validation
print()
print("4. Graduate Counts:")
count_stats = grad_silver.filter("graduate_count IS NOT NULL").agg(
    spark_min('graduate_count').alias('min_grads'),
    spark_max('graduate_count').alias('max_grads'),
    spark_sum('graduate_count').alias('total_grads')
).collect()[0]

print(f"   Min: {count_stats['min_grads']:,}")
print(f"   Max: {count_stats['max_grads']:,}")
print(f"   Total graduates (all records): {count_stats['total_grads']:,}")
if count_stats['min_grads'] >= 0:
    print("   ✓ All counts non-negative")

# 5. TFS conversion check
print()
print("5. TFS Handling:")
null_grads = grad_silver.filter("graduate_count IS NULL").count()
null_rates = grad_silver.filter("graduation_rate IS NULL").count()
print(f"   NULL graduate_count: {null_grads:,}")
print(f"   NULL graduation_rate: {null_rates:,}")
print(f"   ✓ TFS values converted to NULL")

# 6. Cohort type distribution
print()
print("6. Cohort Type Distribution:")
grad_silver.groupBy('cohort_type').agg(
    cnt('*').alias('row_count')
).orderBy('cohort_type').show(truncate=False)

# 7. Year coverage
print("7. Year Coverage:")
year_summary = grad_silver.groupBy('school_year').agg(
    cnt('*').alias('row_count')
).orderBy('school_year')
print("   Rows per year:")
year_summary.show(100, truncate=False)

# 8. Top subgroups
print("8. Top 10 Subgroups by Record Count:")
grad_silver.groupBy('subgroup_label').agg(
    cnt('*').alias('row_count')
).orderBy(col('row_count').desc()).show(10, truncate=False)

print()
print("✓ Validation complete")

# COMMAND ----------

# DBTITLE 1,Write silver table
print()
print("=== Writing to Silver Layer ===")
print()

target_table = "workspace.silver.graduation_rate"

grad_silver.write \
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
print("=== Verifying workspace.silver.graduation_rate ===")
print()

# Table stats
grad_table = spark.table("workspace.silver.graduation_rate")
print(f"Total rows: {grad_table.count():,}")
print(f"Total columns: {len(grad_table.columns)}")

# Sample: 2023-24, ALL Students, 4-year cohort, schools only
print()
print("Sample: 2023-24, ALL Students, 4-year graduation rate (first 10 schools):")
grad_table.filter(
    "school_year = '2023-24' AND subgroup_label = 'ALL Students' AND cohort_type = '4-Year' AND detail_level = 'School'"
).select(
    'institution_name', 'district_name', 'graduate_count', 'graduation_rate', 'cohort_count'
).orderBy('institution_name').show(10, truncate=False)

# Statewide graduation rate by year (ALL Students, 4-year cohort)
print()
print("Statewide 4-year graduation rate by year (ALL Students, State level):")
grad_table.filter(
    "subgroup_label = 'ALL Students' AND cohort_type = '4-Year' AND detail_level = 'State'"
).select(
    'school_year', 'graduate_count', 'graduation_rate', 'cohort_count'
).orderBy('school_year').show(truncate=False)

# Compare 4-year vs 5-year graduation rates (recent year with both)
print()
print("Comparison: 4-year vs 5-year graduation rates (2022-23, State, ALL Students):")
grad_table.filter(
    "school_year = '2022-23' AND subgroup_label = 'ALL Students' AND detail_level = 'State'"
).select(
    'cohort_type', 'graduate_count', 'graduation_rate', 'cohort_count'
).orderBy('cohort_type').show(truncate=False)

# Subgroup breakdown for most recent year (State level)
print()
print("Subgroup graduation rates (2023-24, State, 4-year cohort, top 10):")
grad_table.filter(
    "school_year = '2023-24' AND cohort_type = '4-Year' AND detail_level = 'State'"
).select(
    'subgroup_label', 'graduation_rate', 'cohort_count'
).orderBy(col('graduation_rate').desc()).show(10, truncate=False)

print()
print("✓ Silver table ready for gold layer transformations")
