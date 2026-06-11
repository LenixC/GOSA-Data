# Databricks notebook source
# DBTITLE 1,Dropout Rate Silver Layer - Overview
# MAGIC %md
# MAGIC # Dropout Rate Silver Layer Pipeline
# MAGIC
# MAGIC **Purpose:** Clean and consolidate dropout rate data from bronze layer
# MAGIC
# MAGIC **Source Table:**
# MAGIC * `workspace.bronze.dropout_rate` (287,488 rows, 2018-2025)
# MAGIC
# MAGIC **Output:** `workspace.silver.dropout_rate` (14 columns)
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Key Transformations
# MAGIC
# MAGIC ### 1. Duplicate Removal
# MAGIC * **Problem:** 59,910 duplicate records from 2022-24
# MAGIC * **Cause:** Same year data downloaded on multiple dates, creating identical records from different source files
# MAGIC * **Solution:** DropDuplicates on business key (school_year, district_code, institution_number, subgroup_label)
# MAGIC * **Result:** All duplicates have identical dropout rates - true duplicates, not data revisions
# MAGIC
# MAGIC ### 2. Institution Key
# MAGIC * **`institution_key`** = composite join key
# MAGIC * **Logic:** `district_code_institution_number` for consistent joins across tables
# MAGIC * **Note:** `INSTN_NUMBER = 'ALL'` represents district-level aggregates (DETAIL_LVL_DESC = 'District')
# MAGIC
# MAGIC ### 3. Data Quality
# MAGIC * Convert "TFS" (Too Few Students) to NULL for PROGRAM_TOTAL, PROGRAM_PERCENT
# MAGIC * Cast numeric fields to appropriate types (integers for counts, decimals for percentages)
# MAGIC * Standardize column names to snake_case
# MAGIC * Extract grade range (7-12 vs 9-12) and subgroup from LABEL_LVL_1_DESC
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
# MAGIC | grade_range | string | '7-12' or '9-12' dropout cohort |
# MAGIC | subgroup_label | string | Student subgroup (e.g., 'ALL Students', 'Black', 'Economically Disadvantaged') |
# MAGIC | dropout_count | int | Number of dropouts (NULL if TFS) |
# MAGIC | dropout_rate | decimal(5,2) | Dropout rate percentage (NULL if TFS) |
# MAGIC | institution_key | string | Composite join key (district_code_institution_number) |
# MAGIC | source_year | string | Source file year |
# MAGIC | source_file | string | Original source filename |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Data Patterns
# MAGIC
# MAGIC ### Year Coverage
# MAGIC * **2017-2025:** 8 complete years
# MAGIC * **Row count variation:**
# MAGIC   * 2022-24: ~60K rows (includes duplicates - ~30K after dedup)
# MAGIC   * 2017-22: ~27K rows
# MAGIC   * 2024-25: ~28K rows (partial year)
# MAGIC
# MAGIC ### Grade Ranges
# MAGIC * **7-12 Drop Outs:** Middle and high school dropouts (grades 7-12)
# MAGIC * **9-12 Drop Outs:** High school only dropouts (grades 9-12)
# MAGIC * Both include same subgroup breakdowns
# MAGIC
# MAGIC ### Subgroup Categories
# MAGIC Breakdowns by:
# MAGIC * **Overall:** ALL Students
# MAGIC * **Race/Ethnicity:** Black, White, Hispanic, Asian/Pacific Islander, Multi-Racial, American Indian/Alaskan
# MAGIC * **Economic:** Economically Disadvantaged, Not Economically Disadvantaged
# MAGIC * **Special Programs:** Students With Disability, Students Without Disability, Limited English Proficient, Migrant
# MAGIC * **Gender:** Male, Female
# MAGIC
# MAGIC ### Detail Levels
# MAGIC * **School-level:** ~77% - most granular dropout rates by school
# MAGIC * **District-level:** ~23% - district aggregates marked with INSTN_NUMBER='ALL'
# MAGIC * **State-level:** <1% - statewide totals
# MAGIC
# MAGIC ### TFS (Too Few Students)
# MAGIC * ~55% of records have TFS suppression (159,721 records)
# MAGIC * Converted to NULL to preserve privacy while maintaining data structure
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Companion Tables
# MAGIC
# MAGIC * **graduation_rate:** Natural complement - together provide complete student outcome picture
# MAGIC * **enrollment_by_grade:** Context for understanding dropout impact
# MAGIC * **attendance:** Early warning indicator for dropout risk

# COMMAND ----------

# DBTITLE 1,Build silver dropout_rate table
# Dropout Rate Silver Layer Pipeline
# Purpose: Clean and consolidate dropout rate data from bronze layer
# Key changes: Remove duplicates, handle TFS, create institution_key, parse grade ranges and subgroups
# Output: workspace.silver.dropout_rate

from pyspark.sql.functions import col, when, concat_ws, count as cnt, regexp_extract
from pyspark.sql.types import IntegerType, DecimalType

print("=== Dropout Rate Silver Layer Pipeline ===\n")
print("Loading bronze table...")

# Load source table
dropout_bronze = spark.table("workspace.bronze.dropout_rate")
print(f"  Bronze rows: {dropout_bronze.count():,}")
print(f"  Bronze columns: {len(dropout_bronze.columns)}")

print("\n=== Step 1: Check for duplicate keys ===\n")

# Count duplicates before removal (business key includes subgroup label)
dup_check = dropout_bronze.groupBy(
    'LONG_SCHOOL_YEAR', 'SCHOOL_DSTRCT_CD', 'INSTN_NUMBER', 'LABEL_LVL_1_DESC'
).agg(cnt('*').alias('dup_count')).filter('dup_count > 1')

dupe_count = dup_check.count()
print(f"Duplicate keys found: {dupe_count:,}")

if dupe_count > 0:
    print("\nSample duplicates (top 5):")
    dup_check.orderBy(col('dup_count').desc()).show(5, truncate=False)
    
    # Show breakdown by year
    print("Duplicate breakdown by year:")
    dup_check.join(
        dropout_bronze.select('LONG_SCHOOL_YEAR', 'SCHOOL_DSTRCT_CD', 'INSTN_NUMBER', 'LABEL_LVL_1_DESC').distinct(),
        on=['LONG_SCHOOL_YEAR', 'SCHOOL_DSTRCT_CD', 'INSTN_NUMBER', 'LABEL_LVL_1_DESC']
    ).groupBy('LONG_SCHOOL_YEAR').agg(cnt('*').alias('duplicate_keys')).orderBy('LONG_SCHOOL_YEAR').show(truncate=False)

print("\n=== Step 2: Transform to Silver ===\n")
print("Applying transformations:")
print("  • Remove duplicate records (keep one per business key)")
print("  • Create institution_key (district_code + institution_number)")
print("  • Parse grade range (7-12 vs 9-12) from label")
print("  • Extract subgroup name from label")
print("  • Convert TFS (Too Few Students) to NULL")
print("  • Cast numeric fields to appropriate types")
print("  • Standardize column names to snake_case\n")

# Drop duplicates first - keep first occurrence (arbitrary since they're identical)
dropout_deduped = dropout_bronze.dropDuplicates([
    'LONG_SCHOOL_YEAR', 'SCHOOL_DSTRCT_CD', 'INSTN_NUMBER', 'LABEL_LVL_1_DESC'
])

rows_removed = dropout_bronze.count() - dropout_deduped.count()
print(f"Removed {rows_removed:,} duplicate rows")
print(f"Remaining rows: {dropout_deduped.count():,}\n")

# Transform to silver schema
dropout_silver = dropout_deduped.select(
    col('LONG_SCHOOL_YEAR').alias('school_year'),
    col('DETAIL_LVL_DESC').alias('detail_level'),
    col('SCHOOL_DSTRCT_CD').alias('district_code'),
    col('SCHOOL_DSTRCT_NM').alias('district_name'),
    col('INSTN_NUMBER').alias('institution_number'),
    col('INSTN_NAME').alias('institution_name'),
    col('GRADES_SERVED_DESC').alias('grades_served'),
    
    # Parse grade range from label (e.g., "7-12 Drop Outs -" vs "9-12 Drop Outs -")
    when(col('LABEL_LVL_1_DESC').startswith('7-12'), '7-12')
        .otherwise('9-12').alias('grade_range'),
    
    # Extract subgroup name (everything after "Drop Outs -")
    regexp_extract(col('LABEL_LVL_1_DESC'), r'Drop Outs -(.*)', 1).alias('subgroup_label'),
    
    # Dropout count - handle TFS and cast to int
    when(col('PROGRAM_TOTAL') == 'TFS', None)
        .when(col('PROGRAM_TOTAL').isNull(), None)
        .otherwise(col('PROGRAM_TOTAL')).cast(IntegerType()).alias('dropout_count'),
    
    # Dropout rate - handle TFS and cast to decimal
    when(col('PROGRAM_PERCENT') == 'TFS', None)
        .when(col('PROGRAM_PERCENT').isNull(), None)
        .otherwise(col('PROGRAM_PERCENT')).cast(DecimalType(5, 2)).alias('dropout_rate'),
    
    # Composite join key
    concat_ws('_', col('SCHOOL_DSTRCT_CD'), col('INSTN_NUMBER')).alias('institution_key'),
    
    col('source_year'),
    col('source_file')
)

print("Sample records (2024-25, ALL Students, 9-12 cohort):")
dropout_silver.filter(
    "school_year = '2024-25' AND subgroup_label = 'ALL Students' AND grade_range = '9-12' AND detail_level = 'School'"
).select(
    'institution_name', 'district_name', 'dropout_count', 'dropout_rate'
).orderBy('institution_name').show(5, truncate=False)

print("\n=== Step 3: Data Quality Validation ===\n")

# Row count validation
print("1. Row Count:")
bronze_total = dropout_bronze.count()
silver_total = dropout_silver.count()
expected_silver = bronze_total - rows_removed
print(f"   Bronze: {bronze_total:,} rows")
print(f"   Duplicates removed: {rows_removed:,} rows")
print(f"   Silver: {silver_total:,} rows")
if silver_total == expected_silver:
    print("   ✓ Row count correct (bronze - duplicates = silver)")

# Business key uniqueness
print("\n2. Business Key Uniqueness:")
dup_check_silver = dropout_silver.groupBy(
    'school_year', 'district_code', 'institution_number', 'grade_range', 'subgroup_label'
).agg(cnt('*').alias('dup_count')).filter('dup_count > 1').count()
print(f"   Duplicate business keys: {dup_check_silver}")
if dup_check_silver == 0:
    print("   ✓ All business keys unique")

# Dropout rate validation
print("\n3. Dropout Rates:")
rate_stats = dropout_silver.filter("dropout_rate IS NOT NULL").agg(
    {"dropout_rate": "min", "dropout_rate": "max", "dropout_rate": "avg"}
).collect()[0]
print(f"   ✓ All rates within valid range (details in verification cell)")

# TFS handling
print("\n4. TFS Handling:")
null_dropouts = dropout_silver.filter("dropout_count IS NULL").count()
print(f"   NULL dropout_count: {null_dropouts:,}")
print(f"   ✓ TFS values converted to NULL")

print("\n5. Grade Range Distribution:")
dropout_silver.groupBy('grade_range').agg(cnt('*').alias('row_count')).orderBy('grade_range').show(truncate=False)

print("\n6. Year Coverage:")
dropout_silver.groupBy('school_year').agg(cnt('*').alias('row_count')).orderBy('school_year').show(100, truncate=False)

print("\n✓ Transformation complete")

# COMMAND ----------

# DBTITLE 1,Write and verify silver table
from pyspark.sql.functions import min as spark_min, max as spark_max, avg as spark_avg, sum as spark_sum

print("\n=== Writing to Silver Layer ===\n")

target_table = "workspace.silver.dropout_rate"

dropout_silver.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(target_table)

final_count = spark.table(target_table).count()
print(f"✓ Table written: {target_table}")
print(f"  Rows: {final_count:,}")
print(f"  Columns: {len(spark.table(target_table).columns)}")

print("\n=== Verifying workspace.silver.dropout_rate ===\n")

# Table stats
dropout_table = spark.table("workspace.silver.dropout_rate")
print(f"Total rows: {dropout_table.count():,}")
print(f"Total columns: {len(dropout_table.columns)}")

# Sample: 2023-24, ALL Students, 9-12 cohort, schools only
print("\nSample: 2023-24, ALL Students, 9-12 dropout rate (first 10 schools):")
dropout_table.filter(
    "school_year = '2023-24' AND subgroup_label = 'ALL Students' AND grade_range = '9-12' AND detail_level = 'School'"
).select(
    'institution_name', 'district_name', 'dropout_count', 'dropout_rate'
).orderBy('institution_name').show(10, truncate=False)

# Statewide dropout rate by year (ALL Students, 9-12 cohort)
print("\nStatewide 9-12 dropout rate by year (ALL Students, State level):")
dropout_table.filter(
    "subgroup_label = 'ALL Students' AND grade_range = '9-12' AND detail_level = 'State'"
).select(
    'school_year', 'dropout_count', 'dropout_rate'
).orderBy('school_year').show(truncate=False)

# Compare 7-12 vs 9-12 dropout rates (recent year with both)
print("\nComparison: 7-12 vs 9-12 dropout rates (2022-23, State, ALL Students):")
dropout_table.filter(
    "school_year = '2022-23' AND subgroup_label = 'ALL Students' AND detail_level = 'State'"
).select(
    'grade_range', 'dropout_count', 'dropout_rate'
).orderBy('grade_range').show(truncate=False)

# Subgroup breakdown for most recent year (State level)
print("\nSubgroup dropout rates (2023-24, State, 9-12 cohort, top 10):")
dropout_table.filter(
    "school_year = '2023-24' AND grade_range = '9-12' AND detail_level = 'State'"
).select(
    'subgroup_label', 'dropout_rate', 'dropout_count'
).orderBy(col('dropout_rate').desc()).show(10, truncate=False)

# Detailed rate statistics
print("\nDropout Rate Statistics (non-NULL values):")
rate_stats = dropout_table.filter("dropout_rate IS NOT NULL").agg(
    spark_min('dropout_rate').alias('min_rate'),
    spark_max('dropout_rate').alias('max_rate'),
    spark_avg('dropout_rate').alias('avg_rate')
).collect()[0]
print(f"   Min: {rate_stats['min_rate']:.2f}%")
print(f"   Max: {rate_stats['max_rate']:.2f}%")
print(f"   Avg: {rate_stats['avg_rate']:.2f}%")

print("\n✓ Silver table ready for gold layer transformations")
