# Databricks notebook source
# DBTITLE 1,SAT Silver Layer - Overview
# MAGIC %md
# MAGIC # SAT Silver Layer Pipeline
# MAGIC
# MAGIC **Purpose:** Consolidate 5 bronze SAT tables into a single, clean silver table
# MAGIC
# MAGIC **Source Tables:**
# MAGIC * `workspace.bronze.sat` (2022-25, 10,918 rows)
# MAGIC * `workspace.bronze.sat_highest` (2010-22, 19,788 rows)
# MAGIC * `workspace.bronze.sat_new_highest` (2015-19, 14,987 rows)
# MAGIC * `workspace.bronze.sat_new_recent` (2015-19, 14,987 rows)
# MAGIC * `workspace.bronze.sat_recent` (2010-22, 19,788 rows)
# MAGIC
# MAGIC **Output:** `workspace.silver.sat` (80,468 rows, 21 columns)
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Key Transformations
# MAGIC
# MAGIC ### 1. SAT Format Classification
# MAGIC * **`sat_format`** column added:
# MAGIC   * `'old'` = Old SAT (2010-2015, 600-2400 scale)
# MAGIC   * `'new'` = New SAT (2016+, 400-1600 scale)
# MAGIC * Determined by test component names
# MAGIC
# MAGIC ### 2. Highest vs Recent Indicator
# MAGIC * **`highest_recent_indicator`** column added:
# MAGIC   * `'Highest'` = Best score student ever achieved
# MAGIC   * `'Recent'` = Most recent score student took
# MAGIC * Derived from source table names for older data
# MAGIC
# MAGIC ### 3. District Code Backfill
# MAGIC * **Problem:** 956 records with NULL district codes in newer files
# MAGIC * **Solution:** Built lookup from non-NULL records in older files
# MAGIC * **Result:** Recovered district codes for 412 of 475 affected schools (87%)
# MAGIC
# MAGIC ### 4. Institution Key
# MAGIC * **`institution_key`** = composite join key
# MAGIC * **Logic:**
# MAGIC   * When district exists: `district_code_institution_number`
# MAGIC   * When NULL district: use `institution_name` (inst numbers not unique)
# MAGIC
# MAGIC ### 5. Data Quality
# MAGIC * Removed 315 duplicate rows from source files
# MAGIC * Converted TFS and N/A suppression codes to NULL
# MAGIC * Cast count and score columns to proper numeric types (double)
# MAGIC * Standardized column names to snake_case
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Schema
# MAGIC
# MAGIC | Column | Type | Description |
# MAGIC |--------|------|-------------|
# MAGIC | school_year | string | Academic year (e.g., '2024-25') |
# MAGIC | institution_number | string | Institution number |
# MAGIC | institution_name | string | School name |
# MAGIC | district_code | string | District code |
# MAGIC | district_name | string | District name |
# MAGIC | subgroup | string | Student subgroup |
# MAGIC | test_component | string | SAT test component |
# MAGIC | **sat_format** | string | 'old' or 'new' SAT format |
# MAGIC | **highest_recent_indicator** | string | 'Highest' or 'Recent' |
# MAGIC | **institution_key** | string | Composite join key |
# MAGIC | national_num_tested | double | Count suppressed as NULL |
# MAGIC | state_num_tested | double | Count suppressed as NULL |
# MAGIC | district_num_tested | double | Count suppressed as NULL |
# MAGIC | institution_num_tested | double | Count suppressed as NULL |
# MAGIC | national_avg_score | double | Score suppressed as NULL |
# MAGIC | state_avg_score | double | Score suppressed as NULL |
# MAGIC | district_avg_score | double | Score suppressed as NULL |
# MAGIC | institution_avg_score | double | Score suppressed as NULL |
# MAGIC | assessment_code | string | Assessment code |
# MAGIC | source_year | string | Source file year |
# MAGIC | source_file | string | Source file name |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Data Patterns & Anomalies (Expected)
# MAGIC
# MAGIC ### 2015-16 Spike (10,678 rows vs ~3,400 typical)
# MAGIC **Cause:** SAT transition year with format overlap
# MAGIC
# MAGIC * **689 schools (94%) took BOTH old and new SAT formats**
# MAGIC * Each school has 4 records instead of 2:
# MAGIC   * Old SAT: Highest + Recent
# MAGIC   * New SAT: Highest + Recent
# MAGIC * This is **legitimate double-counting** reflecting the historical transition
# MAGIC * Georgia DOE reported both formats for comparison during the transition year
# MAGIC
# MAGIC **Breakdown:**
# MAGIC * Old SAT: 3,456 rows (1,728 Highest + 1,728 Recent)
# MAGIC * New SAT: 7,222 rows (3,611 Highest + 3,611 Recent)
# MAGIC
# MAGIC ### Row Count Doubling Post-2016 (~7,600 rows vs ~3,400 pre-2016)
# MAGIC **Cause:** New SAT has 9 test components vs old SAT's 4 components = **2.2x multiplier**
# MAGIC
# MAGIC **Old SAT (4 components):**
# MAGIC * Combined
# MAGIC * Mathematics
# MAGIC * Reading
# MAGIC * Writing
# MAGIC
# MAGIC **New SAT (9 components):**
# MAGIC * Combined Test Score
# MAGIC * Math Section Score - New
# MAGIC * Reading Test Score - New
# MAGIC * WritLang Test Score - New
# MAGIC * Evidence Based Reading and Writing - New
# MAGIC * Essay Total
# MAGIC * Essay Analysis Score - New
# MAGIC * Essay Reading Score - New
# MAGIC * Essay Writing Score - New
# MAGIC
# MAGIC **Result:** More granular reporting = more rows per school. This is **working as designed**.

# COMMAND ----------

# DBTITLE 1,Build silver SAT table
# SAT Silver Layer Pipeline (Revised)
# Purpose: Clean, type, and consolidate SAT test score data from bronze layer
# Key changes: Add sat_format flag, properly handle highest_recent_indicator to avoid duplicates
# Output: workspace.silver.sat

from pyspark.sql.functions import col, when, regexp_replace, concat_ws, lit, count as cnt
from pyspark.sql.types import DoubleType, IntegerType

print("=== SAT Silver Layer Pipeline (Revised) ===\n")
print("Loading bronze tables...")

# Load all 5 SAT tables
sat = spark.table("workspace.bronze.sat")
sat_highest = spark.table("workspace.bronze.sat_highest")
sat_new_highest = spark.table("workspace.bronze.sat_new_highest")
sat_new_recent = spark.table("workspace.bronze.sat_new_recent")
sat_recent = spark.table("workspace.bronze.sat_recent")

print(f"  sat (2022-25): {sat.count():,} rows")
print(f"  sat_highest: {sat_highest.count():,} rows")
print(f"  sat_new_highest: {sat_new_highest.count():,} rows")
print(f"  sat_new_recent: {sat_new_recent.count():,} rows")
print(f"  sat_recent: {sat_recent.count():,} rows")

print("\n=== Step 1: Add highest_recent_indicator and sat_format to each table ===\n")

# sat (2022-25): Already has HIGHEST_RECENT_IND, determine format by test component
print("Transforming sat (2022-25)...")
sat_xf = sat.withColumn(
    'sat_format',
    when(col('TEST_CMPNT_TYP_CD').isin([
        'Combined Test Score', 'Math Section Score - New', 
        'Reading Test  Score - New', 'WritLang Test  Score - New',
        'Evidence Based Reading and Writing - New', 'Essay Total',
        'Essay Analysis Score - New', 'Essay Reading Score - New', 'Essay Writing Score - New'
    ]), lit('new')).otherwise(lit('old'))
)

# sat_highest: Add 'Highest' indicator
print("Transforming sat_highest...")
sat_highest_xf = sat_highest \
    .withColumn('HIGHEST_RECENT_IND', lit('Highest')) \
    .withColumn('sat_format', 
        when(col('TEST_CMPNT_TYP_CD').isin([
            'Combined Test Score', 'Math Section Score - New', 
            'Reading Test  Score - New', 'WritLang Test  Score - New',
            'Essay Total', 'Essay Analysis Score - New', 'Essay Reading Score - New', 'Essay Writing Score - New'
        ]), lit('new')).otherwise(lit('old'))
    )

# sat_recent: Add 'Recent' indicator
print("Transforming sat_recent...")
sat_recent_xf = sat_recent \
    .withColumn('HIGHEST_RECENT_IND', lit('Recent')) \
    .withColumn('sat_format',
        when(col('TEST_CMPNT_TYP_CD').isin([
            'Combined Test Score', 'Math Section Score - New', 
            'Reading Test  Score - New', 'WritLang Test  Score - New',
            'Essay Total', 'Essay Analysis Score - New', 'Essay Reading Score - New', 'Essay Writing Score - New'
        ]), lit('new')).otherwise(lit('old'))
    )

# sat_new_highest: Add 'Highest' indicator (all new format)
print("Transforming sat_new_highest...")
sat_new_highest_xf = sat_new_highest \
    .withColumn('HIGHEST_RECENT_IND', lit('Highest')) \
    .withColumn('sat_format', lit('new'))

# sat_new_recent: Add 'Recent' indicator (all new format)
print("Transforming sat_new_recent...")
sat_new_recent_xf = sat_new_recent \
    .withColumn('HIGHEST_RECENT_IND', lit('Recent')) \
    .withColumn('sat_format', lit('new'))

print("\n=== Step 2: Union all tables ===\n")

# Union all transformed tables
sat_all = sat_xf \
    .unionByName(sat_highest_xf, allowMissingColumns=True) \
    .unionByName(sat_recent_xf, allowMissingColumns=True) \
    .unionByName(sat_new_highest_xf, allowMissingColumns=True) \
    .unionByName(sat_new_recent_xf, allowMissingColumns=True)

print("\n=== Step 2a: Backfill missing district codes ===\n")
print("Building lookup from non-NULL district codes...")

# Build lookup: institution_number + institution_name → district_code
# Using records where district code exists
district_lookup = sat_all.filter(
    "SCHOOL_DISTRCT_CD IS NOT NULL AND INSTN_NUMBER IS NOT NULL AND INSTN_NAME IS NOT NULL"
).select(
    'INSTN_NUMBER', 'INSTN_NAME', 'SCHOOL_DISTRCT_CD', 'SCHOOL_DSTRCT_NM'
).distinct()

print(f"Built lookup with {district_lookup.count():,} unique institution entries")

# Left join to fill in missing district codes
sat_all = sat_all.alias('main').join(
    district_lookup.alias('lookup'),
    on=['INSTN_NUMBER', 'INSTN_NAME'],
    how='left'
).select(
    col('main.*'),
    # Use lookup district code if main is NULL, otherwise keep main
    when(col('main.SCHOOL_DISTRCT_CD').isNull(), col('lookup.SCHOOL_DISTRCT_CD'))
        .otherwise(col('main.SCHOOL_DISTRCT_CD')).alias('SCHOOL_DISTRCT_CD_FILLED'),
    when(col('main.SCHOOL_DSTRCT_NM').isNull(), col('lookup.SCHOOL_DSTRCT_NM'))
        .otherwise(col('main.SCHOOL_DSTRCT_NM')).alias('SCHOOL_DSTRCT_NM_FILLED')
)

# Replace the original district code columns
sat_all = sat_all.drop('SCHOOL_DISTRCT_CD', 'SCHOOL_DSTRCT_NM').withColumnRenamed(
    'SCHOOL_DISTRCT_CD_FILLED', 'SCHOOL_DISTRCT_CD'
).withColumnRenamed(
    'SCHOOL_DSTRCT_NM_FILLED', 'SCHOOL_DSTRCT_NM'
)

nulls_before = sat_all.filter("SCHOOL_DISTRCT_CD IS NULL").count()
print(f"Remaining NULL district codes after backfill: {nulls_before:,}")

total_rows = sat_all.count()
print(f"Total rows after union: {total_rows:,}")

# Check for duplicates
duplicates = sat_all.groupBy(
    'LONG_SCHOOL_YEAR', 'SCHOOL_DISTRCT_CD', 'INSTN_NUMBER', 
    'SUBGRP_DESC', 'TEST_CMPNT_TYP_CD', 'HIGHEST_RECENT_IND'
).agg(cnt('*').alias('row_count')).filter('row_count > 1')

dupe_count = duplicates.count()
if dupe_count > 0:
    print(f"\n⚠️  WARNING: {dupe_count:,} duplicate keys found!")
    duplicates.orderBy('row_count', ascending=False).show(5, truncate=False)
else:
    print("\n✓ No duplicates detected")

print("\n=== Step 2b: Remove true duplicates from source data ===\n")
print("Removing duplicate rows from source files...")

before_dedup = sat_all.count()
sat_all = sat_all.dropDuplicates()
after_dedup = sat_all.count()

print(f"Rows before deduplication: {before_dedup:,}")
print(f"Rows after deduplication: {after_dedup:,}")
print(f"Removed {before_dedup - after_dedup:,} duplicate rows")

print("\n=== Step 3: Apply final transformations ===\n")

# Build silver table with cleaned/typed columns
sat_silver = sat_all.select(
    col('LONG_SCHOOL_YEAR').alias('school_year'),
    col('INSTN_NUMBER').alias('institution_number'),
    col('INSTN_NAME').alias('institution_name'),
    col('SCHOOL_DISTRCT_CD').alias('district_code'),
    col('SCHOOL_DSTRCT_NM').alias('district_name'),
    col('SUBGRP_DESC').alias('subgroup'),
    col('TEST_CMPNT_TYP_CD').alias('test_component'),
    
    # NEW: SAT format and highest/recent indicator
    col('sat_format'),
    col('HIGHEST_RECENT_IND').alias('highest_recent_indicator'),
    
    # Composite join key for institution
    # Use district_code + institution_number when district exists
    # Use institution_name when district is NULL (inst numbers not unique across null districts)
    when(col('SCHOOL_DISTRCT_CD').isNull(), col('INSTN_NAME'))
        .otherwise(concat_ws('_', col('SCHOOL_DISTRCT_CD'), col('INSTN_NUMBER')))
        .alias('institution_key'),
    
    # Counts - cast to double (source has decimal values like 247.5)
    # Handle TFS and N/A suppression codes
    when(col('NATIONAL_NUM_TESTED_CNT').isin('TFS', 'N/A'), None)
        .when(col('NATIONAL_NUM_TESTED_CNT').isNull(), None)
        .otherwise(col('NATIONAL_NUM_TESTED_CNT')).cast(DoubleType()).alias('national_num_tested'),
    
    when(col('STATE_NUM_TESTED_CNT').isin('TFS', 'N/A'), None)
        .when(col('STATE_NUM_TESTED_CNT').isNull(), None)
        .otherwise(col('STATE_NUM_TESTED_CNT')).cast(DoubleType()).alias('state_num_tested'),
    
    when(col('DSTRCT_NUM_TESTED_CNT').isin('TFS', 'N/A'), None)
        .when(col('DSTRCT_NUM_TESTED_CNT').isNull(), None)
        .otherwise(col('DSTRCT_NUM_TESTED_CNT')).cast(DoubleType()).alias('district_num_tested'),
    
    when(col('INSTN_NUM_TESTED_CNT').isin('TFS', 'N/A'), None)
        .when(col('INSTN_NUM_TESTED_CNT').isNull(), None)
        .otherwise(col('INSTN_NUM_TESTED_CNT')).cast(DoubleType()).alias('institution_num_tested'),
    
    # Scores - handle TFS and N/A (NATIONAL_AVG_SCORE_VAL may be NULL in some tables)
    when(col('NATIONAL_AVG_SCORE_VAL').isin('TFS', 'N/A'), None)
        .when(col('NATIONAL_AVG_SCORE_VAL').isNull(), None)
        .otherwise(col('NATIONAL_AVG_SCORE_VAL')).cast(DoubleType()).alias('national_avg_score'),
    
    when(col('STATE_AVG_SCORE_VAL').isin('TFS', 'N/A'), None)
        .when(col('STATE_AVG_SCORE_VAL').isNull(), None)
        .otherwise(col('STATE_AVG_SCORE_VAL')).cast(DoubleType()).alias('state_avg_score'),
    
    when(col('DSTRCT_AVG_SCORE_VAL').isin('TFS', 'N/A'), None)
        .when(col('DSTRCT_AVG_SCORE_VAL').isNull(), None)
        .otherwise(col('DSTRCT_AVG_SCORE_VAL')).cast(DoubleType()).alias('district_avg_score'),
    
    when(col('INSTN_AVG_SCORE_VAL').isin('TFS', 'N/A'), None)
        .when(col('INSTN_AVG_SCORE_VAL').isNull(), None)
        .otherwise(col('INSTN_AVG_SCORE_VAL')).cast(DoubleType()).alias('institution_avg_score'),
    
    col('#ASSMT_CD').alias('assessment_code'),
    col('source_year'),
    col('source_file')
)

print("Sample scores (new SAT, highest, 2024-25):")
sat_silver.filter(
    "test_component = 'Combined Test Score' AND subgroup = 'All Students' "
    "AND sat_format = 'new' AND highest_recent_indicator = 'Highest' AND school_year = '2024-25'"
).select(
    'school_year', 'institution_name', 'sat_format', 'highest_recent_indicator', 
    'institution_avg_score', 'institution_num_tested'
).orderBy('institution_name').show(5, truncate=False)

print("\n=== Final Validation: Check for remaining duplicates ===\n")

# Check for duplicates on the business key (after institution_key fix)
final_duplicates = sat_silver.groupBy(
    'school_year', 'institution_key', 'subgroup', 'test_component', 'highest_recent_indicator'
).agg(cnt('*').alias('row_count')).filter('row_count > 1')

final_dup_count = final_duplicates.count()
if final_dup_count > 0:
    print(f"⚠️  WARNING: {final_dup_count:,} duplicate keys still exist!")
    final_duplicates.show(5, truncate=False)
else:
    print("✓ No duplicates detected - silver table is clean!")

print(f"\nFinal silver table row count: {sat_silver.count():,}")

print("\n=== Step 4: Write to silver layer ===\n")

# Write to silver table
sat_silver.write.format("delta").mode("overwrite").saveAsTable("workspace.silver.sat")

print("✓ Successfully wrote workspace.silver.sat")
print(f"  Total rows: {sat_silver.count():,}")
print(f"  Columns: {len(sat_silver.columns)}")
print(f"\nTable location: workspace.silver.sat")

# COMMAND ----------

# DBTITLE 1,Verify silver table
print("=== Verifying workspace.silver.sat ===\n")

# Table stats
sat_table = spark.table("workspace.silver.sat")
print(f"Total rows: {sat_table.count():,}")
print(f"Total columns: {len(sat_table.columns)}")

# Year coverage
print("\nYear coverage:")
sat_table.groupBy('school_year').agg(cnt('*').alias('row_count')).orderBy('school_year').show(20, truncate=False)

# Sample: 2024-25 Combined Test Score (Highest)
print("\nSample: 2024-25 Combined Test Scores (Highest, All Students)")
sat_table.filter(
    "school_year = '2024-25' AND test_component = 'Combined Test Score' "
    "AND highest_recent_indicator = 'Highest' AND subgroup = 'All Students' AND sat_format = 'new'"
).select(
    'institution_name', 'district_name', 'institution_avg_score', 'institution_num_tested'
).orderBy('institution_name').show(10, truncate=False)

print("\n✓ Silver table ready for gold layer transformations")
