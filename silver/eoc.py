# Databricks notebook source
# DBTITLE 1,EOC Silver Layer - Overview
# MAGIC %md
# MAGIC # EOC (End of Course) Silver Layer Pipeline
# MAGIC
# MAGIC **Purpose:** Clean Georgia Milestones End of Course test scores (high school)
# MAGIC
# MAGIC **Source:** `workspace.bronze.eoc` (1,170,716 rows)  
# MAGIC **Output:** `workspace.silver.eoc` (20 columns)
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Key Transformations
# MAGIC
# MAGIC ### 1. Deduplication
# MAGIC * **Business Key:** `school_year + district_code + institution_number + grade_level + subgroup_name + test_component`
# MAGIC * Bronze contains ~30K duplicate rows from source files
# MAGIC * Duplicates are exact copies - safe to remove
# MAGIC
# MAGIC ### 2. Institution Key
# MAGIC * **`institution_key`** = composite join key for linking to other tables
# MAGIC * **Format:** `district_code_institution_number`
# MAGIC * Used to join with demographics, attendance, graduation tables
# MAGIC
# MAGIC ### 3. TFS Suppression Handling
# MAGIC * **TFS** = "Too Few Students" (privacy threshold < 10 students)
# MAGIC * Converted to NULL for all numeric columns
# MAGIC * Preserves data integrity for aggregations
# MAGIC
# MAGIC ### 4. Type Casting
# MAGIC * **Counts:** Cast from string to IntegerType
# MAGIC * **Percentages:** Cast from string to DecimalType(5,2)
# MAGIC * Ensures proper arithmetic operations in gold layer
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Schema
# MAGIC
# MAGIC | Column | Type | Description |
# MAGIC |--------|------|-------------|
# MAGIC | school_year | string | Academic year (e.g., '2016-17') |
# MAGIC | district_code | string | District code |
# MAGIC | district_name | string | District name |
# MAGIC | institution_number | string | Institution number |
# MAGIC | institution_name | string | School name |
# MAGIC | grade_level | string | Grade level (09-12) |
# MAGIC | subgroup_name | string | Student subgroup (All Students, race/ethnicity, SWD, etc.) |
# MAGIC | test_component | string | Course name (Algebra I, Biology, US History, etc.) |
# MAGIC | num_tested | int | Number of students tested (NULL if TFS) |
# MAGIC | begin_count | int | Count at Beginning level (NULL if TFS) |
# MAGIC | developing_count | int | Count at Developing level (NULL if TFS) |
# MAGIC | proficient_count | int | Count at Proficient level (NULL if TFS) |
# MAGIC | distinguished_count | int | Count at Distinguished level (NULL if TFS) |
# MAGIC | begin_pct | decimal(5,2) | Percentage at Beginning level (NULL if TFS) |
# MAGIC | developing_pct | decimal(5,2) | Percentage at Developing level (NULL if TFS) |
# MAGIC | proficient_pct | decimal(5,2) | Percentage at Proficient level (NULL if TFS) |
# MAGIC | distinguished_pct | decimal(5,2) | Percentage at Distinguished level (NULL if TFS) |
# MAGIC | **institution_key** | string | Composite join key |
# MAGIC | source_year | string | Source file year |
# MAGIC | source_file | string | Source file name |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## EOC Courses Tested
# MAGIC
# MAGIC **Core Academic Subjects:**
# MAGIC * **Math:** Algebra I, Geometry, Coordinate Algebra, Analytic Geometry
# MAGIC * **Science:** Biology, Physical Science
# MAGIC * **Social Studies:** US History, Economics
# MAGIC * **English:** 9th Grade Literature and Composition, American Literature and Composition
# MAGIC
# MAGIC ## Proficiency Levels
# MAGIC
# MAGIC **Georgia Milestones 4-Level Performance Scale:**
# MAGIC
# MAGIC 1. **Beginning Learner** - Does not yet demonstrate proficiency
# MAGIC 2. **Developing Learner** - Demonstrates partial proficiency
# MAGIC 3. **Proficient Learner** - Demonstrates proficiency (college/career ready benchmark)
# MAGIC 4. **Distinguished Learner** - Demonstrates advanced proficiency
# MAGIC
# MAGIC **Proficient + Distinguished = "College and Career Ready"**

# COMMAND ----------

# DBTITLE 1,Build EOC silver table
# EOC Silver Layer Pipeline
# Purpose: Clean, type, and deduplicate Georgia Milestones End of Course test score data
# Output: workspace.silver.eoc

from pyspark.sql.functions import col, when, concat_ws, count as cnt
from pyspark.sql.types import IntegerType, DecimalType

print("=== EOC Silver Layer Pipeline ===\n")
print("Loading bronze table...")

eoc_bronze = spark.table("workspace.bronze.eoc")
print(f"Bronze: {eoc_bronze.count():,} rows\n")

print("=== Step 1: Check for duplicates ===\n")

# Business key: school_year + district + institution + grade + subgroup + test component
duplicates = eoc_bronze.groupBy(
    'LONG_SCHOOL_YEAR', 'SCHOOL_DISTRCT_CD', 'INSTN_NUMBER', 
    'ACDMC_LVL', 'SUBGROUP_NAME', 'TEST_CMPNT_TYP_NM'
).agg(cnt('*').alias('row_count')).filter('row_count > 1')

dupe_count = duplicates.count()
if dupe_count > 0:
    print(f"⚠️  Found {dupe_count:,} duplicate business keys")
    print("Sample duplicates:")
    duplicates.orderBy(col('row_count').desc()).show(5, truncate=False)
else:
    print("✓ No duplicates detected")

print("\n=== Step 2: Remove duplicates from source data ===\n")

before_dedup = eoc_bronze.count()
eoc_bronze = eoc_bronze.dropDuplicates([
    'LONG_SCHOOL_YEAR', 'SCHOOL_DISTRCT_CD', 'INSTN_NUMBER', 
    'ACDMC_LVL', 'SUBGROUP_NAME', 'TEST_CMPNT_TYP_NM'
])
after_dedup = eoc_bronze.count()

print(f"Rows before deduplication: {before_dedup:,}")
print(f"Rows after deduplication: {after_dedup:,}")
print(f"Removed {before_dedup - after_dedup:,} duplicate rows")

print("\n=== Step 3: Apply final transformations ===\n")

# Build silver table with cleaned/typed columns
eoc_silver = eoc_bronze.select(
    col('LONG_SCHOOL_YEAR').alias('school_year'),
    col('SCHOOL_DISTRCT_CD').alias('district_code'),
    col('SCHOOL_DSTRCT_NM').alias('district_name'),
    col('INSTN_NUMBER').alias('institution_number'),
    col('INSTN_NAME').alias('institution_name'),
    col('ACDMC_LVL').alias('grade_level'),
    col('SUBGROUP_NAME').alias('subgroup_name'),
    col('TEST_CMPNT_TYP_NM').alias('test_component'),
    
    # Counts - handle TFS suppression, cast to integer
    when(col('NUM_TESTED_CNT') == 'TFS', None)
        .otherwise(col('NUM_TESTED_CNT')).cast(IntegerType()).alias('num_tested'),
    when(col('BEGIN_CNT') == 'TFS', None)
        .otherwise(col('BEGIN_CNT')).cast(IntegerType()).alias('begin_count'),
    when(col('DEVELOPING_CNT') == 'TFS', None)
        .otherwise(col('DEVELOPING_CNT')).cast(IntegerType()).alias('developing_count'),
    when(col('PROFICIENT_CNT') == 'TFS', None)
        .otherwise(col('PROFICIENT_CNT')).cast(IntegerType()).alias('proficient_count'),
    when(col('DISTINGUISHED_CNT') == 'TFS', None)
        .otherwise(col('DISTINGUISHED_CNT')).cast(IntegerType()).alias('distinguished_count'),
    
    # Percentages - handle TFS suppression, cast to decimal
    when(col('BEGIN_PCT') == 'TFS', None)
        .otherwise(col('BEGIN_PCT')).cast(DecimalType(5,2)).alias('begin_pct'),
    when(col('DEVELOPING_PCT') == 'TFS', None)
        .otherwise(col('DEVELOPING_PCT')).cast(DecimalType(5,2)).alias('developing_pct'),
    when(col('PROFICIENT_PCT') == 'TFS', None)
        .otherwise(col('PROFICIENT_PCT')).cast(DecimalType(5,2)).alias('proficient_pct'),
    when(col('DISTINGUISHED_PCT') == 'TFS', None)
        .otherwise(col('DISTINGUISHED_PCT')).cast(DecimalType(5,2)).alias('distinguished_pct'),
    
    # Composite join key for institution
    concat_ws('_', col('SCHOOL_DISTRCT_CD'), col('INSTN_NUMBER')).alias('institution_key'),
    
    col('source_year'),
    col('source_file')
)

print(f"Silver table built: {eoc_silver.count():,} rows")
print("\nSample (Algebra I, 2016-17, All Students):")
eoc_silver.filter(
    "school_year = '2016-17' AND test_component = 'Algebra I' "
    "AND subgroup_name = 'All Students'"
).select(
    'institution_name', 'district_name', 'num_tested', 'proficient_pct', 'distinguished_pct'
).show(5, truncate=False)

# COMMAND ----------

# DBTITLE 1,Write and verify silver table
print("=== Writing silver table ===\n")

eoc_silver.write.format("delta").mode("overwrite").option("overwriteSchema", "true") \
    .saveAsTable("workspace.silver.eoc")

print("✓ workspace.silver.eoc written successfully")

print("\n=== Validation & Quality Checks ===\n")

# Check business key uniqueness
print("Checking business key uniqueness...")
key_check = eoc_silver.groupBy(
    'school_year', 'district_code', 'institution_number', 
    'grade_level', 'subgroup_name', 'test_component'
).agg(cnt('*').alias('key_count')).filter('key_count > 1')

if key_check.count() > 0:
    print("⚠️  WARNING: Duplicate business keys found after deduplication!")
    key_check.show(5, truncate=False)
else:
    print("✓ All business keys are unique")

# TFS suppression analysis
print("\nTFS suppression analysis:")
total_records = eoc_silver.count()
tfs_num_tested = eoc_silver.filter("num_tested IS NULL").count()
tfs_proficient = eoc_silver.filter("proficient_pct IS NULL").count()

print(f"  Total records: {total_records:,}")
print(f"  TFS suppressed (num_tested): {tfs_num_tested:,} ({100*tfs_num_tested/total_records:.1f}%)")
print(f"  TFS suppressed (proficient_pct): {tfs_proficient:,} ({100*tfs_proficient/total_records:.1f}%)")

# Proficiency level consistency check
print("\nProficiency level consistency check:")
inconsistent = eoc_silver.filter(
    "num_tested IS NOT NULL AND "
    "(begin_count + developing_count + proficient_count + distinguished_count) != num_tested"
).count()

if inconsistent > 0:
    print(f"⚠️  WARNING: {inconsistent:,} records where proficiency counts don't sum to num_tested")
    eoc_silver.filter(
        "num_tested IS NOT NULL AND "
        "(begin_count + developing_count + proficient_count + distinguished_count) != num_tested"
    ).select(
        'school_year', 'institution_name', 'grade_level', 'test_component',
        'num_tested', 'begin_count', 'developing_count', 'proficient_count', 'distinguished_count'
    ).show(5, truncate=False)
else:
    print("✓ All proficiency counts sum correctly to num_tested")

# Percentage validation (should sum to ~100%)
print("\nPercentage validation:")
pct_check = eoc_silver.filter(
    "begin_pct IS NOT NULL AND developing_pct IS NOT NULL AND "
    "proficient_pct IS NOT NULL AND distinguished_pct IS NOT NULL"
).withColumn(
    'total_pct', 
    col('begin_pct') + col('developing_pct') + col('proficient_pct') + col('distinguished_pct')
).filter("ABS(total_pct - 100.0) > 0.5")

if pct_check.count() > 0:
    print(f"⚠️  WARNING: {pct_check.count():,} records where percentages don't sum to ~100%")
    pct_check.select(
        'school_year', 'institution_name', 'grade_level', 'test_component',
        'begin_pct', 'developing_pct', 'proficient_pct', 'distinguished_pct', 'total_pct'
    ).show(5, truncate=False)
else:
    print("✓ All percentages sum to approximately 100%")

# Year coverage validation
print("\nYear coverage validation:")
year_dist = eoc_silver.groupBy('school_year').agg(cnt('*').alias('record_count')).orderBy('school_year')
print("Expected: Relatively stable record counts per year (accounting for school changes)")
year_dist.show(20, truncate=False)

print("\n✓ Silver table ready for gold layer transformations")
