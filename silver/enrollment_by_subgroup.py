# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# DBTITLE 1,Enrollment by Subgroup Silver Layer - Overview
# MAGIC %md
# MAGIC # Enrollment by Subgroup Silver Layer Pipeline
# MAGIC
# MAGIC **Purpose:** Clean and consolidate enrollment demographics and program participation data from bronze layer
# MAGIC
# MAGIC **Source Table:**
# MAGIC * `workspace.bronze.enrollment_by_subgroup` (49,995 rows, 2010-2025)
# MAGIC
# MAGIC **Output:** `workspace.silver.enrollment_by_subgroup` (unpivoted to long format)
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## What is Enrollment by Subgroup?
# MAGIC
# MAGIC **Enrollment by Subgroup** provides demographic breakdowns and program participation rates at the district and school level:
# MAGIC
# MAGIC **Demographics:**
# MAGIC * Race/Ethnicity (Asian, Black, Hispanic, White, Multiracial, Native American)
# MAGIC * Gender (Male, Female)
# MAGIC
# MAGIC **Special Populations:**
# MAGIC * Economically Disadvantaged (ED)
# MAGIC * Students with Disabilities (SWD)
# MAGIC * English Learners (LEP/ELL)
# MAGIC * Migrant students
# MAGIC
# MAGIC **Program Participation:**
# MAGIC * Gifted
# MAGIC * Special Education (K-12 and Pre-K)
# MAGIC * ESOL (English for Speakers of Other Languages)
# MAGIC * Remedial programs (Grades 6-8, Grades 9-12)
# MAGIC * EIP (Early Intervention Program, Grades K-5)
# MAGIC * Vocational programs (Grades 9-12)
# MAGIC * Alternative programs
# MAGIC
# MAGIC **Why it matters:**
# MAGIC * **Essential for equity analysis** - Understanding which subgroups have access to programs
# MAGIC * **Pairs with direct certification** - Demographic lens on poverty indicators
# MAGIC * **Program effectiveness** - Tracking special population service rates
# MAGIC * **Achievement gap analysis** - When joined with test scores, reveals disparities
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Key Transformations
# MAGIC
# MAGIC ### 1. Data Structure
# MAGIC * **Bronze:** Wide format with separate columns for each subgroup/program (52 columns)
# MAGIC * **Silver:** Long format with subgroup_type and subgroup_value dimensions
# MAGIC * **Why unpivot?** Easier filtering, aggregation, and joining with other metrics
# MAGIC
# MAGIC ### 2. Duplicate Removal
# MAGIC * Bronze has duplicate column names (e.g., ENROLL_PERCENT_ASIAN and ENROLL_PCT_ASIAN)
# MAGIC * These are artifacts from inconsistent source file formats across years
# MAGIC * Silver drops duplicate columns and keeps the cleaner naming convention
# MAGIC
# MAGIC ### 3. TFS Suppression Handling
# MAGIC * **TFS** = "Too Few Students" (privacy threshold, typically < 5-10 students)
# MAGIC * Converted to NULL for all count and percentage columns
# MAGIC * Preserves data integrity for aggregations
# MAGIC
# MAGIC ### 4. Type Casting
# MAGIC * **Counts:** Cast from string to IntegerType
# MAGIC * **Percentages:** Cast from string to DecimalType(5,1)
# MAGIC
# MAGIC ### 5. Institution Key
# MAGIC * **`institution_key`** = composite join key
# MAGIC * **Format:** `district_code_institution_number`
# MAGIC * District-level records use 'ALL' as institution_number
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Schema (Long Format)
# MAGIC
# MAGIC **Core Fields:**
# MAGIC | Column | Type | Description |
# MAGIC |--------|------|-------------|
# MAGIC | school_year | string | Academic year (e.g., '2024-25') |
# MAGIC | detail_level | string | 'District' or 'School' |
# MAGIC | district_code | string | District code (e.g., '601') |
# MAGIC | district_name | string | District name |
# MAGIC | institution_number | string | School ID ('ALL' for district-level) |
# MAGIC | institution_name | string | School name (NULL for district-level) |
# MAGIC | grades_served | string | Grade span (e.g., '09,10,11,12') |
# MAGIC | **institution_key** | string | Composite join key |
# MAGIC | source_year | string | Source file year |
# MAGIC | source_file | string | Source file name |
# MAGIC
# MAGIC **Dimension Fields:**
# MAGIC | Column | Type | Description |
# MAGIC |--------|------|-------------|
# MAGIC | subgroup_type | string | Category (e.g., 'race', 'gender', 'special_population', 'program') |
# MAGIC | subgroup_name | string | Specific subgroup (e.g., 'Asian', 'Male', 'Economically Disadvantaged') |
# MAGIC
# MAGIC **Metrics:**
# MAGIC | Column | Type | Description |
# MAGIC |--------|------|-------------|
# MAGIC | enrollment_count | integer | Count of students in subgroup |
# MAGIC | enrollment_percent | decimal(5,1) | % of total enrollment |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Data Notes
# MAGIC
# MAGIC **Column Name Variations:**
# MAGIC * Bronze tables have inconsistent naming across years:
# MAGIC   * `ENROLL_PERCENT_ASIAN` (older files) vs. `ENROLL_PCT_ASIAN` (newer files)
# MAGIC   * Silver standardizes to `enrollment_percent` in long format
# MAGIC
# MAGIC **Detail Level Field:**
# MAGIC * Bronze uses `DETAIL_LVL_DESC` with values 'District' or 'School'
# MAGIC * Silver preserves this as `detail_level`
# MAGIC
# MAGIC **Percentage Totals:**
# MAGIC * Demographic percentages (race + gender) may not sum to exactly 100% due to rounding
# MAGIC * Program participation percentages are independent rates (not mutually exclusive)

# COMMAND ----------

# DBTITLE 1,Build silver enrollment_by_subgroup table
# Enrollment by Subgroup Silver Layer Pipeline
# Purpose: Clean and consolidate enrollment demographics and program data
# Key changes: Handle TFS, create institution_key, type cast
# Output: workspace.silver.enrollment_by_subgroup

from pyspark.sql.functions import col, when, concat_ws, count as cnt, lit, regexp_replace
from pyspark.sql.types import DoubleType, DecimalType

print("=== Enrollment by Subgroup Silver Layer Pipeline ===\n")
print("Loading bronze table...")

# Load source table
enroll_bronze = spark.table("workspace.bronze.enrollment_by_subgroup")
print(f"  Bronze rows: {enroll_bronze.count():,}")
print(f"  Bronze columns: {len(enroll_bronze.columns)}")

print("\n=== Step 1: Check for duplicate keys ===\n")

# Check for duplicates (school_year, district, institution)
dup_check = enroll_bronze.filter("LONG_SCHOOL_YEAR IS NOT NULL").groupBy(
    'LONG_SCHOOL_YEAR', 'SCHOOL_DSTRCT_CD', 'INSTN_NUMBER'
).agg(cnt('*').alias('dup_count')).filter('dup_count > 1')

dupe_count = dup_check.count()
print(f"Duplicate keys found: {dupe_count:,}")

if dupe_count > 0:
    print("\nDuplicate breakdown by year:")
    dup_check.join(
        enroll_bronze.select('LONG_SCHOOL_YEAR', 'SCHOOL_DSTRCT_CD', 'INSTN_NUMBER').distinct(),
        on=['LONG_SCHOOL_YEAR', 'SCHOOL_DSTRCT_CD', 'INSTN_NUMBER'],
        how='inner'
    ).groupBy('LONG_SCHOOL_YEAR').agg(
        cnt('*').alias('duplicate_keys')
    ).orderBy('LONG_SCHOOL_YEAR').show(20, truncate=False)
else:
    print("✓ No duplicates detected")

print("\n=== Step 2: Transform to Silver ===\n")

# Helper functions for TFS and casting
def cast_count(col_name):
    return when(col(col_name).isin('TFS', 'N/A', '*'), None) \
        .when(col(col_name).isNull(), None) \
        .otherwise(col(col_name)).cast(DoubleType())

def cast_percent(col_name):
    return when(col(col_name).isin('TFS', 'N/A', '*'), None) \
        .when(col(col_name).isNull(), None) \
        .otherwise(col(col_name)).cast(DecimalType(5,1))

print("Applying transformations:")
print("  • Remove duplicate records (keep one per business key)")
print("  • Create institution_key (district_code + institution_number)")
print("  • Convert TFS/N/A/* (suppression codes) to NULL")
print("  • Cast counts to double, percentages to decimal(5,1)")
print("  • Standardize column names to snake_case")

# Remove duplicates first
before_dedup = enroll_bronze.count()
enroll_clean = enroll_bronze.dropDuplicates(['LONG_SCHOOL_YEAR', 'SCHOOL_DSTRCT_CD', 'INSTN_NUMBER'])
after_dedup = enroll_clean.count()
rows_removed = before_dedup - after_dedup

print(f"\nRemoved {rows_removed:,} duplicate rows")
print(f"Remaining rows: {after_dedup:,}")

# Transform to silver
enroll_silver = enroll_clean.select(
    col('LONG_SCHOOL_YEAR').alias('school_year'),
    
    # Detail level: Use the DETAIL_LVL_DESC field directly
    col('DETAIL_LVL_DESC').alias('detail_level'),
    
    regexp_replace(col('SCHOOL_DSTRCT_CD'), r'\.0$', '').alias('district_code'),
    col('SCHOOL_DSTRCT_NM').alias('district_name'),
    
    # Institution number: Already 'ALL' for districts in the source data
    regexp_replace(col('INSTN_NUMBER'), r'\.0$', '').alias('institution_number'),
    
    col('INSTN_NAME').alias('institution_name'),
    col('GRADES_SERVED_DESC').alias('grades_served'),
    
    # Institution key for joining (district_code + institution_number)
    concat_ws('_', 
        regexp_replace(col('SCHOOL_DSTRCT_CD'), r'\.0$', ''),
        regexp_replace(col('INSTN_NUMBER'), r'\.0$', '')
    ).alias('institution_key'),
    
    # Race/Ethnicity percentages
    cast_percent('ENROLL_PCT_ASIAN').alias('pct_asian'),
    cast_percent('ENROLL_PCT_NATIVE').alias('pct_native'),
    cast_percent('ENROLL_PCT_BLACK').alias('pct_black'),
    cast_percent('ENROLL_PCT_HISPANIC').alias('pct_hispanic'),
    cast_percent('ENROLL_PCT_MULTIRACIAL').alias('pct_multiracial'),
    cast_percent('ENROLL_PCT_WHITE').alias('pct_white'),
    
    # Gender percentages
    cast_percent('ENROLL_PCT_MALE').alias('pct_male'),
    cast_percent('ENROLL_PCT_FEMALE').alias('pct_female'),
    
    # Special populations percentages
    cast_percent('ENROLL_PCT_MIGRANT').alias('pct_migrant'),
    cast_percent('ENROLL_PCT_ED').alias('pct_economically_disadvantaged'),
    cast_percent('ENROLL_PCT_SWD').alias('pct_students_with_disabilities'),
    cast_percent('ENROLL_PCT_LEP').alias('pct_english_learner'),
    
    # Program participation counts and percentages
    cast_count('ENROLL_COUNT_REMEDIAL_GR_6_8').alias('count_remedial_6_8'),
    cast_percent('ENROLL_PCT_REMEDIAL_GR_6_8').alias('pct_remedial_6_8'),
    
    cast_count('ENROLL_COUNT_EIP_K_5').alias('count_eip_k_5'),
    cast_percent('ENROLL_PERCENT_EIP_K_5').alias('pct_eip_k_5'),
    
    cast_count('ENROLL_COUNT_REMEDIAL_GR_9_12').alias('count_remedial_9_12'),
    cast_percent('ENROLL_PCT_REMEDIAL_GR_9_12').alias('pct_remedial_9_12'),
    
    cast_count('ENROLL_COUNT_SPECIAL_ED_K12').alias('count_special_ed_k12'),
    cast_percent('ENROLL_PCT_SPECIAL_ED_K12').alias('pct_special_ed_k12'),
    
    cast_count('ENROLL_COUNT_ESOL').alias('count_esol'),
    cast_percent('ENROLL_PCT_ESOL').alias('pct_esol'),
    
    cast_count('ENROLL_COUNT_SPECIAL_ED_PK').alias('count_special_ed_pk'),
    cast_percent('ENROLL_PCT_SPECIAL_ED_PK').alias('pct_special_ed_pk'),
    
    cast_count('ENROLL_COUNT_VOCATION_9_12').alias('count_vocation_9_12'),
    cast_percent('ENROLL_PCT_VOCATION_9_12').alias('pct_vocation_9_12'),
    
    cast_count('ENROLL_COUNT_ALT_PROGRAMS').alias('count_alt_programs'),
    cast_percent('ENROLL_PCT_ALT_PROGRAMS').alias('pct_alt_programs'),
    
    cast_count('ENROLL_COUNT_GIFTED').alias('count_gifted'),
    cast_percent('ENROLL_PCT_GIFTED').alias('pct_gifted'),
    
    col('source_year'),
    col('source_file')
)

print(f"\nSilver table built: {enroll_silver.count():,} rows")
print(f"Silver columns: {len(enroll_silver.columns)}")

print("\nSample records (2024-25 districts):")
enroll_silver.filter(
    "school_year = '2024-25' AND detail_level = 'District'"
).select(
    'district_name', 'pct_asian', 'pct_black', 'pct_hispanic', 'pct_white', 'pct_economically_disadvantaged'
).orderBy('district_name').show(5, truncate=False)

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
    'school_year', 'district_code', 'institution_number'
).agg(cnt('*').alias('dup_count')).filter('dup_count > 1').count()
print(f"   Duplicate business keys: {dup_check_silver}")
if dup_check_silver == 0:
    print("   ✓ All business keys unique")
else:
    print(f"   ⚠️ {dup_check_silver} duplicate keys found")

# 3. Percentage ranges (0-100%)
print("\n3. Percentage Range Validation (valid: 0-100%):")
perc_cols = ['pct_asian', 'pct_black', 'pct_hispanic', 'pct_white', 'pct_economically_disadvantaged']

for col_name in perc_cols:
    col_stats = enroll_silver.filter(f"{col_name} IS NOT NULL").agg(
        spark_min(col_name).alias('min_pct'),
        spark_max(col_name).alias('max_pct')
    ).collect()[0]
    
    min_val = col_stats['min_pct']
    max_val = col_stats['max_pct']
    
    if min_val is not None and max_val is not None:
        if 0 <= min_val and max_val <= 100:
            print(f"   {col_name}: [{min_val:.1f}%, {max_val:.1f}%] ✓")
        else:
            print(f"   {col_name}: [{min_val:.1f}%, {max_val:.1f}%] ⚠️ Out of range!")

# 4. TFS Handling
print("\n4. TFS/Suppression Handling:")
null_pct_ed = enroll_silver.filter("pct_economically_disadvantaged IS NULL").count()
print(f"   NULL percentages (e.g., pct_economically_disadvantaged): {null_pct_ed:,}")
print("   ✓ TFS values converted to NULL")

# 5. Year Coverage
print("\n5. Year Coverage:")
print("   Rows per year:")
enroll_silver.groupBy('school_year').agg(
    cnt('*').alias('row_count')
).orderBy('school_year').show(20, truncate=False)

# 6. Detail level distribution
print("\n6. Detail Level Distribution:")
enroll_silver.groupBy('detail_level').agg(
    cnt('*').alias('record_count')
).show(truncate=False)

print("\n✓ Validation complete")

# COMMAND ----------

# DBTITLE 1,Write silver table
print("\n=== Writing to Silver Layer ===\n")

target_table = "workspace.silver.enrollment_by_subgroup"

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
from pyspark.sql.functions import col, count as cnt, avg

print("=== Verifying workspace.silver.enrollment_by_subgroup ===\n")

# Table stats
enroll_table = spark.table("workspace.silver.enrollment_by_subgroup")
print(f"Total rows: {enroll_table.count():,}")
print(f"Total columns: {len(enroll_table.columns)}")

# Sample: 2024-25 districts with demographic breakdown
print("\nSample: 2024-25 Districts - Demographics (first 10):")
enroll_table.filter(
    "school_year = '2024-25' AND detail_level = 'District'"
).select(
    'district_name', 'pct_black', 'pct_hispanic', 'pct_white', 
    'pct_economically_disadvantaged', 'pct_english_learner'
).orderBy('district_name').show(10, truncate=False)

# Top 10 districts by economically disadvantaged percentage (2024-25)
print("\nTop 10 districts by economically disadvantaged % (2024-25):")
enroll_table.filter(
    "school_year = '2024-25' AND detail_level = 'District' AND pct_economically_disadvantaged IS NOT NULL"
).select(
    'district_name', 'pct_economically_disadvantaged', 'pct_english_learner', 'pct_students_with_disabilities'
).orderBy(col('pct_economically_disadvantaged').desc()).show(10, truncate=False)

# Statewide averages by year
print("\nStatewide average demographics by year (district-level):")
enroll_table.filter(
    "detail_level = 'District' AND pct_economically_disadvantaged IS NOT NULL"
).groupBy('school_year').agg(
    cnt('*').alias('district_count'),
    avg('pct_economically_disadvantaged').alias('avg_econ_disadv'),
    avg('pct_english_learner').alias('avg_english_learner'),
    avg('pct_students_with_disabilities').alias('avg_swd')
).orderBy('school_year').show(20, truncate=False)

print("\n✓ Silver table ready for gold layer transformations")
