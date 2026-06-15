# Databricks notebook source
# DBTITLE 1,Attendance Silver Layer - Overview
# MAGIC %md
# MAGIC # Attendance Silver Layer Pipeline
# MAGIC
# MAGIC **Purpose:** Clean and consolidate attendance data from bronze layer
# MAGIC
# MAGIC **Source Table:**
# MAGIC * `workspace.bronze.attendance` (25,072 rows, 2010-2025, 85 columns)
# MAGIC
# MAGIC **Output:** `workspace.silver.attendance` (wide format with 15 subgroups)
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Key Transformations
# MAGIC
# MAGIC ### 1. Duplicate Removal
# MAGIC * **Problem:** 2,518 duplicate records (same school/year downloaded multiple times)
# MAGIC * **Cause:** Multiple download dates creating identical records from different source files
# MAGIC * **Solution:** DropDuplicates on business key (school_year, district_code, institution_number)
# MAGIC * **Result:** True duplicates with identical attendance data
# MAGIC
# MAGIC ### 2. Institution Key
# MAGIC * **`institution_key`** = composite join key for linking to other tables
# MAGIC * **Format:** `district_code_institution_number`
# MAGIC * Used to join with test scores, graduation, enrollment tables
# MAGIC
# MAGIC ### 3. Wide Format Preservation
# MAGIC * **Keep wide format:** 15 subgroups as column sets (not unpivoted)
# MAGIC * Each subgroup has 5 metrics:
# MAGIC   * Student count
# MAGIC   * % with ≤5% absences (good attendance)
# MAGIC   * % with 6-15% absences (moderate)
# MAGIC   * % with >15% absences (chronic absenteeism)
# MAGIC   * Chronic absenteeism percentage
# MAGIC
# MAGIC ### 4. TFS Suppression Handling
# MAGIC * **TFS** = "Too Few Students" (privacy threshold < 10 students)
# MAGIC * Converted to NULL for all numeric columns
# MAGIC * Preserves data integrity for aggregations
# MAGIC
# MAGIC ### 5. Type Casting
# MAGIC * **Counts:** Cast from string to IntegerType
# MAGIC * **Percentages:** Cast from string to DecimalType(5,1)
# MAGIC * Ensures proper arithmetic operations in gold layer
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Schema Overview
# MAGIC
# MAGIC **Core Fields (7):**
# MAGIC | Column | Type | Description |
# MAGIC |--------|------|-------------|
# MAGIC | school_year | string | Academic year (e.g., '2024-25') |
# MAGIC | detail_level | string | 'School', 'District', or 'State' |
# MAGIC | district_code | string | District code |
# MAGIC | district_name | string | District name |
# MAGIC | institution_number | string | Institution number ('ALL' for district) |
# MAGIC | institution_name | string | School name |
# MAGIC | grades_served | string | Grade levels (e.g., 'PK,KK,01,02,...') |
# MAGIC | **institution_key** | string | Composite join key |
# MAGIC | source_year | string | Source file year |
# MAGIC | source_file | string | Source file name |
# MAGIC
# MAGIC **Subgroup Metrics (15 subgroups × 5 columns = 75 columns):**
# MAGIC
# MAGIC **Subgroups:**
# MAGIC 1. **ALL** - All students
# MAGIC 2. **INDIAN** - American Indian/Alaska Native
# MAGIC 3. **ASIAN** - Asian
# MAGIC 4. **BLACK** - Black/African American
# MAGIC 5. **WHITE** - White
# MAGIC 6. **HISPANI** - Hispanic/Latino
# MAGIC 7. **MULTI** - Multiracial
# MAGIC 8. **FEMALE** - Female
# MAGIC 9. **MALE** - Male
# MAGIC 10. **SWD** - Students with Disabilities
# MAGIC 11. **NOT_SWD** - Students without Disabilities
# MAGIC 12. **ED** - Economically Disadvantaged
# MAGIC 13. **NOT_ED** - Not Economically Disadvantaged
# MAGIC 14. **LEP** - Limited English Proficiency
# MAGIC 15. **MIGRANT** - Migrant students
# MAGIC
# MAGIC **For each subgroup:**
# MAGIC * `student_count_[subgroup]` (int) - Number of students
# MAGIC * `five_or_fewer_pct_[subgroup]` (decimal) - % with ≤5% absences
# MAGIC * `six_to_fifteen_pct_[subgroup]` (decimal) - % with 6-15% absences
# MAGIC * `over_15_pct_[subgroup]` (decimal) - % with >15% absences (chronic)
# MAGIC * `chronic_absent_pct_[subgroup]` (decimal) - Chronic absenteeism rate
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Attendance Metrics Explained
# MAGIC
# MAGIC **Attendance Buckets:**
# MAGIC * **≤5% absences:** Excellent attendance (present 95%+ of days)
# MAGIC * **6-15% absences:** Moderate attendance concerns
# MAGIC * **>15% absences:** Chronic absenteeism (federal definition)
# MAGIC
# MAGIC **Key Metric: Chronic Absenteeism**
# MAGIC * Students missing 15%+ of school days
# MAGIC * Strong predictor of academic failure, dropout risk
# MAGIC * Used for accountability, early warning systems

# COMMAND ----------

# DBTITLE 1,Build attendance silver table
# Attendance Silver Layer Pipeline
# Purpose: Clean and consolidate attendance data from bronze layer
# Output: workspace.silver.attendance

from pyspark.sql.functions import col, when, concat_ws, count as cnt, regexp_replace
from pyspark.sql.types import IntegerType, DecimalType

print("=== Attendance Silver Layer Pipeline ===\n")
print("Loading bronze table...")

# Load source table
att_bronze = spark.table("workspace.bronze.attendance")
print(f"  Bronze rows: {att_bronze.count():,}")
print(f"  Bronze columns: {len(att_bronze.columns)}")

print("\n=== Step 1: Check for duplicates ===\n")

# Business key: school_year + district + institution
duplicates = att_bronze.groupBy(
    'LONG_SCHOOL_YEAR', 'SCHOOL_DSTRCT_CD', 'INSTN_NUMBER'
).agg(cnt('*').alias('row_count')).filter('row_count > 1')

dupe_count = duplicates.count()
if dupe_count > 0:
    print(f"⚠️  Found {dupe_count:,} duplicate business keys")
    print("Sample duplicates:")
    duplicates.orderBy(col('row_count').desc()).show(5, truncate=False)
    
    # Show year breakdown
    print("Duplicate breakdown by year:")
    duplicates.join(
        att_bronze.select('LONG_SCHOOL_YEAR', 'SCHOOL_DSTRCT_CD', 'INSTN_NUMBER').distinct(),
        on=['LONG_SCHOOL_YEAR', 'SCHOOL_DSTRCT_CD', 'INSTN_NUMBER']
    ).groupBy('LONG_SCHOOL_YEAR').agg(cnt('*').alias('duplicate_keys')).orderBy('LONG_SCHOOL_YEAR').show(truncate=False)
else:
    print("✓ No duplicates detected")

print("\n=== Step 2: Remove duplicates ===\n")

before_dedup = att_bronze.count()
att_bronze = att_bronze.dropDuplicates(['LONG_SCHOOL_YEAR', 'SCHOOL_DSTRCT_CD', 'INSTN_NUMBER'])
after_dedup = att_bronze.count()

print(f"Rows before deduplication: {before_dedup:,}")
print(f"Rows after deduplication: {after_dedup:,}")
print(f"Removed {before_dedup - after_dedup:,} duplicate rows")

print("\n=== Step 3: Apply transformations ===\n")

# Helper function to convert TFS and cast
def cast_count(col_name):
    return when(col(col_name) == 'TFS', None) \
        .when(col(col_name).isNull(), None) \
        .otherwise(col(col_name)).cast(IntegerType()).alias(col_name.lower())

def cast_percent(col_name):
    return when(col(col_name) == 'TFS', None) \
        .when(col(col_name).isNull(), None) \
        .otherwise(col(col_name)).cast(DecimalType(5,1)).alias(col_name.lower())

# Core columns
att_silver = att_bronze.select(
    col('LONG_SCHOOL_YEAR').alias('school_year'),
    col('DETAIL_LVL_DESC').alias('detail_level'),
    col('SCHOOL_DSTRCT_CD').alias('district_code'),
    col('SCHOOL_DSTRCT_NM').alias('district_name'),
    col('INSTN_NUMBER').alias('institution_number'),
    col('INSTN_NAME').alias('institution_name'),
    col('GRADES_SERVED_DESC').alias('grades_served'),
    
    # Institution key for joins
    concat_ws('_', col('SCHOOL_DSTRCT_CD'), col('INSTN_NUMBER')).alias('institution_key'),
    
    # Subgroup columns: ALL
    cast_count('STUDENT_COUNT_ALL'),
    cast_percent('FIVE_OR_FEWER_PERCENT_ALL'),
    cast_percent('SIX_TO_FIFTEEN_PERCENT_ALL'),
    cast_percent('OVER_15_PERCENT_ALL'),
    cast_percent('CHRONIC_ABSENT_PERC_ALL'),
    
    # Subgroup: INDIAN
    cast_count('STUDENT_COUNT_INDIAN'),
    cast_percent('FIVE_OR_FEWER_PERCENT_INDIAN'),
    cast_percent('SIX_TO_FIFTEEN_PERCENT_INDIAN'),
    cast_percent('OVER_15_PERCENT_INDIAN'),
    cast_percent('CHRONIC_ABSENT_PERC_INDIAN'),
    
    # Subgroup: ASIAN
    cast_count('STUDENT_COUNT_ASIAN'),
    cast_percent('FIVE_OR_FEWER_PERCENT_ASIAN'),
    cast_percent('SIX_TO_FIFTEEN_PERCENT_ASIAN'),
    cast_percent('OVER_15_PERCENT_ASIAN'),
    cast_percent('CHRONIC_ABSENT_PERC_ASIAN'),
    
    # Subgroup: BLACK
    cast_count('STUDENT_COUNT_BLACK'),
    cast_percent('FIVE_OR_FEWER_PERCENT_BLACK'),
    cast_percent('SIX_TO_FIFTEEN_PERCENT_BLACK'),
    cast_percent('OVER_15_PERCENT_BLACK'),
    cast_percent('CHRONIC_ABSENT_PERC_BLACK'),
    
    # Subgroup: WHITE
    cast_count('STUDENT_COUNT_WHITE'),
    cast_percent('FIVE_OR_FEWER_PERCENT_WHITE'),
    cast_percent('SIX_TO_FIFTEEN_PERCENT_WHITE'),
    cast_percent('OVER_15_PERCENT_WHITE'),
    cast_percent('CHRONIC_ABSENT_PERC_WHITE'),
    
    # Subgroup: HISPANI
    cast_count('STUDENT_COUNT_HISPANI'),
    cast_percent('FIVE_OR_FEWER_PERCENT_HISPANI'),
    cast_percent('SIX_TO_FIFTEEN_PERCENT_HISPANI'),
    cast_percent('OVER_15_PERCENT_HISPANI'),
    cast_percent('CHRONIC_ABSENT_PERC_HISPANI'),
    
    # Subgroup: MULTI
    cast_count('STUDENT_COUNT_MULTI'),
    cast_percent('FIVE_OR_FEWER_PERCENT_MULTI'),
    cast_percent('SIX_TO_FIFTEEN_PERCENT_MULTI'),
    cast_percent('OVER_15_PERCENT_MULTI'),
    cast_percent('CHRONIC_ABSENT_PERC_MULTI'),
    
    # Subgroup: FEMALE
    cast_count('STUDENT_COUNT_FEMALE'),
    cast_percent('FIVE_OR_FEWER_PERCENT_FEMALE'),
    cast_percent('SIX_TO_FIFTEEN_PERCENT_FEMALE'),
    cast_percent('OVER_15_PERCENT_FEMALE'),
    cast_percent('CHRONIC_ABSENT_PERC_FEMALE'),
    
    # Subgroup: MALE
    cast_count('STUDENT_COUNT_MALE'),
    cast_percent('FIVE_OR_FEWER_PERCENT_MALE'),
    cast_percent('SIX_TO_FIFTEEN_PERCENT_MALE'),
    cast_percent('OVER_15_PERCENT_MALE'),
    cast_percent('CHRONIC_ABSENT_PERC_MALE'),
    
    # Subgroup: SWD (Students With Disabilities)
    cast_count('STUDENT_COUNT_SWD'),
    cast_percent('FIVE_OR_FEWER_PERCENT_SWD'),
    cast_percent('SIX_TO_FIFTEEN_PERCENT_SWD'),
    cast_percent('OVER_15_PERCENT_SWD'),
    cast_percent('CHRONIC_ABSENT_PERC_SWD'),
    
    # Subgroup: NOT_SWD
    cast_count('STUDENT_COUNT_NOT_SWD'),
    cast_percent('FIVE_OR_FEWER_PERCENT_NOT_SWD'),
    cast_percent('SIX_TO_FIFTEEN_PERCENT_NOT_SWD'),
    cast_percent('OVER_15_PERCENT_NOT_SWD'),
    cast_percent('CHRONIC_ABSENT_PERC_NOT_SWD'),
    
    # Subgroup: ED (Economically Disadvantaged)
    cast_count('STUDENT_COUNT_ED'),
    cast_percent('FIVE_OR_FEWER_PERCENT_ED'),
    cast_percent('SIX_TO_FIFTEEN_PERCENT_ED'),
    cast_percent('OVER_15_PERCENT_ED'),
    cast_percent('CHRONIC_ABSENT_PERC_ED'),
    
    # Subgroup: NOT_ED
    cast_count('STUDENT_COUNT_NOT_ED'),
    cast_percent('FIVE_OR_FEWER_PERCENT_NOT_ED'),
    cast_percent('SIX_TO_FIFTEEN_PERCENT_NOT_ED'),
    cast_percent('OVER_15_PERCENT_NOT_ED'),
    cast_percent('CHRONIC_ABSENT_PERC_NOT_ED'),
    
    # Subgroup: LEP (Limited English Proficiency)
    cast_count('STUDENT_COUNT_LEP'),
    cast_percent('FIVE_OR_FEWER_PERCENT_LEP'),
    cast_percent('SIX_TO_FIFTEEN_PERCENT_LEP'),
    cast_percent('OVER_15_PERCENT_LEP'),
    cast_percent('CHRONIC_ABSENT_PERC_LEP'),
    
    # Subgroup: MIGRANT
    cast_count('STUDENT_COUNT_MIGRANT'),
    cast_percent('FIVE_OR_FEWER_PERCENT_MIGRANT'),
    cast_percent('SIX_TO_FIFTEEN_PERCENT_MIGRANT'),
    cast_percent('OVER_15_PERCENT_MIGRANT'),
    cast_percent('CHRONIC_ABSENT_PERC_MIGRANT'),
    
    # Metadata
    col('source_year'),
    col('source_file')
)

print(f"Silver table built: {att_silver.count():,} rows")
print(f"Silver columns: {len(att_silver.columns)}")
print("\nSample (2024-25, Schools with >20% chronic absenteeism):")
att_silver.filter(
    "school_year = '2024-25' AND detail_level = 'School' AND chronic_absent_perc_all > 20"
).select(
    'institution_name', 'district_name', 'student_count_all', 'chronic_absent_perc_all'
).orderBy(col('chronic_absent_perc_all').desc()).show(5, truncate=False)

# COMMAND ----------

# DBTITLE 1,Write and validate silver table
from pyspark.sql.functions import min as spark_min, max as spark_max, avg as spark_avg

print("=== Writing silver table ===\n")

att_silver.write.format("delta").mode("overwrite").option("overwriteSchema", "true") \
    .saveAsTable("workspace.silver.attendance")

print("✓ workspace.silver.attendance written successfully")

print("\n=== Validation & Quality Checks ===\n")

# Check business key uniqueness
print("Checking business key uniqueness...")
key_check = att_silver.groupBy(
    'school_year', 'district_code', 'institution_number'
).agg(cnt('*').alias('key_count')).filter('key_count > 1')

if key_check.count() > 0:
    print("⚠️  WARNING: Duplicate business keys found after deduplication!")
    key_check.show(5, truncate=False)
else:
    print("✓ All business keys are unique")

# TFS suppression analysis
print("\nTFS suppression analysis:")
total_records = att_silver.count()
tfs_student_count = att_silver.filter("student_count_all IS NULL").count()
tfs_chronic_absent = att_silver.filter("chronic_absent_perc_all IS NULL").count()

print(f"  Total records: {total_records:,}")
print(f"  TFS suppressed (student_count_all): {tfs_student_count:,} ({100*tfs_student_count/total_records:.1f}%)")
print(f"  TFS suppressed (chronic_absent_perc_all): {tfs_chronic_absent:,} ({100*tfs_chronic_absent/total_records:.1f}%)")

# Chronic absenteeism range validation (valid range: 0-100%)
print("\nChronic absenteeism range validation (valid: 0-100%):")
absent_ranges = att_silver.filter(
    "chronic_absent_perc_all IS NOT NULL"
).agg(
    spark_min('chronic_absent_perc_all').alias('min_rate'),
    spark_max('chronic_absent_perc_all').alias('max_rate'),
    spark_avg('chronic_absent_perc_all').alias('avg_rate')
).collect()[0]

print(f"  Min chronic absent %: {absent_ranges['min_rate']:.1f}%")
print(f"  Max chronic absent %: {absent_ranges['max_rate']:.1f}%")
print(f"  Avg chronic absent %: {absent_ranges['avg_rate']:.1f}%")
if 0 <= absent_ranges['min_rate'] and absent_ranges['max_rate'] <= 100:
    print("✓ All rates within valid range")
else:
    print("⚠️  WARNING: Rates outside expected range (0-100%)")
    att_silver.filter(
        "chronic_absent_perc_all < 0 OR chronic_absent_perc_all > 100"
    ).select('school_year', 'institution_name', 'chronic_absent_perc_all').show(5)

# Attendance bucket consistency check (should sum to ~100%)
print("\nAttendance bucket consistency check:")
print("Validating that attendance buckets sum to ≈100% for All Students...")

bucket_check = att_silver.filter(
    "five_or_fewer_percent_all IS NOT NULL AND "
    "six_to_fifteen_percent_all IS NOT NULL AND "
    "over_15_percent_all IS NOT NULL"
).withColumn(
    'bucket_sum',
    col('five_or_fewer_percent_all') + col('six_to_fifteen_percent_all') + col('over_15_percent_all')
).withColumn(
    'diff_from_100', 
    col('bucket_sum') - 100
).filter('ABS(diff_from_100) > 1.0')  # Allow 1% tolerance for rounding

inconsistent_buckets = bucket_check.count()
if inconsistent_buckets > 0:
    print(f"⚠️  WARNING: {inconsistent_buckets:,} records where buckets don't sum to 100% (±1% tolerance)")
    bucket_check.select(
        'school_year', 'institution_name', 'five_or_fewer_percent_all', 
        'six_to_fifteen_percent_all', 'over_15_percent_all', 'bucket_sum', 'diff_from_100'
    ).orderBy(col('diff_from_100').desc()).show(5, truncate=False)
else:
    print("✓ All attendance buckets sum to ~100%")

# Detail level distribution
print("\nDetail level distribution:")
att_silver.groupBy('detail_level').agg(cnt('*').alias('record_count')).orderBy('detail_level').show(truncate=False)

# Year coverage validation
print("\nYear coverage validation:")
year_dist = att_silver.groupBy('school_year').agg(cnt('*').alias('record_count')).orderBy('school_year')
print("Expected: Relatively stable record counts per year")
year_dist.show(20, truncate=False)

# Chronic absenteeism trends (statewide, All Students)
print("\nStatewide chronic absenteeism trends (All Students):")
att_silver.filter(
    "detail_level = 'State' AND chronic_absent_perc_all IS NOT NULL"
).select(
    'school_year', 'chronic_absent_perc_all'
).orderBy('school_year').show(20, truncate=False)

# Top 10 schools by chronic absenteeism (2024-25)
print("\nTop 10 schools by chronic absenteeism (2024-25):")
att_silver.filter(
    "school_year = '2024-25' AND detail_level = 'School' AND chronic_absent_perc_all IS NOT NULL"
).select(
    'institution_name', 'district_name', 'student_count_all', 'chronic_absent_perc_all'
).orderBy(col('chronic_absent_perc_all').desc()).show(10, truncate=False)

print("\n✓ Silver table ready for gold layer transformations")
