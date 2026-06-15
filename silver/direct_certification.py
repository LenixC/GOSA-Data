# Databricks notebook source
# DBTITLE 1,Direct Certification Silver Layer - Overview
# MAGIC %md
# MAGIC # Direct Certification Silver Layer Pipeline
# MAGIC
# MAGIC **Purpose:** Clean and consolidate direct certification data from district and school bronze tables
# MAGIC
# MAGIC **Source Tables:**
# MAGIC * `workspace.bronze.direct_certification_district` (2,591 rows, 2015-2025)
# MAGIC * `workspace.bronze.direct_certification_school` (27,503 rows, 2015-2025)
# MAGIC
# MAGIC **Output:** `workspace.silver.direct_certification` (combined district + school, ~2,700 rows/year)
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## What is Direct Certification?
# MAGIC
# MAGIC **Direct Certification** measures the percentage of K-12 students who are **directly certified for free meals** without needing to submit a household income application.
# MAGIC
# MAGIC Students are directly certified if they:
# MAGIC * Receive SNAP (food stamps)
# MAGIC * Receive TANF (welfare)
# MAGIC * Are in foster care
# MAGIC * Are homeless or migrant
# MAGIC
# MAGIC **Why it matters:**
# MAGIC * **Primary poverty indicator** in education data
# MAGIC * **Drives Title I funding** (federal funding for high-poverty schools)
# MAGIC * **Essential for equity analysis** - Understanding achievement gaps by economic status
# MAGIC * **More reliable than F/R lunch eligibility** (since Community Eligibility Provision schools don't collect forms)
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Key Transformations
# MAGIC
# MAGIC ### 1. Combine District + School
# MAGIC * **Union** district and school tables
# MAGIC * Add `detail_level` column ('District' or 'School')
# MAGIC * Standardize column names (SYSTEM_ID → district_code, SCHOOL_ID → institution_number)
# MAGIC
# MAGIC ### 2. Year Field
# MAGIC * **Use FISCAL_YEAR** (not SCHOOL_YEAR, which is mostly NULL in bronze)
# MAGIC * **Format:** 2024.0 → '2023-24' (FY 2024 = school year 2023-24)
# MAGIC
# MAGIC ### 3. TFS Suppression Handling
# MAGIC * **TFS** = "Too Few Students" (privacy threshold)
# MAGIC * Converted to NULL for all numeric columns
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
# MAGIC ## Schema (13 columns)
# MAGIC
# MAGIC **Core Fields (10):**
# MAGIC | Column | Type | Description |
# MAGIC |--------|------|-------------|
# MAGIC | school_year | string | Academic year (e.g., '2023-24') |
# MAGIC | fiscal_year | integer | Federal fiscal year (e.g., 2024) |
# MAGIC | detail_level | string | 'District' or 'School' |
# MAGIC | district_code | string | System ID (e.g., '601') |
# MAGIC | district_name | string | System name |
# MAGIC | institution_number | string | School ID ('ALL' for district) |
# MAGIC | institution_name | string | School name (NULL for district) |
# MAGIC | **institution_key** | string | Composite join key |
# MAGIC | source_year | string | Source file year |
# MAGIC | source_file | string | Source file name |
# MAGIC
# MAGIC **Metrics (3):**
# MAGIC | Column | Type | Description |
# MAGIC |--------|------|-------------|
# MAGIC | direct_cert_percent | decimal(5,1) | % of students directly certified |
# MAGIC | poverty_student_count | integer | Count of directly certified students |
# MAGIC | total_student_count | integer | Total K-12 enrollment |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Data Notes
# MAGIC
# MAGIC **Foster Student Inclusion:**
# MAGIC * **2017 and earlier:** Did NOT include foster students
# MAGIC * **2018 and later:** INCLUDES foster students
# MAGIC * **Implication:** 2017 vs 2018+ not directly comparable

# COMMAND ----------

# DBTITLE 1,Build direct_certification silver table
# Direct Certification Silver Layer Pipeline
# Purpose: Union district and school direct certification data
# Output: workspace.silver.direct_certification

from pyspark.sql.functions import col, when, concat_ws, count as cnt, lit, regexp_replace
from pyspark.sql.types import IntegerType, DecimalType

print("=== Direct Certification Silver Layer Pipeline ===\n")

# Load bronze tables
print("Loading bronze tables...")
dc_dist = spark.table("workspace.bronze.direct_certification_district")
dc_school = spark.table("workspace.bronze.direct_certification_school")

print(f"  District rows: {dc_dist.count():,}")
print(f"  School rows: {dc_school.count():,}")

print("\n=== Step 1: Check for duplicates ===\n")

# Check district duplicates
district_dupes = dc_dist.filter("FISCAL_YEAR IS NOT NULL").groupBy(
    'FISCAL_YEAR', 'SYSTEM_ID'
).agg(cnt('*').alias('row_count')).filter('row_count > 1')

district_dupe_count = district_dupes.count()
if district_dupe_count > 0:
    print(f"⚠️  District: Found {district_dupe_count:,} duplicate keys")
    district_dupes.show(5, truncate=False)
else:
    print("✓ District: No duplicates")

# Check school duplicates  
school_dupes = dc_school.filter("FISCAL_YEAR IS NOT NULL").groupBy(
    'FISCAL_YEAR', 'SYSTEM_ID', 'SCHOOL_ID'
).agg(cnt('*').alias('row_count')).filter('row_count > 1')

school_dupe_count = school_dupes.count()
if school_dupe_count > 0:
    print(f"⚠️  School: Found {school_dupe_count:,} duplicate keys")
    school_dupes.show(5, truncate=False)
else:
    print("✓ School: No duplicates")

print("\n=== Step 2: Remove duplicates & filter valid years ===\n")

# Filter to valid records (FISCAL_YEAR not null) and deduplicate
dc_dist_clean = dc_dist.filter("FISCAL_YEAR IS NOT NULL") \
    .dropDuplicates(['FISCAL_YEAR', 'SYSTEM_ID'])
    
dc_school_clean = dc_school.filter("FISCAL_YEAR IS NOT NULL") \
    .dropDuplicates(['FISCAL_YEAR', 'SYSTEM_ID', 'SCHOOL_ID'])

print(f"District after filtering: {dc_dist_clean.count():,} rows")
print(f"School after filtering: {dc_school_clean.count():,} rows")

print("\n=== Step 3: Standardize and union ===\n")

# Helper function: Convert fiscal year to school year format
# FY 2024 = School Year 2023-24
def fiscal_to_school_year(fy_col):
    return concat_ws('-', 
        (col(fy_col) - 1).cast('int').cast('string').substr(-2, 2),
        col(fy_col).cast('int').cast('string').substr(-2, 2)
    )

# Helper functions for TFS and casting
def cast_count(col_name):
    return when(col(col_name) == 'TFS', None) \
        .when(col(col_name).isNull(), None) \
        .otherwise(col(col_name)).cast(IntegerType()).alias(col_name)

def cast_percent(col_name):
    return when(col(col_name) == 'TFS', None) \
        .when(col(col_name).isNull(), None) \
        .otherwise(col(col_name)).cast(DecimalType(5,1)).alias(col_name)

# District records
district_silver = dc_dist_clean.select(
    fiscal_to_school_year('FISCAL_YEAR').alias('school_year'),
    col('FISCAL_YEAR').cast(IntegerType()).alias('fiscal_year'),
    lit('District').alias('detail_level'),
    regexp_replace(col('SYSTEM_ID'), r'\\.0$', '').alias('district_code'),
    col('SYSTEM_NAME').alias('district_name'),
    lit('ALL').alias('institution_number'),
    lit(None).cast('string').alias('institution_name'),
    concat_ws('_', regexp_replace(col('SYSTEM_ID'), r'\\.0$', ''), lit('ALL')).alias('institution_key'),
    cast_percent('direct_cert_perc').alias('direct_cert_percent'),
    cast_count('K12_POVERTY_STUDENT_CT').alias('poverty_student_count'),
    cast_count('K12_STUDENT_COUNT').alias('total_student_count'),
    col('source_year'),
    col('source_file')
)

# School records
school_silver = dc_school_clean.select(
    fiscal_to_school_year('FISCAL_YEAR').alias('school_year'),
    col('FISCAL_YEAR').cast(IntegerType()).alias('fiscal_year'),
    lit('School').alias('detail_level'),
    regexp_replace(col('SYSTEM_ID'), r'\\.0$', '').alias('district_code'),
    col('SYSTEM_NAME').alias('district_name'),
    regexp_replace(col('SCHOOL_ID'), r'\\.0$', '').alias('institution_number'),
    col('SCHOOL_NAME').alias('institution_name'),
    concat_ws('_',
        regexp_replace(col('SYSTEM_ID'), r'\\.0$', ''),
        regexp_replace(col('SCHOOL_ID'), r'\\.0$', '')
    ).alias('institution_key'),
    cast_percent('direct_cert_perc').alias('direct_cert_percent'),
    cast_count('K12_POVERTY_STUDENT_CT').alias('poverty_student_count'),
    cast_count('K12_STUDENT_COUNT').alias('total_student_count'),
    col('source_year'),
    col('source_file')
)

# Union
dc_silver = district_silver.union(school_silver)

print(f"Silver table built: {dc_silver.count():,} rows")
print(f"Silver columns: {len(dc_silver.columns)}")

print("\nSample (2024-25 districts with highest direct cert %):")
dc_silver.filter("school_year = '23-24' AND detail_level = 'District'") \
    .select('district_name', 'direct_cert_percent', 'poverty_student_count', 'total_student_count') \
    .orderBy(col('direct_cert_percent').desc()).show(5, truncate=False)

# COMMAND ----------

# DBTITLE 1,Write and validate silver table
from pyspark.sql.functions import min as spark_min, max as spark_max, avg as spark_avg

print("=== Writing silver table ===\n")

dc_silver.write.format("delta").mode("overwrite").option("overwriteSchema", "true") \
    .saveAsTable("workspace.silver.direct_certification")

print("✓ workspace.silver.direct_certification written successfully")

print("\n=== Validation & Quality Checks ===\n")

# Check business key uniqueness
print("Checking business key uniqueness...")
key_check = dc_silver.groupBy(
    'school_year', 'district_code', 'institution_number'
).agg(cnt('*').alias('key_count')).filter('key_count > 1')

if key_check.count() > 0:
    print("⚠️  WARNING: Duplicate business keys found!")
    key_check.show(5, truncate=False)
else:
    print("✓ All business keys are unique")

# TFS suppression analysis
print("\nTFS suppression analysis:")
total_records = dc_silver.count()
tfs_percent = dc_silver.filter("direct_cert_percent IS NULL").count()
tfs_poverty_ct = dc_silver.filter("poverty_student_count IS NULL").count()
tfs_total_ct = dc_silver.filter("total_student_count IS NULL").count()

print(f"  Total records: {total_records:,}")
print(f"  TFS suppressed (direct_cert_percent): {tfs_percent:,} ({100*tfs_percent/total_records:.1f}%)")
print(f"  TFS suppressed (poverty_student_count): {tfs_poverty_ct:,} ({100*tfs_poverty_ct/total_records:.1f}%)")
print(f"  TFS suppressed (total_student_count): {tfs_total_ct:,} ({100*tfs_total_ct/total_records:.1f}%)")

# Direct cert percentage range validation (0-100%)
print("\nDirect certification percentage range validation (valid: 0-100%):")
perc_ranges = dc_silver.filter(
    "direct_cert_percent IS NOT NULL"
).agg(
    spark_min('direct_cert_percent').alias('min_pct'),
    spark_max('direct_cert_percent').alias('max_pct'),
    spark_avg('direct_cert_percent').alias('avg_pct')
).collect()[0]

print(f"  Min: {perc_ranges['min_pct']:.1f}%")
print(f"  Max: {perc_ranges['max_pct']:.1f}%")
print(f"  Avg: {perc_ranges['avg_pct']:.1f}%")
if 0 <= perc_ranges['min_pct'] and perc_ranges['max_pct'] <= 100:
    print("✓ All percentages within valid range")
else:
    print("⚠️  WARNING: Percentages outside expected range!")

# Count consistency (poverty_count <= total_count)
print("\nCount consistency check (poverty_count <= total_count):")
inconsistent_counts = dc_silver.filter(
    "poverty_student_count IS NOT NULL AND total_student_count IS NOT NULL AND poverty_student_count > total_student_count"
).count()

if inconsistent_counts > 0:
    print(f"⚠️  WARNING: {inconsistent_counts:,} records where poverty count > total count")
    dc_silver.filter(
        "poverty_student_count > total_student_count"
    ).select('school_year', 'district_name', 'institution_name', 
             'poverty_student_count', 'total_student_count').show(5, truncate=False)
else:
    print("✓ All counts consistent")

# Detail level distribution
print("\nDetail level distribution:")
dc_silver.groupBy('detail_level').agg(cnt('*').alias('record_count')).show(truncate=False)

# Year coverage
print("\nYear coverage:")
year_dist = dc_silver.groupBy('school_year', 'fiscal_year').agg(
    cnt('*').alias('total_records')
).orderBy('fiscal_year')
print("Expected: ~235 districts + ~2,500 schools per year")
year_dist.show(20, truncate=False)

# Statewide average direct cert percentage by year
print("\nStatewide average direct cert % by year (district-level):")
dc_silver.filter("detail_level = 'District' AND direct_cert_percent IS NOT NULL") \
    .groupBy('school_year', 'fiscal_year').agg(
        spark_avg('direct_cert_percent').alias('avg_direct_cert_pct'),
        cnt('*').alias('district_count')
    ).orderBy('fiscal_year').show(20, truncate=False)

# Top 10 districts by direct cert % (2023-24)
print("\nTop 10 districts by direct cert % (2023-24):")
dc_silver.filter(
    "school_year = '23-24' AND detail_level = 'District' AND direct_cert_percent IS NOT NULL"
).select(
    'district_name', 'direct_cert_percent', 'poverty_student_count', 'total_student_count'
).orderBy(col('direct_cert_percent').desc()).show(10, truncate=False)

print("\n✓ Silver table ready for gold layer transformations")

# COMMAND ----------

# DBTITLE 1,Direct Certification Silver Layer - Overview
# MAGIC %md
# MAGIC # Direct Certification Silver Layer Pipeline
# MAGIC
# MAGIC **Purpose:** Clean and consolidate direct certification data from district and school bronze tables
# MAGIC
# MAGIC **Source Tables:**
# MAGIC * `workspace.bronze.direct_certification_district` (2,591 rows, 2015-2025)
# MAGIC * `workspace.bronze.direct_certification_school` (27,503 rows, 2015-2025)
# MAGIC
# MAGIC **Output:** `workspace.silver.direct_certification` (combined district + school, ~2,700 rows/year)
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## What is Direct Certification?
# MAGIC
# MAGIC **Direct Certification** measures the percentage of K-12 students who are **directly certified for free meals** without needing to submit a household income application.
# MAGIC
# MAGIC Students are directly certified if they:
# MAGIC * Receive SNAP (food stamps)
# MAGIC * Receive TANF (welfare)
# MAGIC * Are in foster care
# MAGIC * Are homeless or migrant
# MAGIC
# MAGIC **Why it matters:**
# MAGIC * **Primary poverty indicator** in education data
# MAGIC * **Drives Title I funding** (federal funding for high-poverty schools)
# MAGIC * **Essential for equity analysis** - Understanding achievement gaps by economic status
# MAGIC * **More reliable than F/R lunch eligibility** (since Community Eligibility Provision schools don't collect forms)
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Key Transformations
# MAGIC
# MAGIC ### 1. Combine District + School
# MAGIC * **Union** district and school tables
# MAGIC * Add `detail_level` column ('District' or 'School')
# MAGIC * Standardize column names (SYSTEM_ID → district_code, SCHOOL_ID → institution_number)
# MAGIC
# MAGIC ### 2. Year Field
# MAGIC * **Use FISCAL_YEAR** (not SCHOOL_YEAR, which is mostly NULL in bronze)
# MAGIC * **Format:** 2024.0 → '2023-24' (FY 2024 = school year 2023-24)
# MAGIC
# MAGIC ### 3. TFS Suppression Handling
# MAGIC * **TFS** = "Too Few Students" (privacy threshold)
# MAGIC * Converted to NULL for all numeric columns
# MAGIC * Preserves data integrity for aggregations
# MAGIC
# MAGIC ### 4. Type Casting
# MAGIC * **Counts:** Cast from string to IntegerType
# MAGIC * **Percentages:** Cast from string to DecimalType(5,1)
# MAGIC
# MAGIC ### 5. Institution Key
# MAGIC * **`institution_key`** = composite join key for linking to test scores, attendance, etc.
# MAGIC * **Format:** `district_code_institution_number`
# MAGIC * District-level records use 'ALL' as institution_number
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Schema Overview
# MAGIC
# MAGIC **Core Fields (10):**
# MAGIC | Column | Type | Description |
# MAGIC |--------|------|-------------|
# MAGIC | school_year | string | Academic year (e.g., '2023-24') |
# MAGIC | fiscal_year | integer | Federal fiscal year (e.g., 2024) |
# MAGIC | detail_level | string | 'District' or 'School' |
# MAGIC | district_code | string | System ID (e.g., '601') |
# MAGIC | district_name | string | System name (e.g., 'Appling County') |
# MAGIC | institution_number | string | School ID ('ALL' for district-level) |
# MAGIC | institution_name | string | School name (NULL for district-level) |
# MAGIC | **institution_key** | string | Composite join key |
# MAGIC | source_year | string | Source file year |
# MAGIC | source_file | string | Source file name |
# MAGIC
# MAGIC **Metrics (3):**
# MAGIC | Column | Type | Description |
# MAGIC |--------|------|-------------|
# MAGIC | direct_cert_percent | decimal(5,1) | % of students directly certified |
# MAGIC | poverty_student_count | integer | Count of directly certified students |
# MAGIC | total_student_count | integer | Total K-12 enrollment |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Data Notes
# MAGIC
# MAGIC **Foster Student Inclusion:**
# MAGIC * **2017 and earlier:** Did NOT include foster students
# MAGIC * **2018 and later:** INCLUDES foster students
# MAGIC * **Implication:** 2017 vs 2018+ not directly comparable (expect ~0.5-1% increase in 2018)
# MAGIC
# MAGIC **Bronze Data Quality Issues:**
# MAGIC * Bronze tables have weird column names ("*Note: The 2017...", "_c1", "_c2") due to CSV parsing issues
# MAGIC * These are header rows or notes that got imported as columns
# MAGIC * Silver layer drops these artifact columns

# COMMAND ----------

# DBTITLE 1,Build direct_certification silver table
# Direct Certification Silver Layer Pipeline
# Purpose: Union district and school direct certification data
# Output: workspace.silver.direct_certification

from pyspark.sql.functions import col, when, concat_ws, count as cnt, lit, regexp_replace
from pyspark.sql.types import IntegerType, DecimalType

print("=== Direct Certification Silver Layer Pipeline ===\n")

# Load bronze tables
print("Loading bronze tables...")
dc_dist = spark.table("workspace.bronze.direct_certification_district")
dc_school = spark.table("workspace.bronze.direct_certification_school")

print(f"  District rows: {dc_dist.count():,}")
print(f"  School rows: {dc_school.count():,}")

print("\n=== Step 1: Check for duplicates ===\n")

# Check district duplicates
district_dupes = dc_dist.filter("FISCAL_YEAR IS NOT NULL").groupBy(
    'FISCAL_YEAR', 'SYSTEM_ID'
).agg(cnt('*').alias('row_count')).filter('row_count > 1')

district_dupe_count = district_dupes.count()
if district_dupe_count > 0:
    print(f"⚠️  District: Found {district_dupe_count:,} duplicate keys")
    district_dupes.show(5, truncate=False)
else:
    print("✓ District: No duplicates")

# Check school duplicates  
school_dupes = dc_school.filter("FISCAL_YEAR IS NOT NULL").groupBy(
    'FISCAL_YEAR', 'SYSTEM_ID', 'SCHOOL_ID'
).agg(cnt('*').alias('row_count')).filter('row_count > 1')

school_dupe_count = school_dupes.count()
if school_dupe_count > 0:
    print(f"⚠️  School: Found {school_dupe_count:,} duplicate keys")
    school_dupes.show(5, truncate=False)
else:
    print("✓ School: No duplicates")

print("\n=== Step 2: Remove duplicates & filter valid years ===\n")

# Filter to valid records (FISCAL_YEAR not null) and deduplicate
dc_dist_clean = dc_dist.filter("FISCAL_YEAR IS NOT NULL") \
    .dropDuplicates(['FISCAL_YEAR', 'SYSTEM_ID'])
    
dc_school_clean = dc_school.filter("FISCAL_YEAR IS NOT NULL") \
    .dropDuplicates(['FISCAL_YEAR', 'SYSTEM_ID', 'SCHOOL_ID'])

print(f"District after filtering: {dc_dist_clean.count():,} rows")
print(f"School after filtering: {dc_school_clean.count():,} rows")

print("\n=== Step 3: Standardize and union ===\n")

# Helper function: Convert fiscal year to school year format
# FY 2024 = School Year 2023-24
def fiscal_to_school_year(fy_col):
    return concat_ws('-', 
        (col(fy_col) - 1).cast('int').cast('string').substr(-2, 2),
        col(fy_col).cast('int').cast('string').substr(-2, 2)
    )

# Helper functions for TFS and casting
def cast_count(col_name):
    return when(col(col_name) == 'TFS', None) \
        .when(col(col_name).isNull(), None) \
        .otherwise(col(col_name)).cast(IntegerType()).alias(col_name)

def cast_percent(col_name):
    return when(col(col_name) == 'TFS', None) \
        .when(col(col_name).isNull(), None) \
        .otherwise(col(col_name)).cast(DecimalType(5,1)).alias(col_name)

# District records
district_silver = dc_dist_clean.select(
    fiscal_to_school_year('FISCAL_YEAR').alias('school_year'),
    col('FISCAL_YEAR').cast(IntegerType()).alias('fiscal_year'),
    lit('District').alias('detail_level'),
    regexp_replace(col('SYSTEM_ID'), r'\\.0$', '').alias('district_code'),
    col('SYSTEM_NAME').alias('district_name'),
    lit('ALL').alias('institution_number'),
    lit(None).cast('string').alias('institution_name'),
    concat_ws('_', regexp_replace(col('SYSTEM_ID'), r'\\.0$', ''), lit('ALL')).alias('institution_key'),
    cast_percent('direct_cert_perc'),
    cast_count('K12_POVERTY_STUDENT_CT'),
    cast_count('K12_STUDENT_COUNT'),
    col('source_year'),
    col('source_file')
).withColumnRenamed('direct_cert_perc', 'direct_cert_percent') \
 .withColumnRenamed('K12_POVERTY_STUDENT_CT', 'poverty_student_count') \
 .withColumnRenamed('K12_STUDENT_COUNT', 'total_student_count')

# School records
school_silver = dc_school_clean.select(
    fiscal_to_school_year('FISCAL_YEAR').alias('school_year'),
    col('FISCAL_YEAR').cast(IntegerType()).alias('fiscal_year'),
    lit('School').alias('detail_level'),
    regexp_replace(col('SYSTEM_ID'), r'\\.0$', '').alias('district_code'),
    col('SYSTEM_NAME').alias('district_name'),
    regexp_replace(col('SCHOOL_ID'), r'\\.0$', '').alias('institution_number'),
    col('SCHOOL_NAME').alias('institution_name'),
    concat_ws('_',
        regexp_replace(col('SYSTEM_ID'), r'\\.0$', ''),
        regexp_replace(col('SCHOOL_ID'), r'\\.0$', '')
    ).alias('institution_key'),
    cast_percent('direct_cert_perc'),
    cast_count('K12_POVERTY_STUDENT_CT'),
    cast_count('K12_STUDENT_COUNT'),
    col('source_year'),
    col('source_file')
).withColumnRenamed('direct_cert_perc', 'direct_cert_percent') \
 .withColumnRenamed('K12_POVERTY_STUDENT_CT', 'poverty_student_count') \
 .withColumnRenamed('K12_STUDENT_COUNT', 'total_student_count')

# Union
dc_silver = district_silver.union(school_silver)

print(f"Silver table built: {dc_silver.count():,} rows")
print(f"Silver columns: {len(dc_silver.columns)}")

print("\nSample (2023-24 districts with highest direct cert %):")
dc_silver.filter("school_year = '23-24' AND detail_level = 'District'") \
    .select('district_name', 'direct_cert_percent', 'poverty_student_count', 'total_student_count') \
    .orderBy(col('direct_cert_percent').desc()).show(5, truncate=False)

# COMMAND ----------

# DBTITLE 1,Write and validate silver table
from pyspark.sql.functions import min as spark_min, max as spark_max, avg as spark_avg

print("=== Writing silver table ===\n")

dc_silver.write.format("delta").mode("overwrite").option("overwriteSchema", "true") \
    .saveAsTable("workspace.silver.direct_certification")

print("✓ workspace.silver.direct_certification written successfully")

print("\n=== Validation & Quality Checks ===\n")

# Check business key uniqueness
print("Checking business key uniqueness...")
key_check = dc_silver.groupBy(
    'school_year', 'district_code', 'institution_number'
).agg(cnt('*').alias('key_count')).filter('key_count > 1')

if key_check.count() > 0:
    print("⚠️  WARNING: Duplicate business keys found!")
    key_check.show(5, truncate=False)
else:
    print("✓ All business keys are unique")

# TFS suppression analysis
print("\nTFS suppression analysis:")
total_records = dc_silver.count()
tfs_percent = dc_silver.filter("direct_cert_percent IS NULL").count()
tfs_poverty_ct = dc_silver.filter("poverty_student_count IS NULL").count()
tfs_total_ct = dc_silver.filter("total_student_count IS NULL").count()

print(f"  Total records: {total_records:,}")
print(f"  TFS suppressed (direct_cert_percent): {tfs_percent:,} ({100*tfs_percent/total_records:.1f}%)")
print(f"  TFS suppressed (poverty_student_count): {tfs_poverty_ct:,} ({100*tfs_poverty_ct/total_records:.1f}%)")
print(f"  TFS suppressed (total_student_count): {tfs_total_ct:,} ({100*tfs_total_ct/total_records:.1f}%)")

# Direct cert percentage range validation (0-100%)
print("\nDirect certification percentage range validation (valid: 0-100%):")
perc_ranges = dc_silver.filter(
    "direct_cert_percent IS NOT NULL"
).agg(
    spark_min('direct_cert_percent').alias('min_pct'),
    spark_max('direct_cert_percent').alias('max_pct'),
    spark_avg('direct_cert_percent').alias('avg_pct')
).collect()[0]

print(f"  Min: {perc_ranges['min_pct']:.1f}%")
print(f"  Max: {perc_ranges['max_pct']:.1f}%")
print(f"  Avg: {perc_ranges['avg_pct']:.1f}%")
if 0 <= perc_ranges['min_pct'] and perc_ranges['max_pct'] <= 100:
    print("✓ All percentages within valid range")
else:
    print("⚠️  WARNING: Percentages outside expected range!")

# Count consistency (poverty_count <= total_count)
print("\nCount consistency check (poverty_count <= total_count):")
inconsistent_counts = dc_silver.filter(
    "poverty_student_count IS NOT NULL AND total_student_count IS NOT NULL AND poverty_student_count > total_student_count"
).count()

if inconsistent_counts > 0:
    print(f"⚠️  WARNING: {inconsistent_counts:,} records where poverty count > total count")
    dc_silver.filter(
        "poverty_student_count > total_student_count"
    ).select('school_year', 'district_name', 'institution_name', 
             'poverty_student_count', 'total_student_count').show(5, truncate=False)
else:
    print("✓ All counts consistent")

# Detail level distribution
print("\nDetail level distribution:")
dc_silver.groupBy('detail_level').agg(cnt('*').alias('record_count')).show(truncate=False)

# Year coverage
print("\nYear coverage:")
year_dist = dc_silver.groupBy('school_year', 'fiscal_year').agg(
    cnt('*').alias('total_records')
).orderBy('fiscal_year')
print("Expected: ~235 districts + ~2,500 schools per year")
year_dist.show(20, truncate=False)

# Statewide average direct cert percentage by year
print("\nStatewide average direct cert % by year (district-level):")
dc_silver.filter("detail_level = 'District' AND direct_cert_percent IS NOT NULL") \
    .groupBy('school_year', 'fiscal_year').agg(
        spark_avg('direct_cert_percent').alias('avg_direct_cert_pct'),
        cnt('*').alias('district_count')
    ).orderBy('fiscal_year').show(20, truncate=False)

# Top 10 districts by direct cert % (2023-24)
print("\nTop 10 districts by direct cert % (2023-24):")
dc_silver.filter(
    "school_year = '23-24' AND detail_level = 'District' AND direct_cert_percent IS NOT NULL"
).select(
    'district_name', 'direct_cert_percent', 'poverty_student_count', 'total_student_count'
).orderBy(col('direct_cert_percent').desc()).show(10, truncate=False)

print("\n✓ Silver table ready for gold layer transformations")
