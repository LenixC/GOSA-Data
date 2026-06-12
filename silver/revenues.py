# Databricks notebook source
# DBTITLE 1,Revenues Silver Layer - Overview
# MAGIC %md
# MAGIC # Revenues & Expenditures Silver Layer Pipeline
# MAGIC
# MAGIC **Purpose:** Clean and consolidate school district revenue and expenditure data from bronze layer
# MAGIC
# MAGIC **Source Table:**
# MAGIC * `workspace.bronze.revenues` (424,492 rows, 2010-2024)
# MAGIC
# MAGIC **Output:** `workspace.silver.revenues` (15 columns)
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Key Transformations
# MAGIC
# MAGIC ### 1. Schema Consolidation
# MAGIC * **Problem:** Dual schemas in bronze table
# MAGIC   * **Old format** (2010-2021): SCHOOL_YEAR, DISTRICT_CODE, SCHOOL_CODE, `Dollars per FTE`
# MAGIC   * **New format** (2022-2024): LONG_SCHOOL_YEAR, SCHOOL_DSTRCT_CD, INSTN_NUMBER, DOLLARS_PER_FTE
# MAGIC * **Solution:** Use COALESCE to merge columns
# MAGIC * **Result:** Unified schema across all years
# MAGIC
# MAGIC ### 2. Detail Level Handling
# MAGIC * **State Level:** DISTRICT_CODE = 'ALL', SCHOOL_CODE = 'ALL'
# MAGIC * **District Level:** SCHOOL_CODE = 'ALL' (real district code)
# MAGIC * **School Level:** Specific district and school codes
# MAGIC
# MAGIC ### 3. Institution Key
# MAGIC * **`institution_key`** = composite join key
# MAGIC * **Logic:** `district_code_institution_number`
# MAGIC * **Handle 'ALL':** State/district aggregates get special keys
# MAGIC
# MAGIC ### 4. Data Quality
# MAGIC * Cast monetary values to double (decimals present)
# MAGIC * Cast per-FTE values to double
# MAGIC * Standardize column names to snake_case
# MAGIC * Remove duplicate records (NULL schema rows)
# MAGIC
# MAGIC ### 5. Known Data Issues
# MAGIC * **2017-18 Incomplete Coverage:** Only 2,262 schools (vs. ~3,300+ in other years)
# MAGIC   * Missing ~1,100 schools (~12,000 records)
# MAGIC   * Source file contains incomplete data
# MAGIC   * District and state aggregates appear complete
# MAGIC   * **Impact:** ~33% of school-level records missing for 2017-18
# MAGIC   * **Recommendation:** Flag or exclude 2017-18 from time-series analyses requiring complete coverage
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Schema
# MAGIC
# MAGIC | Column | Type | Description |
# MAGIC |--------|------|-------------|
# MAGIC | school_year | string | Academic year |
# MAGIC | district_code | string | District code ('ALL' for state) |
# MAGIC | district_name | string | District name |
# MAGIC | institution_number | string | School code ('ALL' for district) |
# MAGIC | institution_name | string | School/district name |
# MAGIC | detail_level | string | State/District/School |
# MAGIC | revenue_expenditure_type | string | Revenues or Expenditures |
# MAGIC | category | string | Spending/revenue category |
# MAGIC | amount | double | Dollar amount |
# MAGIC | amount_per_fte | double | Dollars per full-time equivalent student |
# MAGIC | institution_key | string | Composite join key |
# MAGIC | grades_served | string | Grade range served |
# MAGIC | source_year | string | Source file year |
# MAGIC | source_file | string | Source filename |
# MAGIC | report_name | string | Report identifier |

# COMMAND ----------

# DBTITLE 1,Build silver table
from pyspark.sql.functions import col, concat_ws, coalesce, when, count as cnt
from pyspark.sql.types import DoubleType

rev_bronze = spark.table("workspace.bronze.revenues")
print(f"Bronze: {rev_bronze.count():,} rows")

# Remove NULL schema rows (rows where all key columns are NULL)
rev_cleaned = rev_bronze.filter(
    "(SCHOOL_YEAR IS NOT NULL OR LONG_SCHOOL_YEAR IS NOT NULL) AND "
    "(DISTRICT_CODE IS NOT NULL OR SCHOOL_DSTRCT_CD IS NOT NULL)"
)
print(f"After removing null rows: {rev_cleaned.count():,} rows")

# Deduplicate on business key
rev_deduped = rev_cleaned.dropDuplicates([
    'SCHOOL_YEAR', 'LONG_SCHOOL_YEAR', 'DISTRICT_CODE', 'SCHOOL_DSTRCT_CD',
    'SCHOOL_CODE', 'INSTN_NUMBER', 'Revenues/Expenditures', 'Description'
])
print(f"After dedup: {rev_deduped.count():,} rows")

# Build silver table with schema consolidation
rev_silver = rev_deduped.select(
    # Merge dual schemas
    coalesce(col('LONG_SCHOOL_YEAR'), col('SCHOOL_YEAR')).alias('school_year'),
    coalesce(col('SCHOOL_DSTRCT_CD'), col('DISTRICT_CODE')).alias('district_code'),
    coalesce(col('SCHOOL_DSTRCT_NM'), col('DISTRICT_NAME')).alias('district_name'),
    coalesce(col('INSTN_NUMBER'), col('SCHOOL_CODE')).alias('institution_number'),
    coalesce(col('INSTN_NAME'), col('SCHOOL_NAME')).alias('institution_name'),
    
    # Determine detail level
    when(
        (coalesce(col('SCHOOL_DSTRCT_CD'), col('DISTRICT_CODE')) == 'ALL') & 
        (coalesce(col('INSTN_NUMBER'), col('SCHOOL_CODE')) == 'ALL'),
        'State'
    ).when(
        coalesce(col('INSTN_NUMBER'), col('SCHOOL_CODE')) == 'ALL',
        'District'
    ).otherwise('School').alias('detail_level'),
    
    # Revenue/expenditure and category
    col('Revenues/Expenditures').alias('revenue_expenditure_type'),
    col('Description').alias('category'),
    
    # Cast amounts to double
    col('REV_EXP_VALUE').cast(DoubleType()).alias('amount'),
    coalesce(col('DOLLARS_PER_FTE'), col('Dollars per FTE')).cast(DoubleType()).alias('amount_per_fte'),
    
    # Create institution key
    concat_ws('_', 
        coalesce(col('SCHOOL_DSTRCT_CD'), col('DISTRICT_CODE')),
        coalesce(col('INSTN_NUMBER'), col('SCHOOL_CODE'))
    ).alias('institution_key'),
    
    # Additional fields
    col('GRADES_SERVED_DESC').alias('grades_served'),
    col('source_year'),
    col('source_file'),
    coalesce(col('#RPT_NAME'), col('source_file')).alias('report_name')
)

print(f"\nSilver transformation complete: {rev_silver.count():,} rows")
display(rev_silver.limit(5))

# COMMAND ----------

# DBTITLE 1,Comprehensive validation
from pyspark.sql.functions import min as spark_min, max as spark_max, sum as spark_sum, avg, countDistinct, stddev

print("\n=== COMPREHENSIVE DATA QUALITY VALIDATION ===\n")

# 1. Row count and duplicate validation
print("1. Row Count & Deduplication:")
bronze_total = rev_bronze.count()
silver_total = rev_silver.count()
rows_removed = bronze_total - silver_total
print(f"   Bronze: {bronze_total:,} rows")
print(f"   Removed: {rows_removed:,} rows ({rows_removed/bronze_total*100:.1f}%)")
print(f"   Silver: {silver_total:,} rows")
if rows_removed > 0:
    print(f"   ✓ {rows_removed:,} duplicates/null records removed")

# 2. Business key uniqueness
print("\n2. Business Key Uniqueness:")
dup_check = rev_silver.groupBy(
    'school_year', 'district_code', 'institution_number',
    'revenue_expenditure_type', 'category'
).agg(cnt('*').alias('dup_count')).filter('dup_count > 1').count()
print(f"   Duplicate business keys: {dup_check}")
if dup_check == 0:
    print("   ✓ All business keys unique")
else:
    print(f"   ⚠️  {dup_check} duplicate keys found")

# 3. Amount validation - comprehensive statistics
print("\n3. Amount (Dollar Values) - Statistical Summary:")
amount_stats = rev_silver.filter("amount IS NOT NULL").agg(
    cnt('*').alias('count'),
    spark_min('amount').alias('min'),
    spark_max('amount').alias('max'),
    avg('amount').alias('mean'),
    stddev('amount').alias('stddev'),
    spark_sum('amount').alias('total')
).collect()[0]

print(f"   Count: {amount_stats['count']:,} records")
print(f"   Min: ${amount_stats['min']:,.2f}")
print(f"   Max: ${amount_stats['max']:,.2f}")
print(f"   Mean: ${amount_stats['mean']:,.2f}")
print(f"   Std Dev: ${amount_stats['stddev']:,.2f}")
print(f"   Total: ${amount_stats['total']:,.2f}")

# Check for negative amounts
neg_amounts = rev_silver.filter("amount < 0").count()
if neg_amounts == 0:
    print("   ✓ No negative amounts")
else:
    print(f"   ⚠️  {neg_amounts:,} negative amounts found")

# Check for zero amounts
zero_amounts = rev_silver.filter("amount = 0").count()
print(f"   Zero amounts: {zero_amounts:,} ({zero_amounts/silver_total*100:.1f}%)")

# 4. Per-FTE validation
print("\n4. Amount per FTE - Statistical Summary:")
fte_stats = rev_silver.filter("amount_per_fte IS NOT NULL").agg(
    cnt('*').alias('count'),
    spark_min('amount_per_fte').alias('min'),
    spark_max('amount_per_fte').alias('max'),
    avg('amount_per_fte').alias('mean'),
    stddev('amount_per_fte').alias('stddev')
).collect()[0]

print(f"   Count: {fte_stats['count']:,} records")
print(f"   Min: ${fte_stats['min']:,.2f}")
print(f"   Max: ${fte_stats['max']:,.2f}")
print(f"   Mean: ${fte_stats['mean']:,.2f}")
print(f"   Std Dev: ${fte_stats['stddev']:,.2f}")

null_fte = rev_silver.filter("amount_per_fte IS NULL").count()
if null_fte == 0:
    print("   ✓ No NULL per-FTE values")
else:
    print(f"   ⚠️  {null_fte:,} NULL per-FTE values ({null_fte/silver_total*100:.1f}%)")

# 5. Year coverage
print("\n5. Year Coverage:")
rev_silver.groupBy('school_year').agg(
    cnt('*').alias('record_count')
).orderBy('school_year').show(50, truncate=False)

# 6. Detail level distribution
print("\n6. Detail Level Distribution:")
rev_silver.groupBy('detail_level').agg(
    cnt('*').alias('record_count'),
    spark_sum('amount').alias('total_amount')
).orderBy('detail_level').show(truncate=False)

# 7. Revenue vs Expenditure distribution
print("\n7. Revenue vs Expenditure Distribution:")
rev_silver.groupBy('revenue_expenditure_type').agg(
    cnt('*').alias('record_count'),
    countDistinct('category').alias('unique_categories'),
    spark_sum('amount').alias('total_amount')
).orderBy('revenue_expenditure_type').show(truncate=False)

# 8. Category breakdown
print("\n8. Top Categories by Total Amount:")
rev_silver.groupBy('revenue_expenditure_type', 'category').agg(
    cnt('*').alias('record_count'),
    spark_sum('amount').alias('total_amount'),
    avg('amount').alias('avg_amount')
).orderBy(col('total_amount').desc()).show(20, truncate=False)

# 9. Institution key validation
print("\n9. Institution Key Validation:")
invalid_keys = rev_silver.filter(
    "institution_key IS NULL OR institution_key = '' OR institution_key NOT LIKE '%_%'"
).count()
if invalid_keys == 0:
    print("   ✓ All institution keys properly formatted")
else:
    print(f"   ⚠️  {invalid_keys} invalid institution keys")

# Check key patterns
print("\n   Key patterns:")
rev_silver.groupBy('institution_key').agg(
    cnt('*').alias('record_count')
).orderBy(col('record_count').desc()).show(10, truncate=False)

# 10. NULL value summary across all columns
print("\n10. NULL Value Summary:")
null_summary = []
for column in rev_silver.columns:
    null_count = rev_silver.filter(f"`{column}` IS NULL").count()
    if null_count > 0:
        null_summary.append((column, null_count, f"{null_count/silver_total*100:.2f}%"))

if null_summary:
    print("   Columns with NULL values:")
    for col_name, count, pct in null_summary:
        print(f"     {col_name:30} {count:>10,} ({pct})")
else:
    print("   ✓ No NULL values in any column")

print("\n✓ Validation complete")

# COMMAND ----------

# DBTITLE 1,Investigate zero and negative amounts
print("=== Investigating Zero and Negative Dollar Amounts ===\n")

# Zero amounts
zero_amt = rev_silver.filter("amount = 0")
zero_count = zero_amt.count()

if zero_count > 0:
    print(f"1. ZERO AMOUNTS: {zero_count:,} records\n")
    
    print("   Distribution by category:")
    zero_amt.groupBy('revenue_expenditure_type', 'category').agg(
        cnt('*').alias('zero_count')
    ).orderBy('revenue_expenditure_type', col('zero_count').desc()).show(20, truncate=False)
    
    print("\n   Distribution by detail level:")
    zero_amt.groupBy('detail_level').agg(
        cnt('*').alias('zero_count')
    ).show(truncate=False)
    
    print("\n   Sample zero amount records:")
    zero_amt.select(
        'school_year', 'district_name', 'institution_name', 'detail_level',
        'revenue_expenditure_type', 'category', 'amount', 'amount_per_fte'
    ).show(10, truncate=False)
else:
    print("1. ZERO AMOUNTS: None found ✓")

# Negative amounts
print("\n" + "="*60 + "\n")
neg_amt = rev_silver.filter("amount < 0")
neg_count = neg_amt.count()

if neg_count > 0:
    print(f"2. NEGATIVE AMOUNTS: {neg_count:,} records\n")
    
    print("   Distribution by category:")
    neg_amt.groupBy('revenue_expenditure_type', 'category').agg(
        cnt('*').alias('negative_count'),
        spark_sum('amount').alias('total_negative_amount')
    ).orderBy('revenue_expenditure_type', col('negative_count').desc()).show(20, truncate=False)
    
    print("\n   Sample negative amount records:")
    neg_amt.select(
        'school_year', 'district_name', 'institution_name', 'detail_level',
        'revenue_expenditure_type', 'category', 'amount', 'amount_per_fte'
    ).orderBy('amount').show(10, truncate=False)
else:
    print("2. NEGATIVE AMOUNTS: None found ✓")

# Very high amounts (outliers)
print("\n" + "="*60 + "\n")
print("3. OUTLIERS - Top 10 Highest Amounts:\n")
rev_silver.filter("amount > 0").select(
    'school_year', 'district_name', 'institution_name', 'detail_level',
    'revenue_expenditure_type', 'category', 'amount', 'amount_per_fte'
).orderBy(col('amount').desc()).show(10, truncate=False)

print("\n✓ Investigation complete")

# COMMAND ----------

# DBTITLE 1,Write to silver
rev_silver.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("workspace.silver.revenues")
print(f"✓ workspace.silver.revenues: {spark.table('workspace.silver.revenues').count():,} rows")

# COMMAND ----------

# DBTITLE 1,Investigate 2017-18 data gap
print("=== Investigating 2017-18 Data Anomaly ===\n")

# Compare record counts across years
print("1. Records by Detail Level and Year:")
rev_silver.groupBy('school_year', 'detail_level').agg(
    cnt('*').alias('record_count')
).orderBy('school_year', 'detail_level').show(50, truncate=False)

print("\n2. Records by Revenue/Expenditure Type:")
rev_silver.groupBy('school_year', 'revenue_expenditure_type').agg(
    cnt('*').alias('record_count')
).orderBy('school_year', 'revenue_expenditure_type').show(50, truncate=False)

# Check if certain categories are missing in 2017-18
print("\n3. Category Breakdown for 2017-18 vs Other Years:")
print("\n   2017-18 categories:")
rev_2017 = rev_silver.filter("school_year = '2017-18'")
rev_2017.groupBy('revenue_expenditure_type', 'category').agg(
    cnt('*').alias('count')
).orderBy('revenue_expenditure_type', 'category').show(50, truncate=False)

print("\n   2018-19 categories (for comparison):")
rev_2018 = rev_silver.filter("school_year = '2018-19'")
rev_2018.groupBy('revenue_expenditure_type', 'category').agg(
    cnt('*').alias('count')
).orderBy('revenue_expenditure_type', 'category').show(50, truncate=False)

# Check unique districts
print("\n4. District Count Comparison:")
for year in ['2017-18', '2018-19', '2019-20', '2020-21']:
    dist_count = rev_silver.filter(f"school_year = '{year}'").select('district_code').distinct().count()
    school_count = rev_silver.filter(f"school_year = '{year}' AND detail_level = 'School'").select('institution_key').distinct().count()
    print(f"   {year}: {dist_count} districts, {school_count} schools")

# Check source files for 2017-18
print("\n5. Source Files for 2017-18:")
rev_2017.select('source_file').distinct().show(truncate=False)

print("\n6. Detail Level Distribution:")
print("   2017-18:")
rev_2017.groupBy('detail_level').agg(cnt('*').alias('count')).show()
print("   2018-19:")
rev_2018.groupBy('detail_level').agg(cnt('*').alias('count')).show()

print("\n✓ Investigation complete")

# COMMAND ----------


