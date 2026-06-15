# Databricks notebook source
# DBTITLE 1,Fact Revenues - Overview
# MAGIC %md
# MAGIC # Fact: Revenues & Expenditures
# MAGIC
# MAGIC **Purpose:** Financial metrics for revenue and spending analysis
# MAGIC
# MAGIC **Grain:** School × Year × Revenue/Expenditure Type × Category
# MAGIC
# MAGIC **Source Tables:**
# MAGIC * `workspace.silver.revenues` - Financial data (revenues and expenditures)
# MAGIC
# MAGIC **Dimension Tables:**
# MAGIC * `workspace.gold.dim_school`
# MAGIC * `workspace.gold.dim_year`
# MAGIC
# MAGIC **Output:** `workspace.gold.fact_revenues`
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Facts (Measures)
# MAGIC * **amount** - Dollar amount
# MAGIC * **amount_per_fte** - Dollars per FTE (full-time equivalent)
# MAGIC
# MAGIC ## Dimensions (Foreign Keys)
# MAGIC * **school_key** → dim_school
# MAGIC * **year_key** → dim_year
# MAGIC
# MAGIC ## Attributes (Degenerate Dimensions)
# MAGIC * **revenue_expenditure_type** - 'Revenues' or 'Expenditures'
# MAGIC * **category** - Specific revenue/expenditure category

# COMMAND ----------

# DBTITLE 1,Load dimensions
from pyspark.sql.functions import col, lit

print("=== Loading Dimension Tables ===")

# Load dimensions
dim_school = spark.table("workspace.gold.dim_school")
dim_year = spark.table("workspace.gold.dim_year")

print(f"dim_school: {dim_school.count():,} rows")
print(f"dim_year: {dim_year.count():,} rows")

# COMMAND ----------

# DBTITLE 1,Transform revenues to fact grain
print("=== Transforming Revenues ===")

# Load revenues silver (School level only)
rev_silver = spark.table("workspace.silver.revenues").filter("detail_level = 'School'")
print(f"Revenues silver (schools only): {rev_silver.count():,} rows")

# Transform to fact grain: school x year x type x category
rev_fact = rev_silver.select(
    col('institution_key'),
    col('school_year'),
    col('revenue_expenditure_type'),
    col('category'),
    col('amount'),
    col('amount_per_fte')
)

print(f"Revenue fact records: {rev_fact.count():,} rows")
print("\nSample:")
rev_fact.show(5, truncate=False)

# Show distribution by type
print("\nRevenue/Expenditure Type Distribution:")
rev_fact.groupBy('revenue_expenditure_type').count().orderBy('revenue_expenditure_type').show()

# COMMAND ----------

# DBTITLE 1,Join to dimensions and build final fact table
print("=== Joining to Dimension Tables ===")

# Join to dim_school
fact_with_school = rev_fact.join(
    dim_school,
    rev_fact.institution_key == dim_school.institution_key,
    'left'
).select(
    rev_fact['*'],
    dim_school.school_key
)

print(f"After school join: {fact_with_school.count():,} rows")

# Check for unmatched schools
unmatched_schools = fact_with_school.filter("school_key IS NULL").select('institution_key').distinct().count()
if unmatched_schools > 0:
    print(f"  ⚠️  {unmatched_schools} institution_keys not found in dim_school")
else:
    print("  ✓ All schools matched")

# Join to dim_year
fact_with_year = fact_with_school.join(
    dim_year,
    fact_with_school.school_year == dim_year.school_year,
    'left'
).select(
    fact_with_school['*'],
    dim_year.year_key
)

print(f"After year join: {fact_with_year.count():,} rows")

# Build final fact table
fact_revenues = fact_with_year.select(
    col('school_key'),
    col('year_key'),
    col('revenue_expenditure_type'),
    col('category'),
    col('amount'),
    col('amount_per_fte')
)

print(f"\nFinal fact_revenues: {fact_revenues.count():,} rows")
print("\nSample:")
fact_revenues.show(10, truncate=False)

# COMMAND ----------

# DBTITLE 1,Validate fact table
from pyspark.sql.functions import count as cnt, sum as spark_sum, avg, min as spark_min, max as spark_max

print("=== Fact Table Validation ===\n")

# 1. Check for NULL foreign keys
print("1. NULL Foreign Keys:")
null_school = fact_revenues.filter("school_key IS NULL").count()
null_year = fact_revenues.filter("year_key IS NULL").count()

print(f"   school_key NULL: {null_school:,}")
print(f"   year_key NULL: {null_year:,}")

if null_school == 0 and null_year == 0:
    print("   ✓ All foreign keys populated")
else:
    print("   ⚠️ Some foreign keys are NULL")

# 2. Check amount statistics
print("\n2. Amount Statistics:")
stats = fact_revenues.filter("amount IS NOT NULL").select(
    spark_sum('amount').alias('total_amount'),
    avg('amount').alias('avg_amount'),
    spark_min('amount').alias('min_amount'),
    spark_max('amount').alias('max_amount')
).collect()[0]

print(f"   Total amount: ${stats['total_amount']:,.2f}")
print(f"   Avg amount: ${stats['avg_amount']:,.2f}")
print(f"   Min amount: ${stats['min_amount']:,.2f}")
print(f"   Max amount: ${stats['max_amount']:,.2f}")

# 3. Check for negative amounts (valid for some categories)
print("\n3. Negative Amounts:")
neg_count = fact_revenues.filter("amount < 0").count()
print(f"   Records with negative amounts: {neg_count:,}")
if neg_count > 0:
    print("   ℹ️ Negative amounts can be valid (e.g., adjustments, refunds)")

# 4. Distribution by type and category
print("\n4. Top Categories by Total Amount:")
fact_revenues.groupBy('revenue_expenditure_type', 'category').agg(
    cnt('*').alias('record_count'),
    spark_sum('amount').alias('total_amount')
).orderBy(col('total_amount').desc()).show(10, truncate=False)

# 5. Year coverage
print("\n5. Year Coverage:")
fact_revenues.groupBy('year_key').count().orderBy('year_key').show()

# COMMAND ----------

# DBTITLE 1,Write to gold
fact_revenues.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("workspace.gold.fact_revenues")
print(f"✓ workspace.gold.fact_revenues: {spark.table('workspace.gold.fact_revenues').count():,} rows")

# COMMAND ----------


