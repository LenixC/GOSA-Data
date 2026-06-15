# Databricks notebook source
# DBTITLE 1,Fact Graduation - Overview
# MAGIC %md
# MAGIC # Fact: Graduation
# MAGIC
# MAGIC **Purpose:** Unified view of graduation and completion outcomes
# MAGIC
# MAGIC **Grain:** School × Year × Subgroup × Cohort Type
# MAGIC
# MAGIC **Source Tables:**
# MAGIC * `workspace.silver.graduation_rate` - 4-year and 5-year graduation cohorts
# MAGIC
# MAGIC **Dimension Tables:**
# MAGIC * `workspace.gold.dim_school`
# MAGIC * `workspace.gold.dim_year`
# MAGIC * `workspace.gold.dim_subgroup`
# MAGIC
# MAGIC **Output:** `workspace.gold.fact_graduation`
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Facts (Measures)
# MAGIC * **graduate_count** - Number of graduates
# MAGIC * **graduation_rate** - Percent graduating
# MAGIC * **cohort_count** - Total students in cohort
# MAGIC * **cohort_type** - 4-Year or 5-Year cohort
# MAGIC
# MAGIC ## Dimensions (Foreign Keys)
# MAGIC * **school_key** → dim_school
# MAGIC * **year_key** → dim_year
# MAGIC * **subgroup_key** → dim_subgroup

# COMMAND ----------

# DBTITLE 1,Load dimensions
from pyspark.sql.functions import col, lit, coalesce

print("=== Loading Dimension Tables ===")

# Load dimensions
dim_school = spark.table("workspace.gold.dim_school")
dim_year = spark.table("workspace.gold.dim_year")
dim_subgroup = spark.table("workspace.gold.dim_subgroup")

print(f"dim_school: {dim_school.count():,} rows")
print(f"dim_year: {dim_year.count():,} rows")
print(f"dim_subgroup: {dim_subgroup.count():,} rows")

# COMMAND ----------

# DBTITLE 1,Transform graduation_rate to fact grain
print("=== Transforming Graduation Rate ===")

# Load graduation silver (School level only)
grad_silver = spark.table("workspace.silver.graduation_rate").filter("detail_level = 'School'")
print(f"Graduation silver (schools only): {grad_silver.count():,} rows")

# Transform to fact grain: school x year x subgroup x cohort
grad_fact = grad_silver.select(
    col('institution_key'),
    col('school_year'),
    col('subgroup_label').alias('subgroup'),
    col('cohort_type'),
    col('graduate_count'),
    col('graduation_rate'),
    col('cohort_count')
)

print(f"Graduation fact records: {grad_fact.count():,} rows")
print("\nSample:")
grad_fact.show(5, truncate=False)

# Show cohort type distribution
print("\nCohort Type Distribution:")
grad_fact.groupBy('cohort_type').count().orderBy('cohort_type').show()

# COMMAND ----------

# DBTITLE 1,Join to dimensions and build final fact table
print("=== Joining to Dimension Tables ===")

# Join to dim_school
fact_with_school = grad_fact.join(
    dim_school,
    grad_fact.institution_key == dim_school.institution_key,
    'left'
).select(
    grad_fact['*'],
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

# Join to dim_subgroup
fact_with_subgroup = fact_with_year.join(
    dim_subgroup,
    fact_with_year.subgroup == dim_subgroup.subgroup,
    'left'
).select(
    fact_with_year['*'],
    dim_subgroup.subgroup_key
)

print(f"After subgroup join: {fact_with_subgroup.count():,} rows")

# Check for unmatched subgroups
unmatched_subgroups = fact_with_subgroup.filter("subgroup_key IS NULL").select('subgroup').distinct().count()
if unmatched_subgroups > 0:
    print(f"  ⚠️  {unmatched_subgroups} subgroups not found in dim_subgroup")
    print("\n  Unmatched subgroups:")
    fact_with_subgroup.filter("subgroup_key IS NULL").select('subgroup').distinct().show(truncate=False)
else:
    print("  ✓ All subgroups matched")

# Build final fact table
fact_graduation = fact_with_subgroup.select(
    col('school_key'),
    col('year_key'),
    col('subgroup_key'),
    col('cohort_type'),
    col('graduate_count'),
    col('graduation_rate'),
    col('cohort_count')
)

print(f"\nFinal fact_graduation: {fact_graduation.count():,} rows")
print("\nSample:")
fact_graduation.show(10)

# COMMAND ----------

# DBTITLE 1,Validate fact table
from pyspark.sql.functions import count as cnt, sum as spark_sum, avg, min as spark_min, max as spark_max

print("=== Fact Table Validation ===\n")

# 1. Check for NULL foreign keys
print("1. NULL Foreign Keys:")
null_school = fact_graduation.filter("school_key IS NULL").count()
null_year = fact_graduation.filter("year_key IS NULL").count()
null_subgroup = fact_graduation.filter("subgroup_key IS NULL").count()

print(f"   school_key NULL: {null_school:,}")
print(f"   year_key NULL: {null_year:,}")
print(f"   subgroup_key NULL: {null_subgroup:,}")

if null_school == 0 and null_year == 0 and null_subgroup == 0:
    print("   ✓ All foreign keys populated")
else:
    print("   ⚠️ Some foreign keys are NULL")

# 2. Check graduation rate ranges
print("\n2. Graduation Rate Statistics:")
stats = fact_graduation.filter("graduation_rate IS NOT NULL").select(
    avg('graduation_rate').alias('avg_rate'),
    spark_min('graduation_rate').alias('min_rate'),
    spark_max('graduation_rate').alias('max_rate'),
    cnt('*').alias('non_null_count')
).collect()[0]

print(f"   Avg graduation rate: {stats['avg_rate']:.1f}%")
print(f"   Min rate: {stats['min_rate']:.1f}%")
print(f"   Max rate: {stats['max_rate']:.1f}%")
print(f"   Non-NULL records: {stats['non_null_count']:,}")

# 3. Check graduate counts
print("\n3. Graduate Count Statistics:")
grad_stats = fact_graduation.filter("graduate_count IS NOT NULL").select(
    spark_sum('graduate_count').alias('total_graduates'),
    avg('graduate_count').alias('avg_per_record'),
    spark_min('graduate_count').alias('min_count'),
    spark_max('graduate_count').alias('max_count')
).collect()[0]

print(f"   Total graduates: {grad_stats['total_graduates']:,.0f}")
print(f"   Avg per record: {grad_stats['avg_per_record']:.0f}")
print(f"   Min count: {grad_stats['min_count']:.0f}")
print(f"   Max count: {grad_stats['max_count']:.0f}")

# 4. Check cohort type distribution
print("\n4. Cohort Type Distribution:")
fact_graduation.groupBy('cohort_type').agg(cnt('*').alias('record_count')).orderBy('cohort_type').show()

# 5. Year coverage
print("\n5. Year Coverage:")
fact_graduation.groupBy('year_key').count().orderBy('year_key').show()

# COMMAND ----------

# DBTITLE 1,Write to gold
fact_graduation.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("workspace.gold.fact_graduation")
print(f"✓ workspace.gold.fact_graduation: {spark.table('workspace.gold.fact_graduation').count():,} rows")

# COMMAND ----------


