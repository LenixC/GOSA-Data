# Databricks notebook source
# DBTITLE 1,Fact Dropout - Overview
# MAGIC %md
# MAGIC # Fact: Dropout Rate
# MAGIC
# MAGIC **Purpose:** Student dropout outcomes by grade range and subgroup
# MAGIC
# MAGIC **Grain:** School × Year × Subgroup × Grade Range
# MAGIC
# MAGIC **Source Tables:**
# MAGIC * `workspace.silver.dropout_rate` - Dropout data for grades 7-12 and 9-12
# MAGIC
# MAGIC **Dimension Tables:**
# MAGIC * `workspace.gold.dim_school`
# MAGIC * `workspace.gold.dim_year`
# MAGIC * `workspace.gold.dim_subgroup`
# MAGIC
# MAGIC **Output:** `workspace.gold.fact_dropout`
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Facts (Measures)
# MAGIC * **dropout_count** - Number of dropouts
# MAGIC * **dropout_rate** - Dropout percentage
# MAGIC
# MAGIC ## Dimensions (Foreign Keys)
# MAGIC * **school_key** → dim_school
# MAGIC * **year_key** → dim_year
# MAGIC * **subgroup_key** → dim_subgroup
# MAGIC
# MAGIC ## Attributes (Degenerate Dimensions)
# MAGIC * **grade_range** - '7-12' or '9-12' cohort

# COMMAND ----------

# DBTITLE 1,Load dimensions
from pyspark.sql.functions import col

print("=== Loading Dimension Tables ===")

# Load dimensions
dim_school = spark.table("workspace.gold.dim_school")
dim_year = spark.table("workspace.gold.dim_year")
dim_subgroup = spark.table("workspace.gold.dim_subgroup")

print(f"dim_school: {dim_school.count():,} rows")
print(f"dim_year: {dim_year.count():,} rows")
print(f"dim_subgroup: {dim_subgroup.count():,} rows")

# COMMAND ----------

# DBTITLE 1,Transform dropout_rate to fact grain
print("=== Transforming Dropout Rate ===")

# Load dropout silver (School level only)
dropout_silver = spark.table("workspace.silver.dropout_rate").filter("detail_level = 'School'")
print(f"Dropout silver (schools only): {dropout_silver.count():,} rows")

# Transform to fact grain: school x year x subgroup x grade_range
dropout_fact = dropout_silver.select(
    col('institution_key'),
    col('school_year'),
    col('subgroup_label').alias('subgroup'),
    col('grade_range'),
    col('dropout_count'),
    col('dropout_rate')
)

print(f"Dropout fact records: {dropout_fact.count():,} rows")
print("\nSample:")
dropout_fact.show(5, truncate=False)

# Show grade range distribution
print("\nGrade Range Distribution:")
dropout_fact.groupBy('grade_range').count().orderBy('grade_range').show()

# COMMAND ----------

# DBTITLE 1,Join to dimensions and build final fact table
print("=== Joining to Dimension Tables ===")

# Join to dim_school
fact_with_school = dropout_fact.join(
    dim_school,
    dropout_fact.institution_key == dim_school.institution_key,
    'left'
).select(
    dropout_fact['*'],
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
fact_dropout = fact_with_subgroup.select(
    col('school_key'),
    col('year_key'),
    col('subgroup_key'),
    col('grade_range'),
    col('dropout_count'),
    col('dropout_rate')
)

print(f"\nFinal fact_dropout: {fact_dropout.count():,} rows")
print("\nSample:")
fact_dropout.show(10)

# COMMAND ----------

# DBTITLE 1,Validate fact table
from pyspark.sql.functions import count as cnt, sum as spark_sum, avg, min as spark_min, max as spark_max

print("=== Fact Table Validation ===\n")

# 1. Check for NULL foreign keys
print("1. NULL Foreign Keys:")
null_school = fact_dropout.filter("school_key IS NULL").count()
null_year = fact_dropout.filter("year_key IS NULL").count()
null_subgroup = fact_dropout.filter("subgroup_key IS NULL").count()

print(f"   school_key NULL: {null_school:,}")
print(f"   year_key NULL: {null_year:,}")
print(f"   subgroup_key NULL: {null_subgroup:,}")

if null_school == 0 and null_year == 0 and null_subgroup == 0:
    print("   ✓ All foreign keys populated")
else:
    print("   ⚠️ Some foreign keys are NULL")

# 2. Dropout rate statistics
print("\n2. Dropout Rate Statistics:")
stats = fact_dropout.filter("dropout_rate IS NOT NULL").select(
    avg('dropout_rate').alias('avg_rate'),
    spark_min('dropout_rate').alias('min_rate'),
    spark_max('dropout_rate').alias('max_rate'),
    cnt('*').alias('non_null_count')
).collect()[0]

print(f"   Avg dropout rate: {stats['avg_rate']:.2f}%")
print(f"   Min rate: {stats['min_rate']:.2f}%")
print(f"   Max rate: {stats['max_rate']:.2f}%")
print(f"   Non-NULL records: {stats['non_null_count']:,}")

# 3. Dropout count statistics
print("\n3. Dropout Count Statistics:")
count_stats = fact_dropout.filter("dropout_count IS NOT NULL").select(
    spark_sum('dropout_count').alias('total_dropouts'),
    avg('dropout_count').alias('avg_per_record'),
    spark_max('dropout_count').alias('max_count')
).collect()[0]

print(f"   Total dropouts: {count_stats['total_dropouts']:,.0f}")
print(f"   Avg per record: {count_stats['avg_per_record']:.1f}")
print(f"   Max count: {count_stats['max_count']:,.0f}")

# 4. Grade range distribution
print("\n4. Grade Range Distribution:")
fact_dropout.groupBy('grade_range').agg(cnt('*').alias('record_count')).orderBy('grade_range').show()

# 5. Year coverage
print("\n5. Year Coverage:")
fact_dropout.groupBy('year_key').count().orderBy('year_key').show()

# COMMAND ----------

# DBTITLE 1,Write to gold
fact_dropout.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("workspace.gold.fact_dropout")
print(f"✓ workspace.gold.fact_dropout: {spark.table('workspace.gold.fact_dropout').count():,} rows")

# COMMAND ----------


