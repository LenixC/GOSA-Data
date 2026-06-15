# Databricks notebook source
# DBTITLE 1,Fact Staffing - Overview
# MAGIC %md
# MAGIC # Fact: Staffing
# MAGIC
# MAGIC **Purpose:** Certified personnel metrics for workforce analysis
# MAGIC
# MAGIC **Grain:** School × Year × Employee Type × Data Category × Data Subcategory
# MAGIC
# MAGIC **Source Tables:**
# MAGIC * `workspace.silver.certified_personnel` - Teacher and staff counts
# MAGIC
# MAGIC **Dimension Tables:**
# MAGIC * `workspace.gold.dim_school`
# MAGIC * `workspace.gold.dim_year`
# MAGIC
# MAGIC **Output:** `workspace.gold.fact_staffing`
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Facts (Measures)
# MAGIC * **staff_count** - Number of certified personnel
# MAGIC
# MAGIC ## Dimensions (Foreign Keys)
# MAGIC * **school_key** → dim_school
# MAGIC * **year_key** → dim_year
# MAGIC
# MAGIC ## Attributes (Degenerate Dimensions)
# MAGIC * **employee_type** - Teachers, Administrators, Support Staff, etc.
# MAGIC * **data_category** - Certificate Level, Gender, Personnel, etc.
# MAGIC * **data_sub_category** - Bachelor's, Master's, Doctoral, Male, Female, Full-time, Part-time, etc.

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

# DBTITLE 1,Transform certified_personnel to fact grain
print("=== Transforming Certified Personnel ===")

# Load certified personnel silver
staff_silver = spark.table("workspace.silver.certified_personnel")
print(f"Certified personnel silver: {staff_silver.count():,} rows")

# Transform to fact grain: school x year x employee_type x category x subcategory
staff_fact = staff_silver.select(
    col('institution_key'),
    col('school_year'),
    col('employee_type'),
    col('data_category'),
    col('data_sub_category'),
    col('staff_count')
)

print(f"Staff fact records: {staff_fact.count():,} rows")
print("\nSample:")
staff_fact.show(5, truncate=False)

# Show distribution by employee type
print("\nEmployee Type Distribution:")
staff_fact.groupBy('employee_type').count().orderBy(col('count').desc()).show()

# Show distribution by data category
print("\nData Category Distribution:")
staff_fact.groupBy('data_category').count().orderBy(col('count').desc()).show()

# COMMAND ----------

# DBTITLE 1,Join to dimensions and build final fact table
print("=== Joining to Dimension Tables ===")

# Join to dim_school
fact_with_school = staff_fact.join(
    dim_school,
    staff_fact.institution_key == dim_school.institution_key,
    'left'
).select(
    staff_fact['*'],
    dim_school.school_key
)

print(f"After school join: {fact_with_school.count():,} rows")

# Check for unmatched schools
unmatched_schools = fact_with_school.filter("school_key IS NULL").select('institution_key').distinct().count()
if unmatched_schools > 0:
    print(f"  ⚠️  {unmatched_schools} institution_keys not found in dim_school")
    print("\n  Sample unmatched institution_keys:")
    fact_with_school.filter("school_key IS NULL").select('institution_key').distinct().show(5, truncate=False)
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
fact_staffing = fact_with_year.select(
    col('school_key'),
    col('year_key'),
    col('employee_type'),
    col('data_category'),
    col('data_sub_category'),
    col('staff_count')
)

print(f"\nFinal fact_staffing: {fact_staffing.count():,} rows")
print("\nSample:")
fact_staffing.show(10, truncate=False)

# COMMAND ----------

# DBTITLE 1,Validate fact table
from pyspark.sql.functions import count as cnt, sum as spark_sum, avg, min as spark_min, max as spark_max

print("=== Fact Table Validation ===\n")

# 1. Check for NULL foreign keys
print("1. NULL Foreign Keys:")
null_school = fact_staffing.filter("school_key IS NULL").count()
null_year = fact_staffing.filter("year_key IS NULL").count()

print(f"   school_key NULL: {null_school:,}")
print(f"   year_key NULL: {null_year:,}")

if null_school == 0 and null_year == 0:
    print("   ✓ All foreign keys populated")
else:
    print("   ⚠️ Some foreign keys are NULL")

# 2. Check staff count statistics
print("\n2. Staff Count Statistics:")
stats = fact_staffing.filter("staff_count IS NOT NULL").select(
    spark_sum('staff_count').alias('total_staff'),
    avg('staff_count').alias('avg_per_record'),
    spark_min('staff_count').alias('min_count'),
    spark_max('staff_count').alias('max_count')
).collect()[0]

print(f"   Total staff (sum of all records): {stats['total_staff']:,.0f}")
print(f"   Avg per record: {stats['avg_per_record']:.1f}")
print(f"   Min count: {stats['min_count']:.0f}")
print(f"   Max count: {stats['max_count']:.0f}")

# 3. Check for zero/negative counts
print("\n3. Zero/Negative Staff Counts:")
zero_count = fact_staffing.filter("staff_count = 0").count()
neg_count = fact_staffing.filter("staff_count < 0").count()
print(f"   Zero counts: {zero_count:,} ({zero_count/fact_staffing.count()*100:.1f}%)")
print(f"   Negative counts: {neg_count:,}")
if zero_count > 0:
    print("   ℹ️ Zero counts are valid (school may not have staff in that category)")

# 4. Check employee type distribution
print("\n4. Employee Type Distribution:")
fact_staffing.groupBy('employee_type').agg(
    cnt('*').alias('record_count'),
    spark_sum('staff_count').alias('total_staff')
).orderBy(col('total_staff').desc()).show(truncate=False)

# 5. Check data category distribution
print("\n5. Data Category Distribution:")
fact_staffing.groupBy('data_category').agg(
    cnt('*').alias('record_count')
).orderBy(col('record_count').desc()).show(truncate=False)

# 6. Year coverage
print("\n6. Year Coverage:")
fact_staffing.groupBy('year_key').count().orderBy('year_key').show()

# COMMAND ----------

# DBTITLE 1,Write to gold
fact_staffing.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("workspace.gold.fact_staffing")
print(f"✓ workspace.gold.fact_staffing: {spark.table('workspace.gold.fact_staffing').count():,} rows")

# COMMAND ----------


