# Databricks notebook source
# DBTITLE 1,Fact Enrollment by Grade - Overview
# MAGIC %md
# MAGIC # Fact: Enrollment by Grade
# MAGIC
# MAGIC **Purpose:** Grade-level enrollment detail for trend analysis
# MAGIC
# MAGIC **Grain:** School × Year × Enrollment Period × Grade Level
# MAGIC
# MAGIC **Source Tables:**
# MAGIC * `workspace.silver.enrollment_by_grade` - Grade-level enrollment counts
# MAGIC
# MAGIC **Dimension Tables:**
# MAGIC * `workspace.gold.dim_school`
# MAGIC * `workspace.gold.dim_year`
# MAGIC
# MAGIC **Output:** `workspace.gold.fact_enrollment_by_grade`
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Facts (Measures)
# MAGIC * **enrollment_count** - Number of students enrolled at grade level
# MAGIC
# MAGIC ## Dimensions (Foreign Keys)
# MAGIC * **school_key** → dim_school
# MAGIC * **year_key** → dim_year
# MAGIC
# MAGIC ## Attributes (Degenerate Dimensions)
# MAGIC * **enrollment_period** - 'Fall', 'Spring', 'October FTE' timing
# MAGIC * **grade_level** - PK, KK, 1st, 2nd, ... 12th
# MAGIC
# MAGIC **Note:** Different grain than fact_enrollment (which uses enrollment_by_subgroup at school x year level)

# COMMAND ----------

# DBTITLE 1,Load dimensions
from pyspark.sql.functions import col

print("=== Loading Dimension Tables ===")

# Load dimensions
dim_school = spark.table("workspace.gold.dim_school")
dim_year = spark.table("workspace.gold.dim_year")

print(f"dim_school: {dim_school.count():,} rows")
print(f"dim_year: {dim_year.count():,} rows")

# COMMAND ----------

# DBTITLE 1,Transform enrollment_by_grade to fact grain
print("=== Transforming Enrollment by Grade ===")

# Load enrollment_by_grade silver (School level only)
enroll_grade_silver = spark.table("workspace.silver.enrollment_by_grade").filter("detail_level = 'School'")
print(f"Enrollment by grade silver (schools only): {enroll_grade_silver.count():,} rows")

# Transform to fact grain: school x year x period x grade
enroll_grade_fact = enroll_grade_silver.select(
    col('institution_key'),
    col('school_year'),
    col('enrollment_period'),
    col('grade_level'),
    col('enrollment_count')
)

print(f"Enrollment by grade fact records: {enroll_grade_fact.count():,} rows")
print("\nSample:")
enroll_grade_fact.show(5, truncate=False)

# Show distribution by enrollment period
print("\nEnrollment Period Distribution:")
enroll_grade_fact.groupBy('enrollment_period').count().orderBy('enrollment_period').show()

# Show distribution by grade level
print("\nGrade Level Distribution (top 10):")
enroll_grade_fact.groupBy('grade_level').count().orderBy(col('count').desc()).show(10)

# COMMAND ----------

# DBTITLE 1,Join to dimensions and build final fact table
print("=== Joining to Dimension Tables ===")

# Join to dim_school
fact_with_school = enroll_grade_fact.join(
    dim_school,
    enroll_grade_fact.institution_key == dim_school.institution_key,
    'left'
).select(
    enroll_grade_fact['*'],
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
fact_enrollment_by_grade = fact_with_year.select(
    col('school_key'),
    col('year_key'),
    col('enrollment_period'),
    col('grade_level'),
    col('enrollment_count')
)

print(f"\nFinal fact_enrollment_by_grade: {fact_enrollment_by_grade.count():,} rows")
print("\nSample:")
fact_enrollment_by_grade.show(10)

# COMMAND ----------

# DBTITLE 1,Validate fact table
from pyspark.sql.functions import count as cnt, sum as spark_sum, avg, min as spark_min, max as spark_max

print("=== Fact Table Validation ===\n")

# 1. Check for NULL foreign keys
print("1. NULL Foreign Keys:")
null_school = fact_enrollment_by_grade.filter("school_key IS NULL").count()
null_year = fact_enrollment_by_grade.filter("year_key IS NULL").count()

print(f"   school_key NULL: {null_school:,}")
print(f"   year_key NULL: {null_year:,}")

if null_school == 0 and null_year == 0:
    print("   ✓ All foreign keys populated")
else:
    print("   ⚠️ Some foreign keys are NULL")

# 2. Enrollment count statistics
print("\n2. Enrollment Count Statistics:")
stats = fact_enrollment_by_grade.filter("enrollment_count IS NOT NULL").select(
    spark_sum('enrollment_count').alias('total_enrollment'),
    avg('enrollment_count').alias('avg_per_record'),
    spark_min('enrollment_count').alias('min_count'),
    spark_max('enrollment_count').alias('max_count')
).collect()[0]

print(f"   Total enrollment: {stats['total_enrollment']:,.0f}")
print(f"   Avg per record: {stats['avg_per_record']:.1f}")
print(f"   Min count: {stats['min_count']:,.0f}")
print(f"   Max count: {stats['max_count']:,.0f}")

# 3. TFS suppression
print("\n3. TFS Suppression:")
total = fact_enrollment_by_grade.count()
tfs_count = fact_enrollment_by_grade.filter("enrollment_count IS NULL").count()
print(f"   Total records: {total:,}")
print(f"   TFS suppressed: {tfs_count:,} ({100*tfs_count/total:.1f}%)")

# 4. Distribution by enrollment period
print("\n4. Enrollment Period Distribution:")
fact_enrollment_by_grade.groupBy('enrollment_period').agg(
    cnt('*').alias('record_count'),
    spark_sum('enrollment_count').alias('total_enrollment')
).orderBy('enrollment_period').show()

# 5. Distribution by grade level (top 10)
print("\n5. Grade Level Distribution (top 10):")
fact_enrollment_by_grade.groupBy('grade_level').agg(
    cnt('*').alias('record_count'),
    spark_sum('enrollment_count').alias('total_enrollment')
).orderBy(col('total_enrollment').desc()).show(10)

# 6. Year coverage
print("\n6. Year Coverage:")
fact_enrollment_by_grade.groupBy('year_key').count().orderBy('year_key').show()

# COMMAND ----------

# DBTITLE 1,Write to gold
fact_enrollment_by_grade.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("workspace.gold.fact_enrollment_by_grade")
print(f"✓ workspace.gold.fact_enrollment_by_grade: {spark.table('workspace.gold.fact_enrollment_by_grade').count():,} rows")

# COMMAND ----------


