# Databricks notebook source
# DBTITLE 1,Fact Enrollment - Overview
# MAGIC %md
# MAGIC # Fact: Enrollment
# MAGIC
# MAGIC **Purpose:** Unified view of enrollment data by demographics and programs
# MAGIC
# MAGIC **Grain:** School × Year × Subgroup
# MAGIC
# MAGIC **Source Tables:**
# MAGIC * `workspace.silver.enrollment_by_subgroup` - Demographics and program participation
# MAGIC * `workspace.silver.enrollment_by_grade` - Grade-level enrollment
# MAGIC
# MAGIC **Dimension Tables:**
# MAGIC * `workspace.gold.dim_school`
# MAGIC * `workspace.gold.dim_year`
# MAGIC * `workspace.gold.dim_subgroup`
# MAGIC
# MAGIC **Output:** `workspace.gold.fact_enrollment`
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Facts (Measures)
# MAGIC * **total_enrollment** - Total enrolled students
# MAGIC * **pct_asian, pct_black, pct_hispanic, pct_white** - Racial demographics
# MAGIC * **pct_economically_disadvantaged** - Economic status
# MAGIC * **pct_students_with_disabilities** - Special education
# MAGIC * **pct_english_learner** - ELL/LEP students
# MAGIC * **pct_gifted** - Gifted program participation
# MAGIC
# MAGIC ## Dimensions (Foreign Keys)
# MAGIC * **school_key** → dim_school
# MAGIC * **year_key** → dim_year
# MAGIC * **subgroup_key** → dim_subgroup (for program/demographic drill-down)

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

# DBTITLE 1,Transform enrollment_by_subgroup to fact grain
print("=== Transforming Enrollment by Subgroup ===")

# Load enrollment silver (School level only for fact table)
enroll_silver = spark.table("workspace.silver.enrollment_by_subgroup").filter("detail_level = 'School'")
print(f"Enrollment silver (schools only): {enroll_silver.count():,} rows")

# Transform to fact grain: school x year
# Aggregate subgroup information at school level
enroll_fact = enroll_silver.select(
    col('institution_key'),
    col('school_year'),
    col('pct_asian'),
    col('pct_black'),
    col('pct_hispanic'),
    col('pct_white'),
    col('pct_multiracial'),
    col('pct_native'),
    col('pct_male'),
    col('pct_female'),
    col('pct_economically_disadvantaged'),
    col('pct_students_with_disabilities'),
    col('pct_english_learner'),
    col('pct_migrant'),
    col('pct_gifted'),
    col('pct_special_ed_k12'),
    col('pct_esol'),
    col('pct_remedial_6_8'),
    col('pct_remedial_9_12'),
    col('pct_eip_k_5'),
    col('pct_vocation_9_12'),
    col('pct_alt_programs')
)

print(f"Enrollment fact records: {enroll_fact.count():,} rows")
print("Sample:")
enroll_fact.show(5, truncate=False)

# COMMAND ----------

# DBTITLE 1,Join to dimensions and build final fact table
print("=== Joining to Dimension Tables ===")

# Join to dim_school
fact_with_school = enroll_fact.join(
    dim_school,
    enroll_fact.institution_key == dim_school.institution_key,
    'left'
).select(
    enroll_fact['*'],
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

# Build final fact table with surrogate keys and measures
fact_enrollment = fact_with_year.select(
    col('school_key'),
    col('year_key'),
    col('pct_asian'),
    col('pct_black'),
    col('pct_hispanic'),
    col('pct_white'),
    col('pct_multiracial'),
    col('pct_native'),
    col('pct_male'),
    col('pct_female'),
    col('pct_economically_disadvantaged'),
    col('pct_students_with_disabilities'),
    col('pct_english_learner'),
    col('pct_migrant'),
    col('pct_gifted'),
    col('pct_special_ed_k12'),
    col('pct_esol'),
    col('pct_remedial_6_8'),
    col('pct_remedial_9_12'),
    col('pct_eip_k_5'),
    col('pct_vocation_9_12'),
    col('pct_alt_programs')
)

print(f"\nFinal fact_enrollment: {fact_enrollment.count():,} rows")
print("\nSample:")
fact_enrollment.show(10)

# COMMAND ----------

# DBTITLE 1,Validate fact table
from pyspark.sql.functions import count as cnt, sum as spark_sum, avg, min as spark_min, max as spark_max

print("=== Fact Table Validation ===\n")

# 1. Check for NULL foreign keys
print("1. NULL Foreign Keys:")
null_school = fact_enrollment.filter("school_key IS NULL").count()
null_year = fact_enrollment.filter("year_key IS NULL").count()

print(f"   school_key NULL: {null_school:,}")
print(f"   year_key NULL: {null_year:,}")

if null_school == 0 and null_year == 0:
    print("   ✓ All foreign keys populated")
else:
    print("   ⚠️ Some foreign keys are NULL")

# 2. Record count
print("\n2. Record Count:")
print(f"   Total records: {fact_enrollment.count():,}")

# 3. Check year coverage
print("\n3. Year Coverage:")
fact_enrollment.groupBy('year_key').count().orderBy('year_key').show()

# 4. Check percentage ranges
print("\n4. Percentage Validation (should be 0-100):")
for pct_col in ['pct_economically_disadvantaged', 'pct_english_learner', 'pct_gifted']:
    min_val = fact_enrollment.select(spark_min(pct_col)).collect()[0][0]
    max_val = fact_enrollment.select(spark_max(pct_col)).collect()[0][0]
    if min_val is not None and max_val is not None:
        print(f"   {pct_col}: [{min_val:.1f}, {max_val:.1f}]")
        if min_val >= 0 and max_val <= 100:
            print(f"      ✓ Valid range")
        else:
            print(f"      ⚠️ Out of range")

# COMMAND ----------

# DBTITLE 1,Write to gold
fact_enrollment.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("workspace.gold.fact_enrollment")
print(f"✓ workspace.gold.fact_enrollment: {spark.table('workspace.gold.fact_enrollment').count():,} rows")

# COMMAND ----------


