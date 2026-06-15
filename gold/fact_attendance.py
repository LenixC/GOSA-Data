# Databricks notebook source
# DBTITLE 1,Fact Attendance - Overview
# MAGIC %md
# MAGIC # Fact: Attendance
# MAGIC
# MAGIC **Purpose:** Attendance and chronic absenteeism metrics by subgroup
# MAGIC
# MAGIC **Grain:** School × Year
# MAGIC
# MAGIC **Source Tables:**
# MAGIC * `workspace.silver.attendance` - Attendance data by 15 subgroups (wide format)
# MAGIC
# MAGIC **Dimension Tables:**
# MAGIC * `workspace.gold.dim_school`
# MAGIC * `workspace.gold.dim_year`
# MAGIC
# MAGIC **Output:** `workspace.gold.fact_attendance`
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Facts (Measures)
# MAGIC * **15 subgroups** (ALL, racial/ethnic groups, gender, SWD, ED, LEP, Migrant)
# MAGIC * For each subgroup:
# MAGIC   * student_count
# MAGIC   * chronic_absent_perc (% with >15% absences)
# MAGIC   * five_or_fewer_pct (% with ≤5% absences)
# MAGIC   * six_to_fifteen_pct (% with 6-15% absences)
# MAGIC   * over_15_pct (% with >15% absences)
# MAGIC
# MAGIC ## Dimensions (Foreign Keys)
# MAGIC * **school_key** → dim_school
# MAGIC * **year_key** → dim_year
# MAGIC
# MAGIC **Note:** Kept in wide format (not unpivoted) to match silver layer structure

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

# DBTITLE 1,Transform attendance to fact grain
print("=== Transforming Attendance ===")

# Load attendance silver (School level only)
att_silver = spark.table("workspace.silver.attendance").filter("detail_level = 'School'")
print(f"Attendance silver (schools only): {att_silver.count():,} rows")

# Transform to fact grain: school x year (keep wide format with all subgroups)
att_fact = att_silver.select(
    col('institution_key'),
    col('school_year'),
    # All students
    col('student_count_all'),
    col('five_or_fewer_percent_all').alias('five_or_fewer_pct_all'),
    col('six_to_fifteen_percent_all').alias('six_to_fifteen_pct_all'),
    col('over_15_percent_all').alias('over_15_pct_all'),
    col('chronic_absent_perc_all'),
    # Demographics
    col('student_count_indian'), col('chronic_absent_perc_indian'),
    col('student_count_asian'), col('chronic_absent_perc_asian'),
    col('student_count_black'), col('chronic_absent_perc_black'),
    col('student_count_white'), col('chronic_absent_perc_white'),
    col('student_count_hispani').alias('student_count_hispanic'), col('chronic_absent_perc_hispani').alias('chronic_absent_perc_hispanic'),
    col('student_count_multi'), col('chronic_absent_perc_multi'),
    # Gender
    col('student_count_female'), col('chronic_absent_perc_female'),
    col('student_count_male'), col('chronic_absent_perc_male'),
    # Special populations
    col('student_count_swd'), col('chronic_absent_perc_swd'),
    col('student_count_not_swd'), col('chronic_absent_perc_not_swd'),
    col('student_count_ed'), col('chronic_absent_perc_ed'),
    col('student_count_not_ed'), col('chronic_absent_perc_not_ed'),
    col('student_count_lep'), col('chronic_absent_perc_lep'),
    col('student_count_migrant'), col('chronic_absent_perc_migrant')
)

print(f"Attendance fact records: {att_fact.count():,} rows")
print(f"Columns: {len(att_fact.columns)}")
print("\nSample (ALL students metrics):")
att_fact.select('institution_key', 'school_year', 'student_count_all', 'chronic_absent_perc_all').show(5)

# COMMAND ----------

# DBTITLE 1,Join to dimensions and build final fact table
print("=== Joining to Dimension Tables ===")

# Join to dim_school
fact_with_school = att_fact.join(
    dim_school,
    att_fact.institution_key == dim_school.institution_key,
    'left'
).select(
    att_fact['*'],
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

# Build final fact table (drop natural keys, keep surrogate keys + measures)
fact_attendance = fact_with_year.drop('institution_key', 'school_year')

print(f"\nFinal fact_attendance: {fact_attendance.count():,} rows")
print(f"Columns: {len(fact_attendance.columns)}")
print("\nSample:")
fact_attendance.select('school_key', 'year_key', 'student_count_all', 'chronic_absent_perc_all').show(10)

# COMMAND ----------

# DBTITLE 1,Validate fact table
from pyspark.sql.functions import count as cnt, avg, min as spark_min, max as spark_max

print("=== Fact Table Validation ===\n")

# 1. Check for NULL foreign keys
print("1. NULL Foreign Keys:")
null_school = fact_attendance.filter("school_key IS NULL").count()
null_year = fact_attendance.filter("year_key IS NULL").count()

print(f"   school_key NULL: {null_school:,}")
print(f"   year_key NULL: {null_year:,}")

if null_school == 0 and null_year == 0:
    print("   ✓ All foreign keys populated")
else:
    print("   ⚠️ Some foreign keys are NULL")

# 2. Chronic absenteeism statistics (ALL students)
print("\n2. Chronic Absenteeism (ALL students):")
stats = fact_attendance.filter("chronic_absent_perc_all IS NOT NULL").select(
    avg('chronic_absent_perc_all').alias('avg_rate'),
    spark_min('chronic_absent_perc_all').alias('min_rate'),
    spark_max('chronic_absent_perc_all').alias('max_rate'),
    cnt('*').alias('non_null_count')
).collect()[0]

print(f"   Avg chronic absenteeism: {stats['avg_rate']:.1f}%")
print(f"   Min: {stats['min_rate']:.1f}%")
print(f"   Max: {stats['max_rate']:.1f}%")
print(f"   Non-NULL records: {stats['non_null_count']:,}")

# 3. Check percentage ranges (should be 0-100)
print("\n3. Percentage Range Validation:")
for perc_col in ['chronic_absent_perc_all', 'five_or_fewer_pct_all', 'over_15_pct_all']:
    min_val = fact_attendance.select(spark_min(perc_col)).collect()[0][0]
    max_val = fact_attendance.select(spark_max(perc_col)).collect()[0][0]
    if min_val is not None and max_val is not None:
        print(f"   {perc_col}: [{min_val:.1f}, {max_val:.1f}]")
        if min_val >= 0 and max_val <= 100:
            print(f"      ✓ Valid range")
        else:
            print(f"      ⚠️ Out of range")

# 4. Data suppression (TFS)
print("\n4. TFS Suppression Analysis:")
total = fact_attendance.count()
tfs_all = fact_attendance.filter("student_count_all IS NULL").count()
print(f"   Total records: {total:,}")
print(f"   TFS suppressed (student_count_all): {tfs_all:,} ({100*tfs_all/total:.1f}%)")

# 5. Year coverage
print("\n5. Year Coverage:")
fact_attendance.groupBy('year_key').count().orderBy('year_key').show()

# COMMAND ----------

# DBTITLE 1,Write to gold
fact_attendance.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("workspace.gold.fact_attendance")
print(f"✓ workspace.gold.fact_attendance: {spark.table('workspace.gold.fact_attendance').count():,} rows")

# COMMAND ----------


