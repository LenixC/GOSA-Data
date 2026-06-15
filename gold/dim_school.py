# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# DBTITLE 1,Dim School - Overview
# MAGIC %md
# MAGIC # Dimension: School (Type 2 SCD)
# MAGIC
# MAGIC **Purpose:** Slowly changing dimension capturing school attributes over time
# MAGIC
# MAGIC **Grain:** One row per school per set of attribute values
# MAGIC
# MAGIC **Source Tables:**
# MAGIC * `workspace.silver.enrollment_by_grade` - institution details, grades served
# MAGIC * `workspace.silver.revenues` - institution details
# MAGIC * Additional silver tables as available
# MAGIC
# MAGIC **Output:** `workspace.gold.dim_school`
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Key Attributes
# MAGIC * **school_key** - Surrogate key (auto-generated)
# MAGIC * **institution_key** - Business key (district_code + institution_number)
# MAGIC * **institution_number** - School code
# MAGIC * **institution_name** - School name
# MAGIC * **district_code** - District code
# MAGIC * **district_name** - District name
# MAGIC * **detail_level** - State/District/School
# MAGIC * **grades_served** - Grade range
# MAGIC
# MAGIC ## SCD Type 2 Tracking
# MAGIC * **effective_date** - When this version became active
# MAGIC * **end_date** - When this version expired (NULL if current)
# MAGIC * **is_current** - Boolean flag for current record
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Data Quality Rules
# MAGIC * Deduplicate on business key + attributes
# MAGIC * Handle name changes over time
# MAGIC * Track school openings/closings via effective dates

# COMMAND ----------

# DBTITLE 1,Build dim_school
from pyspark.sql import functions as F
from pyspark.sql.functions import col, coalesce, lit, monotonically_increasing_id, row_number
from pyspark.sql.window import Window

print("=== Building Dim_School ===")

# Gather school attributes from enrollment_by_grade (most complete source)
enroll = spark.table("workspace.silver.enrollment_by_grade")

schools_enroll = enroll.select(
    col('institution_key'),
    col('institution_number'),
    col('institution_name'),
    col('district_code'),
    col('district_name'),
    col('detail_level'),
    col('grades_served'),
    col('school_year')
).distinct()

print(f"Schools from enrollment: {schools_enroll.count():,}")

# Gather from revenues (may have additional schools)
revenues = spark.table("workspace.silver.revenues")

schools_rev = revenues.select(
    col('institution_key'),
    col('institution_number'),
    col('institution_name'),
    col('district_code'),
    col('district_name'),
    col('detail_level'),
    col('grades_served'),
    col('school_year')
).distinct()

print(f"Schools from revenues: {schools_rev.count():,}")

# Union and deduplicate
schools_all = schools_enroll.union(schools_rev).distinct()

print(f"\nTotal unique school-year combinations: {schools_all.count():,}")

# For Type 2 SCD: track attribute changes over time
# Group by institution_key + attributes to find when attributes changed
school_groups = schools_all.groupBy(
    'institution_key',
    'institution_number',
    'institution_name',
    'district_code',
    'district_name',
    'detail_level',
    'grades_served'
).agg(
    F.min('school_year').alias('first_year'),
    F.max('school_year').alias('last_year')
)

print(f"Unique school attribute combinations: {school_groups.count():,}")

# For now, simplified approach: take most recent attributes per school
# (Full SCD Type 2 would track every name/grade change)
window_spec = Window.partitionBy('institution_key').orderBy(col('school_year').desc())

schools_current = schools_all.withColumn(
    'rn',
    row_number().over(window_spec)
).filter('rn = 1').drop('rn', 'school_year')

print(f"\nUnique schools (current attributes): {schools_current.count():,}")

# Add surrogate key and SCD fields
dim_school = schools_current.withColumn(
    'school_key',
    monotonically_increasing_id()
).withColumn(
    'is_current',
    lit(True)
).withColumn(
    'effective_date',
    lit('2010-11')  # Earliest year in dataset
).withColumn(
    'end_date',
    lit(None).cast('string')
).select(
    'school_key',
    'institution_key',
    'institution_number',
    'institution_name',
    'district_code',
    'district_name',
    'detail_level',
    'grades_served',
    'effective_date',
    'end_date',
    'is_current'
)

print(f"\nDim_School built: {dim_school.count():,} rows")
display(dim_school.orderBy('district_code', 'institution_number').limit(10))

# COMMAND ----------

# DBTITLE 1,Validate dim_school
from pyspark.sql.functions import count as cnt

print("=== Dim_School Validation ===\n")

# 1. Check for NULL business keys
print("1. NULL Business Keys:")
null_keys = dim_school.filter("institution_key IS NULL").count()
if null_keys == 0:
    print("   ✓ No NULL institution_keys")
else:
    print(f"   ⚠️  {null_keys} NULL institution_keys")

# 2. Check for duplicate business keys
print("\n2. Duplicate Business Keys:")
duplicates = dim_school.groupBy('institution_key').agg(
    cnt('*').alias('count')
).filter('count > 1').count()

if duplicates == 0:
    print("   ✓ No duplicate institution_keys")
else:
    print(f"   ⚠️  {duplicates} duplicate institution_keys")
    print("\n   Sample duplicates:")
    dim_school.groupBy('institution_key').agg(
        cnt('*').alias('count')
    ).filter('count > 1').orderBy(col('count').desc()).show(10)

# 3. Detail level distribution
print("\n3. Detail Level Distribution:")
dim_school.groupBy('detail_level').agg(
    cnt('*').alias('count')
).orderBy('detail_level').show()

# 4. District coverage
print("\n4. District Coverage:")
district_count = dim_school.filter("detail_level = 'District'").select('district_code').distinct().count()
school_count = dim_school.filter("detail_level = 'School'").count()
state_count = dim_school.filter("detail_level = 'State'").count()

print(f"   Districts: {district_count}")
print(f"   Schools: {school_count}")
print(f"   State records: {state_count}")

# 5. Check for schools without names
print("\n5. Data Completeness:")
no_name = dim_school.filter("institution_name IS NULL OR institution_name = ''").count()
no_grades = dim_school.filter("grades_served IS NULL OR grades_served = ''").count()

print(f"   Missing institution_name: {no_name}")
print(f"   Missing grades_served: {no_grades}")

# 6. Sample records by detail level
print("\n6. Sample Records:")
print("\n   State:")
dim_school.filter("detail_level = 'State'").select(
    'institution_key', 'district_name', 'institution_name', 'detail_level'
).show(3, truncate=False)

print("\n   District:")
dim_school.filter("detail_level = 'District'").select(
    'institution_key', 'district_code', 'district_name', 'institution_name'
).show(5, truncate=False)

print("\n   School:")
dim_school.filter("detail_level = 'School'").select(
    'institution_key', 'institution_number', 'institution_name', 'district_name', 'grades_served'
).show(5, truncate=False)

print("\n✓ Validation complete")

# COMMAND ----------

# DBTITLE 1,Write to gold
dim_school.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("workspace.gold.dim_school")
print(f"✓ workspace.gold.dim_school: {spark.table('workspace.gold.dim_school').count():,} rows")

# COMMAND ----------


