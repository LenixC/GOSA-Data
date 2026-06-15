# Databricks notebook source
# DBTITLE 1,Fact School Performance - Overview
# MAGIC %md
# MAGIC # Fact: School Performance
# MAGIC
# MAGIC **Purpose:** Unified view of all test performance data across assessments
# MAGIC
# MAGIC **Grain:** School × Year × Subgroup × Assessment
# MAGIC
# MAGIC **Source Tables:**
# MAGIC * `workspace.silver.eog` - Elementary/middle grades
# MAGIC * `workspace.silver.eoc` - High school end-of-course
# MAGIC * `workspace.silver.sat` - SAT college admission
# MAGIC * `workspace.silver.act` - ACT college admission
# MAGIC
# MAGIC **Dimension Tables:**
# MAGIC * `workspace.gold.dim_school`
# MAGIC * `workspace.gold.dim_year`
# MAGIC * `workspace.gold.dim_subgroup`
# MAGIC * `workspace.gold.dim_assessment`
# MAGIC
# MAGIC **Output:** `workspace.gold.fact_school_performance`
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Facts (Measures)
# MAGIC * **num_tested** - Count of students tested
# MAGIC * **avg_score** - Average test score
# MAGIC * **proficiency_rate** - Percent proficient (for state tests)
# MAGIC * **participation_rate** - Percent participating
# MAGIC
# MAGIC ## Dimensions (Foreign Keys)
# MAGIC * **school_key** → dim_school
# MAGIC * **year_key** → dim_year
# MAGIC * **subgroup_key** → dim_subgroup
# MAGIC * **assessment_key** → dim_assessment
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Design Notes
# MAGIC * **Score normalization:** Different assessments have different scales
# MAGIC * **NULL handling:** Not all subgroups tested in all assessments
# MAGIC * **Grain consistency:** One row per unique school/year/subgroup/assessment combination

# COMMAND ----------

# DBTITLE 1,Load dimensions
from pyspark.sql.functions import col, lit

print("=== Loading Dimension Tables ===")

# Load dimensions
dim_school = spark.table("workspace.gold.dim_school")
dim_year = spark.table("workspace.gold.dim_year")
dim_subgroup = spark.table("workspace.gold.dim_subgroup")
dim_assessment = spark.table("workspace.gold.dim_assessment")

print(f"dim_school: {dim_school.count():,} rows")
print(f"dim_year: {dim_year.count():,} rows")
print(f"dim_subgroup: {dim_subgroup.count():,} rows")
print(f"dim_assessment: {dim_assessment.count():,} rows")

# COMMAND ----------

# DBTITLE 1,Transform EOG to fact grain
print("=== Transforming EOG ===")

# Load EOG silver
eog_silver = spark.table("workspace.silver.eog")
print(f"EOG silver: {eog_silver.count():,} rows")

# Transform to fact grain: school x year x subgroup x assessment
eog_fact = eog_silver.select(
    col('institution_key'),
    col('school_year'),
    col('subgroup_name').alias('subgroup'),
    col('test_component').alias('subject'),
    col('grade_level').alias('grade'),
    col('num_tested').alias('num_tested'),
    (col('proficient_pct') + col('distinguished_pct')).alias('proficiency_rate'),
    (col('proficient_pct') + col('distinguished_pct')).alias('avg_score'),  # Use proficiency as proxy for score
    lit(None).cast('double').alias('participation_rate'),
    lit('EOG').alias('assessment_name'),
    lit(None).cast('string').alias('assessment_format'),
    lit(None).cast('string').alias('score_type')
)

print(f"EOG fact records: {eog_fact.count():,} rows")
print("Sample:")
eog_fact.show(5, truncate=False)

# COMMAND ----------

# DBTITLE 1,Transform EOC to fact grain
print("=== Transforming EOC ===")

# Load EOC silver
eoc_silver = spark.table("workspace.silver.eoc")
print(f"EOC silver: {eoc_silver.count():,} rows")

# Transform to fact grain
eoc_fact = eoc_silver.select(
    col('institution_key'),
    col('school_year'),
    col('subgroup_name').alias('subgroup'),
    col('test_component').alias('subject'),
    lit(None).cast('string').alias('grade'),
    col('num_tested').alias('num_tested'),
    (col('proficient_pct') + col('distinguished_pct')).alias('proficiency_rate'),
    (col('proficient_pct') + col('distinguished_pct')).alias('avg_score'),  # Use proficiency as proxy for score
    lit(None).cast('double').alias('participation_rate'),
    lit('EOC').alias('assessment_name'),
    lit(None).cast('string').alias('assessment_format'),
    lit(None).cast('string').alias('score_type')
)

print(f"EOC fact records: {eoc_fact.count():,} rows")

# COMMAND ----------

# DBTITLE 1,Transform SAT to fact grain
print("=== Transforming SAT ===")

# Load SAT silver
sat_silver = spark.table("workspace.silver.sat")
print(f"SAT silver: {sat_silver.count():,} rows")

# Transform to fact grain
sat_fact = sat_silver.select(
    col('institution_key'),
    col('school_year'),
    col('subgroup'),
    col('test_component').alias('subject'),
    lit(None).cast('string').alias('grade'),
    col('institution_num_tested').alias('num_tested'),
    col('institution_avg_score').alias('avg_score'),
    lit(None).cast('double').alias('proficiency_rate'),
    lit(None).cast('double').alias('participation_rate'),
    lit('SAT').alias('assessment_name'),
    col('sat_format').alias('assessment_format'),
    col('highest_recent_indicator').alias('score_type')
)

print(f"SAT fact records: {sat_fact.count():,} rows")

# COMMAND ----------

# DBTITLE 1,Transform ACT to fact grain
print("=== Transforming ACT ===")

# Load ACT silver
act_silver = spark.table("workspace.silver.act")
print(f"ACT silver: {act_silver.count():,} rows")

# Transform to fact grain
act_fact = act_silver.select(
    col('institution_key'),
    col('school_year'),
    col('subgroup'),
    col('test_component').alias('subject'),
    lit(None).cast('string').alias('grade'),
    col('institution_num_tested').alias('num_tested'),
    col('institution_avg_score').alias('avg_score'),
    lit(None).cast('double').alias('proficiency_rate'),
    lit(None).cast('double').alias('participation_rate'),
    lit('ACT').alias('assessment_name'),
    lit(None).cast('string').alias('assessment_format'),
    col('highest_recent_indicator').alias('score_type')
)

print(f"ACT fact records: {act_fact.count():,} rows")

# COMMAND ----------

# DBTITLE 1,Union all assessments
print("=== Unioning All Assessments ===")

# Union all fact records
all_facts = eog_fact.union(eoc_fact).union(sat_fact).union(act_fact)

print(f"\nTotal fact records before joins: {all_facts.count():,} rows")

# Show distribution by assessment
print("\nFact distribution by assessment:")
all_facts.groupBy('assessment_name').count().orderBy('assessment_name').show()

print("\nSample records:")
all_facts.show(10, truncate=False)

# COMMAND ----------

# DBTITLE 1,Join to dimensions and build final fact table
from pyspark.sql.functions import coalesce

print("=== Joining to Dimension Tables ===")

# Join to dim_school
fact_with_school = all_facts.join(
    dim_school,
    all_facts.institution_key == dim_school.institution_key,
    'left'
).select(
    all_facts['*'],
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

# Join to dim_assessment (more complex - need to match on multiple fields)
fact_with_assessment = fact_with_subgroup.join(
    dim_assessment,
    (fact_with_subgroup.assessment_name == dim_assessment.assessment_name) &
    (fact_with_subgroup.subject == dim_assessment.subject) &
    (coalesce(fact_with_subgroup.grade, lit('')) == coalesce(dim_assessment.grade_level, lit(''))) &
    (coalesce(fact_with_subgroup.assessment_format, lit('')) == coalesce(dim_assessment.assessment_format, lit(''))) &
    (coalesce(fact_with_subgroup.score_type, lit('')) == coalesce(dim_assessment.score_type, lit(''))),
    'left'
).select(
    fact_with_subgroup['*'],
    dim_assessment.assessment_key
)

print(f"After assessment join: {fact_with_assessment.count():,} rows")

# Check for unmatched assessments
unmatched_assessments = fact_with_assessment.filter("assessment_key IS NULL").count()
if unmatched_assessments > 0:
    print(f"  ⚠️  {unmatched_assessments} records with no matching assessment")
    print("\n  Sample unmatched:")
    fact_with_assessment.filter("assessment_key IS NULL").select(
        'assessment_name', 'subject', 'grade', 'assessment_format', 'score_type'
    ).distinct().show(10, truncate=False)
else:
    print("  ✓ All assessments matched")

# Build final fact table with only needed columns
fact_school_performance = fact_with_assessment.select(
    col('school_key'),
    col('year_key'),
    col('subgroup_key'),
    col('assessment_key'),
    col('num_tested'),
    col('avg_score'),
    col('proficiency_rate'),
    col('participation_rate')
)

print(f"\n=== Final Fact Table ===")
print(f"Total rows: {fact_school_performance.count():,}")
print("\nSample:")
fact_school_performance.show(10)

# COMMAND ----------

# DBTITLE 1,Validate fact table
from pyspark.sql.functions import count as cnt, sum as spark_sum, avg, min as spark_min, max as spark_max

print("=== Fact Table Validation ===\n")

# 1. Check for NULL foreign keys
print("1. NULL Foreign Keys:")
null_school = fact_school_performance.filter("school_key IS NULL").count()
null_year = fact_school_performance.filter("year_key IS NULL").count()
null_subgroup = fact_school_performance.filter("subgroup_key IS NULL").count()
null_assessment = fact_school_performance.filter("assessment_key IS NULL").count()

print(f"   school_key NULL: {null_school:,}")
print(f"   year_key NULL: {null_year:,}")
print(f"   subgroup_key NULL: {null_subgroup:,}")
print(f"   assessment_key NULL: {null_assessment:,}")

if null_school + null_year + null_subgroup + null_assessment == 0:
    print("   ✓ All foreign keys populated")

# 2. Grain validation - check for duplicates
print("\n2. Grain Validation (School x Year x Subgroup x Assessment):")
duplicates = fact_school_performance.groupBy(
    'school_key', 'year_key', 'subgroup_key', 'assessment_key'
).agg(cnt('*').alias('count')).filter('count > 1').count()

if duplicates == 0:
    print("   ✓ No duplicate grain combinations")
else:
    print(f"   ⚠️  {duplicates} duplicate grain combinations")

# 3. Measure validation
print("\n3. Measure Statistics:")
stats = fact_school_performance.select(
    cnt('*').alias('total_records'),
    cnt('num_tested').alias('has_num_tested'),
    cnt('avg_score').alias('has_avg_score'),
    cnt('proficiency_rate').alias('has_proficiency_rate'),
    spark_sum('num_tested').alias('total_students_tested'),
    avg('avg_score').alias('avg_of_avg_scores'),
    spark_min('avg_score').alias('min_score'),
    spark_max('avg_score').alias('max_score')
).collect()[0]

print(f"   Total records: {stats['total_records']:,}")
print(f"   Records with num_tested: {stats['has_num_tested']:,}")
print(f"   Records with avg_score: {stats['has_avg_score']:,}")
print(f"   Records with proficiency_rate: {stats['has_proficiency_rate']:,}")
print(f"   Total students tested: {stats['total_students_tested']:,.0f}")
print(f"   Average score (overall): {stats['avg_of_avg_scores']:.2f}")
print(f"   Score range: {stats['min_score']:.2f} to {stats['max_score']:.2f}")

# 4. Distribution by dimension
print("\n4. Record Distribution:")
print("\n   By year:")
fact_school_performance.join(
    dim_year, 'year_key'
).groupBy('school_year').count().orderBy('school_year').show(20, truncate=False)

print("\n   By assessment (top 10):")
fact_school_performance.join(
    dim_assessment, 'assessment_key'
).groupBy('assessment_name', 'subject').count().orderBy(col('count').desc()).show(10, truncate=False)

print("\n✓ Validation complete")

# COMMAND ----------

# DBTITLE 1,Write to gold
fact_school_performance.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("workspace.gold.fact_school_performance")
print(f"✓ workspace.gold.fact_school_performance: {spark.table('workspace.gold.fact_school_performance').count():,} rows")

# COMMAND ----------


