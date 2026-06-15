# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# DBTITLE 1,Dim Assessment - Overview
# MAGIC %md
# MAGIC # Dimension: Assessment
# MAGIC
# MAGIC **Purpose:** Catalog of all assessments/tests in the system
# MAGIC
# MAGIC **Grain:** One row per unique assessment + subject + grade combination
# MAGIC
# MAGIC **Sources:** EOG, EOC, SAT, ACT test tables
# MAGIC
# MAGIC **Output:** `workspace.gold.dim_assessment`
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Attributes
# MAGIC * **assessment_key** - Surrogate key
# MAGIC * **assessment_name** - EOG, EOC, SAT, ACT
# MAGIC * **subject** - Subject/test component (Math, ELA, Science, etc.)
# MAGIC * **grade_level** - Grade level (or NULL for SAT/ACT)
# MAGIC * **assessment_format** - old_sat vs new_sat, etc.
# MAGIC * **score_type** - highest vs recent (for SAT)
# MAGIC * **assessment_category** - State (EOG/EOC) vs College Readiness (SAT/ACT)
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Assessment Types
# MAGIC * **EOG** - End of Grade (3-8, state-mandated)
# MAGIC * **EOC** - End of Course (HS, state-mandated)
# MAGIC * **SAT** - College admission test
# MAGIC * **ACT** - College admission test

# COMMAND ----------

# DBTITLE 1,Build dim_assessment
from pyspark.sql.functions import col, lit, monotonically_increasing_id

print("=== Building Dim_Assessment ===")

# EOG assessments
eog = spark.table("workspace.silver.eog")
eog_assessments = eog.select(
    lit('EOG').alias('assessment_name'),
    col('test_component').alias('subject'),
    col('grade_level'),
    lit(None).cast('string').alias('assessment_format'),
    lit(None).cast('string').alias('score_type'),
    lit('State').alias('assessment_category')
).distinct()

print(f"EOG assessments: {eog_assessments.count()}")

# EOC assessments
eoc = spark.table("workspace.silver.eoc")
eoc_assessments = eoc.select(
    lit('EOC').alias('assessment_name'),
    col('test_component').alias('subject'),
    lit(None).cast('string').alias('grade_level'),
    lit(None).cast('string').alias('assessment_format'),
    lit(None).cast('string').alias('score_type'),
    lit('State').alias('assessment_category')
).distinct()

print(f"EOC assessments: {eoc_assessments.count()}")

# SAT assessments
sat = spark.table("workspace.silver.sat")
sat_assessments = sat.select(
    lit('SAT').alias('assessment_name'),
    col('test_component').alias('subject'),
    lit(None).cast('string').alias('grade_level'),
    col('sat_format').alias('assessment_format'),
    col('highest_recent_indicator').alias('score_type'),
    lit('College Readiness').alias('assessment_category')
).distinct()

print(f"SAT assessments: {sat_assessments.count()}")

# ACT assessments
act = spark.table("workspace.silver.act")
act_assessments = act.select(
    lit('ACT').alias('assessment_name'),
    col('test_component').alias('subject'),
    lit(None).cast('string').alias('grade_level'),
    lit(None).cast('string').alias('assessment_format'),
    col('highest_recent_indicator').alias('score_type'),
    lit('College Readiness').alias('assessment_category')
).distinct()

print(f"ACT assessments: {act_assessments.count()}")

# Union all
all_assessments = eog_assessments.union(eoc_assessments).union(sat_assessments).union(act_assessments)

print(f"\nTotal unique assessments: {all_assessments.count()}")

# Add surrogate key
dim_assessment = all_assessments.withColumn(
    'assessment_key',
    monotonically_increasing_id()
).select(
    'assessment_key',
    'assessment_name',
    'subject',
    'grade_level',
    'assessment_format',
    'score_type',
    'assessment_category'
).orderBy('assessment_category', 'assessment_name', 'grade_level', 'subject')

print(f"\nDim_Assessment built: {dim_assessment.count()} rows\n")

# Show breakdown
print("Assessment breakdown by name:")
dim_assessment.groupBy('assessment_name').count().orderBy('assessment_name').show()

print("\nSample records:")
display(dim_assessment.limit(20))

# COMMAND ----------

# DBTITLE 1,Write to gold
dim_assessment.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("workspace.gold.dim_assessment")
print(f"✓ workspace.gold.dim_assessment: {spark.table('workspace.gold.dim_assessment').count():,} rows")

# COMMAND ----------


