# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# DBTITLE 1,Dim Subgroup - Overview
# MAGIC %md
# MAGIC # Dimension: Subgroup
# MAGIC
# MAGIC **Purpose:** Student demographic subgroups for disaggregation
# MAGIC
# MAGIC **Grain:** One row per unique subgroup value
# MAGIC
# MAGIC **Sources:** Enrollment, test scores, attendance (all use subgroup field)
# MAGIC
# MAGIC **Output:** `workspace.gold.dim_subgroup`
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Attributes
# MAGIC * **subgroup_key** - Surrogate key
# MAGIC * **subgroup** - Business key (the actual subgroup name)
# MAGIC * **subgroup_category** - Category classification
# MAGIC   * `All Students`
# MAGIC   * `Race/Ethnicity`
# MAGIC   * `Gender`
# MAGIC   * `Special Populations` (ELL, SWD, Migrant, etc.)
# MAGIC   * `Economic Status`
# MAGIC * **is_total** - Boolean flag for "All Students" record
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Subgroup Categorization
# MAGIC Derived from common patterns in GOSA data:
# MAGIC * **All Students** → Total/Aggregate
# MAGIC * **Race** → Asian, Black, Hispanic, White, Multi-Racial, etc.
# MAGIC * **Gender** → Male, Female
# MAGIC * **Special Pop** → English Learners, Students with Disabilities, Migrant, Homeless
# MAGIC * **Economic** → Economically Disadvantaged, Foster Care

# COMMAND ----------

# DBTITLE 1,Build dim_subgroup
from pyspark.sql.functions import col, when, lit, monotonically_increasing_id

print("=== Building Dim_Subgroup ===")

# Gather all unique subgroups from test score tables (enrollment_by_subgroup is pivoted)
eog = spark.table("workspace.silver.eog")
all_subgroups = eog.select(col('subgroup_name').alias('subgroup')).distinct()

print(f"Unique subgroups from EOG: {all_subgroups.count()}")
print(f"\nTotal unique subgroups: {all_subgroups.count()}")

# Categorize subgroups based on common patterns
dim_subgroup = all_subgroups.withColumn(
    'subgroup_category',
    when(col('subgroup') == 'All Students', 'Total')
    .when(col('subgroup').isin(
        'Asian', 'Black', 'Hispanic', 'White', 'Multi-Racial', 
        'American Indian', 'Pacific Islander', 'Two or More Races'
    ), 'Race/Ethnicity')
    .when(col('subgroup').isin('Male', 'Female'), 'Gender')
    .when(col('subgroup').isin(
        'English Learners', 'Students with Disabilities', 'SWD',
        'EL', 'Limited English Proficient', 'Migrant', 'Homeless'
    ), 'Special Populations')
    .when(col('subgroup').isin(
        'Economically Disadvantaged', 'ED', 'Foster Care'
    ), 'Economic Status')
    .otherwise('Other')
).withColumn(
    'is_total',
    when(col('subgroup') == 'All Students', lit(True)).otherwise(lit(False))
).withColumn(
    'subgroup_key',
    monotonically_increasing_id()
).select(
    'subgroup_key',
    'subgroup',
    'subgroup_category',
    'is_total'
).orderBy('subgroup_category', 'subgroup')

print(f"\nDim_Subgroup built: {dim_subgroup.count()} rows\n")

# Show breakdown by category
print("Subgroup breakdown by category:")
dim_subgroup.groupBy('subgroup_category').count().orderBy('subgroup_category').show()

display(dim_subgroup)

# COMMAND ----------

# DBTITLE 1,Write to gold
dim_subgroup.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("workspace.gold.dim_subgroup")
print(f"✓ workspace.gold.dim_subgroup: {spark.table('workspace.gold.dim_subgroup').count():,} rows")

# COMMAND ----------


