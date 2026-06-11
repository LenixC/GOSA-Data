# Databricks notebook source
# MAGIC %md
# MAGIC # Dump GOSA CSVs to Delta Tables by Report Type
# MAGIC
# MAGIC Reads all CSVs from the upload volume and writes **one Delta table per report type**.
# MAGIC Each table contains all years of that report type (e.g., all Attendance data, all SAT data).
# MAGIC This avoids schema conflicts when different report types have different columns.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Collect all CSV files and group by report type

# COMMAND ----------

# DBTITLE 1,Discover all files
import re
from collections import defaultdict

volume_path = "/Volumes/workspace/default/gosa_data"

# Collect all CSV files with their full paths and metadata
all_csv_files = []

for entry in dbutils.fs.ls(volume_path):
    if not entry.isDir():
        continue
    year_dir = entry.name.rstrip('/')
    
    # Skip data_dictionary directory - handle separately
    if year_dir == "data_dictionary":
        continue
    
    # CSVs may be directly in the year dir or in an Exports/ subdirectory
    for child in dbutils.fs.ls(entry.path):
        if child.name.endswith(".csv"):
            all_csv_files.append((child.path, child.name, year_dir))
        elif child.isDir() and child.name == "Exports":
            for grandchild in dbutils.fs.ls(child.path):
                if grandchild.name.endswith(".csv"):
                    all_csv_files.append((grandchild.path, grandchild.name, year_dir))

print(f"Total CSV files found: {len(all_csv_files)}")

# Show unique filename patterns to help with manual mapping
print("\n=== Unique Filename Patterns (for mapping) ===")
filename_patterns = defaultdict(list)
for file_path, file_name, year in all_csv_files:
    # Extract base name (strip year suffixes, dates, timestamps)
    base = re.sub(r'_\d{4}[-_]\d{2}.*', '', file_name)  # Remove _2024-25_timestamp
    base = re.sub(r'_\d{8}_\d{6}.*', '', base)  # Remove _20260219_003226
    base = re.sub(r'_fy\d{2,4}.*', '', base, flags=re.IGNORECASE)  # Remove _fy2018, _FY24
    base = re.sub(r'\.csv$', '', base, flags=re.IGNORECASE)
    filename_patterns[base.lower()].append((file_name, year))

for pattern in sorted(filename_patterns.keys())[:50]:  # Show first 50
    files = filename_patterns[pattern]
    years = sorted(set(y for _, y in files))
    print(f"  {pattern}: {len(files)} files, years {years[0]}-{years[-1]}")

if len(filename_patterns) > 50:
    print(f"  ... and {len(filename_patterns) - 50} more patterns")

# COMMAND ----------

# DBTITLE 1,Manual report type mapping
# Manual mapping: filename pattern → canonical report name
# Add patterns as you discover them from the output above
# Patterns are checked in order (first match wins)

REPORT_TYPE_MAPPING = [
    # Attendance
    (r'attendance', 'attendance'),
    
    # SAT variants
    (r'sat_highest', 'sat_highest'),
    (r'sat_recent', 'sat_recent'),
    (r'sat_new_highest', 'sat_new_highest'),
    (r'sat_new_recent', 'sat_new_recent'),
    (r'^sat[^_]', 'sat'),  # SAT but not sat_highest/sat_recent
    
    # ACT variants
    (r'act_highest', 'act_highest'),
    (r'^act[^_]', 'act'),
    
    # AP
    (r'^ap[^_]', 'ap'),
    
    # Graduation
    (r'graduation', 'graduation_rate'),
    (r'5-year', 'graduation_5year'),
    
    # Dropout
    (r'dropout', 'dropout_rate'),
    
    # Enrollment
    (r'enrollment_by_grade', 'enrollment_by_grade'),
    (r'enrollment_by_subgroup', 'enrollment_by_subgroup'),
    
    # Direct Certification (strip year prefix)
    (r'direct.*certification.*_d', 'direct_certification_district'),
    (r'direct.*certification.*_s', 'direct_certification_school'),
    (r'directly_certified_district', 'direct_certification_district'),
    (r'directly_certified_school', 'direct_certification_school'),
    
    # Mobility (catches district_mobility, mobility_district, etc. in any order)
    (r'(district.*mobility|mobility.*district)', 'mobility_district'),
    (r'(school.*mobility|mobility.*school)', 'mobility_school'),
    
    # Tests
    (r'^crct', 'crct'),
    (r'^eoct', 'eoct'),
    (r'^eoc_lexile', 'eoc_lexile'),
    (r'^eoc_by_grade', 'eoc_by_grade'),
    (r'^eoc[^_]', 'eoc'),
    (r'^eog_lexile', 'eog_lexile'),
    (r'^eog_by_grade', 'eog_by_grade'),
    (r'^eog[^_]', 'eog'),
    (r'^gaa', 'gaa'),
    (r'^ghswt', 'ghswt'),
    (r'^egwa', 'egwa'),
    
    # Personnel/Staffing
    (r'certified_personnel', 'certified_personnel'),
    (r'educator', 'educator'),
    (r'salaries', 'salaries'),
    
    # Financial - PPE (per-pupil expenditure, catches district_ppe, ppe_district, etc. in any order)
    (r'(district.*ppe|ppe.*district)', 'ppe_district'),
    (r'(school.*ppe|ppe.*school)', 'ppe_school'),
    
    # Financial - FESR (fiscal expenditure summary report, order-agnostic)
    (r'(district.*fesr|fesr.*district)', 'fesr_district'),
    (r'(school.*fesr|fesr.*school)', 'fesr_school'),
    
    # Financial - Revenues
    (r'revenues', 'revenues'),
    
    # EL/ELL
    (r'el_exit.*state', 'el_exit_state'),
    (r'el_exit.*district', 'el_exit_district'),
    (r'ell_deferred', 'ell_deferred'),
    
    # Completers
    (r'hs_complet.*credential', 'hs_completer_credentials'),
    (r'hs_complet', 'hs_completers'),
    (r'highschool_completer', 'hs_completers'),
    
    # Other
    (r'hope', 'hope_eligible'),
    (r'retained', 'retained'),
    (r'^c11', 'c11_hs_graduates'),
    (r'^c12', 'c12_hs_graduates'),
]

def map_filename_to_report_type(filename):
    """Apply manual mapping rules to determine canonical report type."""
    filename_lower = filename.lower()
    
    for pattern, report_type in REPORT_TYPE_MAPPING:
        if re.search(pattern, filename_lower):
            return report_type
    
    # Fallback: use first part of filename
    base = re.sub(r'[_\s]\d.*', '', filename).lower()
    return base.replace('.csv', '')

print("Loaded manual mapping with {} rules".format(len(REPORT_TYPE_MAPPING)))

# COMMAND ----------

# DBTITLE 1,Apply mapping and group files
# Apply the mapping to group files by canonical report type
csvs_by_report_type = defaultdict(list)

for file_path, file_name, year in all_csv_files:
    report_type = map_filename_to_report_type(file_name)
    csvs_by_report_type[report_type].append((file_path, year))

print(f"\n=== Grouped into {len(csvs_by_report_type)} report types ===")
for report_type in sorted(csvs_by_report_type.keys()):
    files = csvs_by_report_type[report_type]
    years = sorted(set(year for _, year in files))
    print(f"  {report_type}: {len(files)} files, years {years[0]}-{years[-1]}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Write one Delta table per report type

# COMMAND ----------

# DBTITLE 1,Create bronze schema
# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS workspace.bronze

# COMMAND ----------

# DBTITLE 1,Drop existing bronze tables
# Clean up existing bronze tables to avoid orphaned year-based tables
out_catalog = "workspace.bronze"

existing_tables = spark.sql(f"SHOW TABLES IN {out_catalog}").collect()
if existing_tables:
    print(f"Dropping {len(existing_tables)} existing bronze tables...")
    for row in existing_tables:
        table_name = f"{out_catalog}.{row.tableName}"
        spark.sql(f"DROP TABLE IF EXISTS {table_name}")
        print(f"  Dropped {table_name}")
    print("✓ All existing bronze tables dropped")
else:
    print("No existing bronze tables to drop")

# COMMAND ----------

# DBTITLE 1,Cell 5
from pyspark.sql.functions import lit
from functools import reduce

out_catalog = "workspace.bronze"

for report_type, file_list in sorted(csvs_by_report_type.items()):
    table_name = f"{out_catalog}.{report_type}"
    print(f"\nProcessing {report_type}: {len(file_list)} files")
    
    # Read each file as RAW strings - no schema inference, no type casting
    # Bronze layer is 1:1 with source CSVs
    dfs = []
    for file_path, year in file_list:
        df = spark.read \
            .option("header", "true") \
            .option("inferSchema", "false") \
            .csv(file_path)
        
        # Add metadata columns
        df = df.withColumns({
            "source_year": lit(year),
            "source_file": lit(file_path.split('/')[-1])
        })
        dfs.append(df)
        print(f"  · {year}/{file_path.split('/')[-1]}: {len(df.columns)} columns")
    
    # Union all years for this report type
    # allowMissingColumns=True accepts schema drift - nulls will be present where columns don't exist
    # Silver layer will handle column standardization/merging explicitly
    unified_df = reduce(lambda a, b: a.unionByName(b, allowMissingColumns=True), dfs)
    
    # Write to Delta (all columns are strings at this point)
    writer = unified_df.write.mode("overwrite")
    
    # Enable column mapping for columns with spaces/special chars
    writer = writer.option("delta.columnMapping.mode", "name")
    
    writer.saveAsTable(table_name)
    row_count = unified_df.count()
    col_count = len(unified_df.columns)
    print(f"  ✓ Created {table_name} with {row_count:,} rows, {col_count} columns (all strings)")

# COMMAND ----------

# DBTITLE 1,List bronze tables
bronze_tables = spark.sql(f"SHOW TABLES IN {out_catalog}").collect()
print(f"Created {len(bronze_tables)} bronze tables:\n")
for row in sorted(bronze_tables, key=lambda x: x.tableName):
    print(f"  workspace.bronze.{row.tableName}")

# COMMAND ----------

# DBTITLE 1,Summary statistics
print("\n=== Bronze Layer Summary ===")
total_rows = 0
for report_type in sorted(csvs_by_report_type.keys()):
    table_name = f"{out_catalog}.{report_type}"
    try:
        df = spark.table(table_name)
        row_count = df.count()
        years = df.select("source_year").distinct().count()
        total_rows += row_count
        print(f"  {report_type}: {row_count:,} rows across {years} years")
    except:
        pass

print(f"\nTotal rows across all tables: {total_rows:,}")

# COMMAND ----------

# DBTITLE 1,Load data dictionary
# Handle data dictionary separately (has special column names with spaces)
print("\nProcessing data_dictionary...")
dd_path = f"{volume_path}/data_dictionary/*.csv"
dd_df = spark.read.option("header", "true").option("inferSchema", "true").csv(dd_path)
dd_df.write.mode("overwrite").option("delta.columnMapping.mode", "name").saveAsTable(f"{out_catalog}.data_dictionary")
print(f"  ✓ Created workspace.bronze.data_dictionary with {dd_df.count()} rows")

# COMMAND ----------

# DBTITLE 1,Sample query
# Example: Query attendance data
print("\nSample: Attendance data for 2024")
if 'attendance' in csvs_by_report_type:
    attendance_df = spark.table(f"{out_catalog}.attendance")
    attendance_df.filter("source_year = '2024'").show(5, truncate=False)
