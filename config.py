"""BigQuery Snowflake Benchmark config values"""


import os
from pathlib import Path

# 1.0 Project's current path locations
# >> Do NOT edit this section
cwd = os.path.dirname(os.path.realpath(__file__))
sep = os.path.sep
user_dir = os.path.expanduser('~')

# 1.1 Host computer's CPU count for TPC data generation
cpu_count = os.cpu_count()

# 2.0 GCP Service Account Credential File
# >> Edit this with your credential file location
#cred_file_name = "sada-colin-dietrich-bd003814fcb1.json"
cred_file_name = "tpc-benchmarking-9432-3fe6b68089ac.json"

# full path used in method calls
gcp_cred_file = user_dir + sep + "code" + sep + cred_file_name

# 2.1 GCP Project and BigQuery Dataset
# >> Edit this to what project is hosting this work on GCP
# note: this will have to match your credential file
# note: if the project name has uppercase, 
#       the BQ dataset will need to be converted to all lower 
#gcp_project      = "sada-colin-dietrich"
gcp_project      = "TPC-Benchmarking-9432"
gcp_location     = "US"

# 2.2 Cloud Storage Buckets
# >> Edit to correct Link URL of TPC-DS & TPC-H zip files downloaded from TPC
#gcs_zip_bucket   = "tpc-benchmark-zips-9432"
#gcs_data_bucket  = "tpc-benchmark-9432"

gcs_zip_bucket   = "tpc-benchmark-zips-5947"
gcs_data_bucket  = "tpc-benchmark-5947"

# 2.3 BigQuery Datasets
# >> Do NOT edit this section after dev work
#gcp_dataset      = "gcprabbit"

dataset_h_1GB_basic    = "h_1GB_basic"
dataset_h_100GB_basic  = "h_100GB_basic"
dataset_h_1TB_basic    = "h_1TB_basic"
dataset_h_10TB_basic   = "h_10TB_basic"

dataset_ds_1GB_basic   = "ds_1GB_basic"
dataset_ds_100GB_basic = "ds_100GB_basic"
dataset_ds_1TB_basic   = "ds_1TB_basic"
dataset_ds_10TB_basic  = "ds_10TB_basic"

# 2.4 Compute Engine Mounted Persistent Disk
# >> Edit only if you created a separate persistent disk on the VM
fp_output_mnt    = "/mnt/disks/data"

# leave thes as is
fp_ds_output_mnt = fp_output_mnt + sep + "ds"
fp_h_output_mnt  = fp_output_mnt + sep + "h"


# 3.0 TPC installer zip file names
gcs_ds_zip       = "tpc-ds_v2.11.0rc2.zip"
gcs_h_zip        = "tpc-h_2.18.0_rc2.zip"

# 3.1 TPC File and Data Locations
# >> Do NOT edit this section
fp_ds                  = cwd   + sep + "ds"
#fp_ds_output           = fp_ds + sep + "output"  # folder local to the user
fp_ds_output           = fp_ds

fp_h                   = cwd   + sep + "h"
#fp_h_output            = fp_h  + sep + "output"
fp_h_output            = fp_h

# 3.2 contingent generated data output locations
# >> Do NOT edit this section
if os.path.exists(fp_ds_output_mnt):
    fp_ds_data_out = fp_ds_output_mnt
else:
    fp_ds_data_out = fp_ds_output

if os.path.exists(fp_ds_output_mnt):
    fp_h_data_out = fp_h_output_mnt
else:
    fp_h_data_out = fp_h_output

fp_download = cwd + sep + "download"

# 3.3 Extracted TPC Binaries
# >> Edit this based on what's in stored in gcs_zip_bucket
fp_ds_zip              = fp_download + sep + gcs_ds_zip   # "tpc-ds_v2.11.0rc2.zip"
fp_ds_src_version      = "v2.11.0rc2"  # folder name in the .zip
fp_ds_src              = fp_ds + sep + fp_ds_src_version

fp_h_zip               = fp_download + sep + gcs_h_zip  # "tpc-h_2.18.0_rc2.zip"
fp_h_src_version       = "2.18.0_rc2"  # folder name in the .zip
fp_h_src               = fp_h  + sep + fp_h_src_version

# 3.4 Random Seed for data generation
random_seed = 13

# 3.5 TPC-H makefile parameters
# >> Edit this if not using Linux and targeting SQL
# see lines 104-108 in makefile.suite
c_compiler = "gcc"
database   = "SQLSERVER"
machine    = "LINUX"
workload   = "TPCH"

# 4.1 ANSI/DDL SQL Schema Files
ds_schema_ansi_sql_filepath = fp_ds_src + sep + "tools" + sep + "tpcds.sql"
h_schema_ddl_filepath = fp_h_src + sep + "dbgen" + sep + "dss.ddl"

# 4.2 Basic BigQuery Schema Files
fp_schema = cwd + sep + "schema"
fp_ds_schema = fp_schema + sep + "ds"
fp_h_schema  = fp_schema + sep + "h"

#ds_schema_bq_basic_filepath = fp_ds_output + sep + "tpcds_schema_bq_basic.sql"
#h_schema_bq_basic_filepath  = fp_h_output + sep + "tpc_h_schema_bq_basic.ddl"

# 4.3 Files output by data generators from either test
# to ignore
ignore_files = ["version"]

# 4.4 key to find in queries for project and dataset appending
p_d_id = "49586899439487868_"

# 5.0 Experimental Setup
tests = ["ds", "h"]
scale_factors = [1, 100, 1000, 10000]  # GB
scale_factor_mapper = {"1GB": 1, "100GB": 100, "1TB": 1000, "10TB": 10000}

# 5.1 Query Templates
fp_query_templates = cwd + sep + "tpl"

# for backup of default outputs
fp_ds_ansi_gen_template_dir = fp_query_templates + sep + "ansi_ds_gen"
fp_h_ansi_gen_template_dir  = fp_query_templates + sep + "ansi_h_gen"

fp_ds_bq_gen_template_dir   = fp_query_templates + sep + "bq_ds_gen"
fp_h_bq_gen_template_dir    = fp_query_templates + sep + "bq_h_gen"

fp_ds_sf_gen_template_dir   = fp_query_templates + sep + "sf_ds_gen"
fp_h_sf_gen_template_dir    = fp_query_templates + sep + "sf_h_gen"

fp_ds_bq_template_dir   = fp_query_templates + sep + "bq_ds"
fp_h_bq_template_dir    = fp_query_templates + sep + "bq_h"

fp_ds_sf_template_dir   = fp_query_templates + sep + "sf_ds"
fp_h_sf_template_dir    = fp_query_templates + sep + "sf_h"

# 5.2 Schema Variations
fp_schema = cwd + sep + "sc"

bq_schema = ["ds_1GB_basic"]
sf_schema = ["sf_1GB_basic"]

# 5.3 Generated Queries  
fp_query = cwd + sep + "q"

# naive, mimimal conversion of ANSI to BQ or SF syntax
#bq_queries = ["ds_00_default_ansi", "ds_01_naive"]
#sf_queries = ["h_00_default_ansi", "h_01_naive"]

#fp_ds_q_ex01_naive = fp_query + sep + "ds_q_ex01_naive"
#fp_h_q_ex01_naive  = fp_query + sep + "h_q_ex01_naive"

# 5.4 Qualification Query Answers  
fp_ds_answers = fp_ds_src + sep + "answer_sets"


# 5.4 Test Schema Combinations
# TODO: perhaps remove?
"""
test_schema_bq = []
for test in tests:
    for scale in scale_factors:
        for schema in bq_schema:
            test_schema_bq.append(test + "_" + str(scale) + "GB_" + schema)

test_schema_sf = []
for test in tests:
    for schema in sf_schema:
        test_schema_sf.append(test + "_" + schema)
"""

