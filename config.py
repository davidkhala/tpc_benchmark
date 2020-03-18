"""BigQuery Snowflake Benchmark config values"""


import os
from pathlib import Path

# 0.0 This project's current path locations
# >> Do NOT edit this section
cwd = os.path.dirname(os.path.realpath(__file__))
sep = os.path.sep
user_dir = os.path.expanduser('~')

# 1.0 GCP Service Account Credential File
# >> Edit this with your credential file location
gcp_cred_file = user_dir + sep + "code" + sep + "sada-colin-dietrich-bd003814fcb1.json"

# 1.1 GCP Project and BigQuery Dataset
# >> Edit this to what you want created
gcp_project  = "sada-colin-dietrich"
gcp_dataset  = "gcprabbit"
gcp_location = "US"

# 1.2 GCP Storage Buckets
# >> Edit to correct Link URL of TPC-DS & TPC-H zip files downloaded from TPC
gcs_zip_bucket = "tpc-benchmark-9432"
gcs_ds_zip     = "tpc-ds_v2.11.0rc2.zip"
gcs_h_zip      = "tpc-h_2.18.0_rc2.zip"

# 1.3 Compute Engine Mounted Persistent Disk
fp_output_mnt    = "/mnt/disks/20tb"
fp_ds_output_mnt = fp_output_mnt + sep + "ds"
fp_h_output_mnt  = fp_output_mnt + sep + "h"

gcs_ds_bucket  = "tpc-ds-9432"
gcs_h_bucket   = "tpc-h-9432"

# 1.4 CPU options for TPC data generation, scale factor
cpu_count = os.cpu_count()
tpc_scale = [1, 100, 1000, 10000]  # GB

# 1.5 Random Seed
random_seed = 13

# 2.0 File Locations
# >> Do NOT edit this section

fp_ds                  = cwd   + sep + "ds"
fp_ds_output           = fp_ds + sep + "output"  # folder local to the user

fp_h                   = cwd   + sep + "h"
fp_h_output            = fp_h  + sep + "output"

# 2.1 contingent generated data output locations
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

# 2.1 Extracted TPC Binaries
# >> Edit this 
fp_ds_zip              = fp_download + sep + "tpc-ds_v2.11.0rc2.zip"
fp_ds_src_version      = "v2.11.0rc2"  # folder name in the .zip
fp_ds_src              = fp_ds + sep + fp_ds_src_version

fp_h_zip               = fp_download + sep + "tpc-h_2.18.0_rc2.zip"
fp_h_src_version       = "2.18.0_rc2"  # folder name in the .zip
fp_h_src               = fp_h  + sep + fp_h_src_version

# 2.2 TPC-H makefile parameters
# >> Edit this if not using Linux and targeting SQL
# see lines 104-108 in makefile.suite
c_compiler = "gcc"
database   = "SQLSERVER"
machine    = "LINUX"
workload   = "TPCH"

# 2.3 SQL schema files
tpcds_schema_ansi_sql_filepath = fp_ds_src + sep + "tools" + sep + "tpcds.sql"
tpcds_schema_bq_filepath       = fp_ds_output + sep + "tpcds_schema_bq.sql"

h_schema_ddl_filepath = fp_h_src + sep + "dbgen" + sep + "dss.ddl"
h_schema_bq_filepath = fp_h_output + sep + "tpc_h_schema_bq.ddl"

# 2.4 SQL schema table names (for upload method)
tpcds_table_names = ""
