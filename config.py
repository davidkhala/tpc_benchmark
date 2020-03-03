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
gcp_dataset  = "foobar2"
gcp_location = "US"

# 1.2 GCP BigQuery Table Name
# >> Edit this to what you want created
gcp_bq_table = "test1"

# 2.0 File Locations
# >> Do NOT edit this section

fp_ds                  = cwd   + sep + "ds"
#fp_ds_src              = fp_ds + sep + "src"
fp_ds_output           = fp_ds + sep + "output"  # folder local to the user
fp_ds_output_data      = "/mnt/disks/20tb"       # mounted persistent disk in the VM
fp_ds_output_snowflake = fp_ds + sep + "output_snowflake"

fp_h                   = cwd   + sep + "h"
#fp_h_src               = fp_h  + sep + "src"
fp_h_output            = fp_ds + sep + "output"
fp_h_output_data       = None
fp_h_output_snowflake  = fp_ds + sep + "output_snowflake"

fp_download = cwd + sep + "download"

# 2.1 Extracted TPC Binaries
# >> Edit this 
fp_ds_zip              = fp_download + sep + "tpc-ds_v2.11.0rc2.zip"
fp_ds_src_version      = "v2.11.0rc2"  # folder name in the .zip
fp_ds_src              = fp_ds + sep + fp_ds_src_version

# >> Edit this 
fp_h_zip               = fp_download + sep + ""
fp_h_src_version       = "2.18.0_rc2"  # folder name in the .zip
fp_h_src               = fp_h  + sep + fp_h_src_version

# 2.2 SQL schema files
tpcds_schema_ansi_sql_filepath = fp_ds_src + sep + "tools" + sep + "tpcds.sql"
tpcds_schema_bq_filepath       = fp_ds_output + sep + "tpcds_schema_bq.sql"
