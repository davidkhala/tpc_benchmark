"""TPC-DS Pipeline

Downloads, makes and generated bash scripts to generate
TPC-DS data for benchmarking.
"""

import config, tpcds_setup, tpcds_bq 

# create project folders
tpcds_setup.make_directories()

# download tpc-ds and tpc-h zipfiles
tpcds_setup.download_zip()

# extract tpc-ds zip
tpcds_setup.extract_tpcds_zip(zip_filepath=config.fp_ds_zip,
                              version=config.fp_ds_src_version)

# run tpc-ds make for linux
tpcds_setup.make_tpcds()

# edit the tpc-ds schema file
tpcds_bq.schema(filepath_in=config.tpcds_schema_ansi_sql_filepath, 
                filepath_out=config.tpcds_schema_bq_filepath, 
                dataset_name=config.gcp_bq_table)

# generate install specific tpc-ds dsdgen bash scripts
tpcds_setup.dsdgen_bash_scripts(verbose=True)

# move tpc-ds dsdgen bash scripts and related files to output folder
tpcds_setup.dsdgen_move_bash_scripts()

print("done!")