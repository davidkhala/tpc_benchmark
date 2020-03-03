"""TPC-DS Pipeline, step 1"""

import tpcds_setup, tpcds_bq, config

tpcds_setup.extract_tpcds_zip(zip_filepath=config.fp_ds_zip,
                              version=config.fp_ds_src_version)

tpcds_setup.make_tpcds()

tpcds_bq.schema(filepath_in=config.tpcds_schema_ansi_sql_filepath, 
                filepath_out=config.tpcds_schema_bq_filepath, 
                dataset_name=config.gcp_bq_table)

tpcds_setup.dsdgen_bash_scripts()

tpcds_setup.dsdgen_move_bash_scripts()

print("done!")