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
tpcds_bq.rewrite_schema(filepath_in=config.tpcds_schema_ansi_sql_filepath, 
                        filepath_out=config.tpcds_schema_bq_filepath, 
                        dataset_name=config.gcp_dataset)

scale = 1

# run dsdgen with the max number of cpus on this machine
tpcds_setup.run_dsdgen(cpu=config.cpu_count,
                       scale=scale)

# generate install specific tpc-ds dsdgen bash scripts
# tpcds_setup.dsdgen_bash_scripts(data_out=config.fp_ds_output_mnt, verbose=True)

# move tpc-ds dsdgen bash scripts and related files to output folder
# tpcds_setup.dsdgen_move_bash_scripts()

# create BQ Dataset
tpcds_bq.create_dataset()

# create naive schema
tpcds_bq.create_schema()

folder = config.fp_ds_data_out + config.sep + str(scale) + "GB"

# upload the 1 GB data
df = tpcds_bq.upload_all_local(directory=folder,
                               dataset=config.gcp_dataset, verbose=True)

# print the validation data
print(df)

print("Done!")
print("-----")
