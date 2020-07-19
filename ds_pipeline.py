"""TPC-DS Pipeline

Downloads, makes and generated bash scripts to generate
TPC-DS data for benchmarking.

MIT License, see LICENSE file for complete text.
Copyright (c) 2020 SADA Systems, Inc.
"""

import config, ds_setup, bq

# create project folders
ds_setup.make_directories()

# download tpc-ds and tpc-h zipfiles
ds_setup.download_zip()

# extract tpc-ds zip
ds_setup.extract_tpcds_zip(zip_filepath=config.fp_ds_zip,
                           version=config.fp_ds_src_version)

# run tpc-ds make for linux
ds_setup.make_tpcds()

# edit the tpc-ds schema file
bq.rewrite_schema(filepath_in=config.tpcds_schema_ansi_sql_filepath,
                  filepath_out=config.tpcds_schema_bq_filepath,
                  dataset_name=config.gcp_dataset)

scale = 1

# run dsdgen with the max number of cpus on this machine
ds_setup.run_dsdgen(scale=scale, seed=config.random_seed)

# generate install specific tpc-ds dsdgen bash scripts
# tpcds_setup.dsdgen_bash_scripts(data_out=config.fp_ds_output_mnt, verbose=True)

# move tpc-ds dsdgen bash scripts and related files to output folder
# tpcds_setup.dsdgen_move_bash_scripts()

# create BQ Dataset
bq.create_dataset()

# create naive schema
bq.create_schema()

folder = config.fp_ds_data_out + config.sep + str(scale) + "GB"

# upload the 1 GB data
df = bq.upload_all_local(directory=folder,
                         dataset=config.gcp_dataset, verbose=True)

# print the validation data
print(df)

print("Done!")
print("-----")
