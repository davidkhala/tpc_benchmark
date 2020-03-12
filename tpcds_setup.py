"""Setup file structure for TPC-DS Test

Colin Dietrich, SADA, 2020
"""

import os
import subprocess
import zipfile
import time
import glob
import shutil
import requests

from google.cloud import storage

import config, gcp_storage


def make_directories():
    """Make local directories for TPC tests"""
    filepath_list_1 = [
        config.fp_ds, config.fp_ds_output, 
        config.fp_h, config.fp_h_output,
        config.fp_download
    ]

    for fp in filepath_list_1:
        if not os.path.exists(fp):
            os.mkdir(fp)
            
    if os.path.exists(config.fp_output_mnt):
        filepath_list_2 = [config.fp_ds_output_mnt,
                           config.fp_h_output_mnt]
        for fp in filepath_list_2:
            if not os.path.exists(fp):
                os.mkdir(fp)
            
    with open(config.fp_download + config.sep +
              "place_tpc_zip_here.txt", "w") as f:
        f.write("# Place TCP-DS and TPC-H zip files in this directory\n")
        f.write("# Then run setup_tpcds.py or setup_tpch.py\n")


def make_tpcds():
    """Using the installed C compiler, build TPCDS.  This assumes an 
    installed C compiler is available on the host OS.
    
    Security note: This is also directly running the command line where ever
    config.fp_ds_src is set so be careful.
    """
    subprocess.run(["make", "-C", config.fp_ds_src + config.sep + "tools"])

def download_zip():
    """Download the copy of tpcds source.  See README for versioning."""

    client = storage.Client.from_service_account_json(config.gcp_cred_file)
    print("Client created using default project: {}".format(client.project))
    
    bs = gcp_storage.BlobSync(client=client,
                  bucket_name=config.gcs_zip_bucket,
                  blob_name=config.gcs_ds_zip,
                  local_filepath=config.fp_ds_zip)
    bs.download()
    
    return bs.local_filepath
    
def extract_tpcds_zip(zip_filepath, version):
    """Extract downloaded TPC-DS test .zip to a standard location as set 
    in config.py
    
    Note: it may be better for repeatibility to do this step manually
    
    Parameters
    ----------
    filepath : str, file location of .zip file
    version : str, version of TPC-DS downloaded
    """
    
    with zipfile.ZipFile(zip_filepath) as z:
        z.extractall(config.fp_ds)
    
    
def dsdgen_bash_text(cpu, dsdgen_bin, data_out, scale=1, seed=None):
    """Generate bash scripts for Linux to build TPC-DS datasets
    
    Parameters
    ----------
    cpu : int, number of cpus to use
    dsdgen_bin : str, path to dsdgen binary to execute
    data_out :str, path to output directory
    scale : int, scale factor to generate data in
    seed : int, number to use for random seed of generated data
    
    Returns
    -------
    str : bash syntax using xargs to build TPC-DS data
    """
    
    if scale not in config.tpc_scale:
        raise ValueError("Scale factor must be one of:", config.tpc_scale)
    
    data_out = data_out + config.sep + "{}GB".format(scale)
    
    s = f"""
mkdir {data_out}
seq 1 {cpu} \\
    | xargs -t -P{cpu} -I__ \\
    {dsdgen_bin} \\
    -DIR {data_out} \\
    -SCALE {scale} \\
    -DELIMITER \| \\
    -TERMINATE N  \\"""
    
    if seed is not None:
        s = s + f"""
    -RNGSEED {seed} \\"""
    if cpu > 1:
        s = s + f"""
    -PARALLEL {cpu} \\
    -CHILD __  \\"""              
    return s

def dsdgen_bash_scripts(data_out=None, verbose=False):
    """Generate Bash scripts for various configurations
    
    Range of configuration values:
    cpu = [1, 8, 16, 32, 64, 96]
    scale = [1, 1000, 10000]
    
    Therefore, for each combination, a bash script for that cpu use and scale:
    i.e. `dsdgen_cpu_8_scale_3000.sh`
    would be use 8 cpus to generate the 3TB dataset
    
    data_out : str, target override for data output location.
        If not set, will attempt to find in the following order:
            1. GCS storge set in config.py as fp_ds_output_gcs
            2. Persistent disk location in config.py as fp_output_mnt
            3. Locally set in configy.py fp_ds_output
        If set, str must be a valid path to a directory.
    """
    
    dsdgen_bin = (config.fp_ds_src + 
                  config.sep + 
                  "tools" + 
                  config.sep + 
                  "dsdgen")
    if data_out is None:

        # use the data output folder on GCS if it exists
        # this used FUSE and didn't seem to work - possible latency issue?
        #if os.path.exists(config.fp_ds_output_gcs):
        #    data_out = config.fp_ds_output_gcs
        
        # fall back to persistent disk if it exists
        if os.path.exists(config.fp_ds_output_mnt):
            data_out = config.fp_ds_output_mnt
        # save locally
        else:
            data_out = config.fp_ds_output
    
    if verbose:
        print("Bash scripts will write data to:")
        print(">>", data_out)
    
    for cpu in config.tpc_cpus:
        for scale in config.tpc_scale:
            text = dsdgen_bash_text(cpu=cpu, 
                                    dsdgen_bin=dsdgen_bin, 
                                    data_out=data_out,
                                    scale=scale, 
                                    seed=13)
            fp = (config.fp_ds_src + 
                  config.sep + 
                  "tools" + 
                  config.sep + 
                  f"dsdgen_cpu_{cpu}_scale_{scale}GB.sh")
            with open(fp, "w") as f:
                f.write(text)
                
def dsdgen_move_bash_scripts():
    """Move all Bash scripts needed to generate data to
    tpcds_root/ds/output
    """
    
    glob.glob(config.fp_ds_src + config.sep + 
          "tools" + config.sep + "dsdgen_cpu*")
    
    source_files = glob.glob(config.fp_ds_src + config.sep + 
          "tools" + config.sep + "dsdgen_cpu*")
    
    destination_files = [(config.fp_ds_output + config.sep +
                          f.split(config.sep)[-1]) for f in source_files]
    
    for s, d in zip(source_files, destination_files):
        shutil.move(s, d)
        
    shutil.copyfile(config.fp_ds_src + config.sep + 
          "tools" + config.sep + "tpcds.idx",
                config.fp_ds_output + config.sep + "tpcds.idx")
    
def upload_tpc_data(folder, bucket_name, limit=2000, verbose=False):
    """Upload all data files generated by tpc-ds dsdgen
    
    Parameters
    ----------
    folder : str, absolute path to folder with .dat files
    bucket_name : str, GCS bucket name to upload to
    limit : int, size in MB above which the upload should be skipped
    verbose : bool, print debug and status messages
    
    Returns
    -------
    inventory : list of lists, each item is: 
        [absolute file path, size in MB]
    """
    
    gcs_client = storage.Client.from_service_account_json(config.gcp_cred_file)
    bucket_name = config.gcs_1gb
    
    inventory = []
    
    files = glob.glob(folder)
    for fp in files:
        #table_name = extract_table_name(fp)
        file_name = os.path.basename(fp)
        fp_size = os.path.getsize(fp)/10**6
        if fp_size < limit:

            if verbose:
                print("Uploading {} @ {} MB".format(file_name, fp_size))

            gcs_sync = gcp_storage.BlobSync(client=gcs_client,
                                            bucket_name=bucket_name,
                                            blob_name=file_name,
                                            local_filepath=fp)
            gcs_sync.upload()
            #inventory.append([table_name, file_name, fp])
            inventory.append([file_name, fp])
            if verbose:
                print("...done!")
        elif verbose:
            print("Skipping {} @ {} MB".format(file_name, fp_size))
    return inventory

