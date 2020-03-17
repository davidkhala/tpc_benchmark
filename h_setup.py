"""Setup TPC-H

Colin Dietrich, SADA, 2020

Note: 
"""

import os
import shutil
import subprocess
import zipfile
import time
import glob
import shutil
import requests

from google.cloud import storage

import config, gcp_storage


def make_directories():
    """Make local directories for TPC DS & H tests"""
    filepath_list_1 = [
        #config.fp_ds, config.fp_ds_output, 
        config.fp_h, config.fp_h_output,
        config.fp_download
    ]

    filepath_list_1 += [config.fp_h_output + 
                        config.sep + str(i) + "GB" for i in config.tpc_scale]
    
    for fp in filepath_list_1:
        if not os.path.exists(fp):
            os.mkdir(fp)
            
    if os.path.exists(config.fp_output_mnt):
        filepath_list_2 = [#config.fp_ds_output_mnt,
                           config.fp_h_output_mnt]
        filepath_list_1 += [config.fp_h_output_mnt + 
                            config.sep + str(i) + "GB" for i in config.tpc_scale]
    
        
        for fp in filepath_list_2:
            if not os.path.exists(fp):
                os.mkdir(fp)
            
    with open(config.fp_download + config.sep +
              "place_tpc_zip_here.txt", "w") as f:
        f.write("# Place TCP-DS and TPC-H zip files in this directory\n")
        f.write("# Then run setup_tpcds.py or setup_tpch.py\n")
        

def download_zip():
    """Download the copy of tpcds source.  See README for versioning."""

    client = storage.Client.from_service_account_json(config.gcp_cred_file)
    print("Client created using default project: {}".format(client.project))
    
    bs = gcp_storage.BlobSync(client=client,
                  bucket_name=config.gcs_zip_bucket,
                  blob_name=config.gcs_h_zip,
                  local_filepath=config.fp_h_zip)
    bs.download()
    
    return bs.local_filepath

def extract_zip():
    """Extract downloaded TPC-DS test .zip to a standard location as set 
    in config.py
    
    Note: it may be better for repeatibility to do this step manually
    
    Parameters
    ----------
    filepath : str, file location of .zip file
    version : str, version of TPC-DS downloaded
    """
    
    with zipfile.ZipFile(config.fp_h_zip) as z:
        z.extractall(config.fp_h)
        
def create_makefile(verbose=False):
    """Edit dbgen/makefile.suite and save to makefile
    setting the c compiler, database, machine and workload
    """
    replacement = {"CC      ="  : "CC       = {}".format(config.c_compiler),
                   "DATABASE="  : "DATABASE = {}".format(config.database),
                   "MACHINE ="  : "MACHINE  = {}".format(config.machine),
                   "WORKLOAD =" : "WORKLOAD = {}".format(config.workload)}

    fp = config.fp_h_src + config.sep + "dbgen" + config.sep
    new_lines = []
    
    with open(fp + "makefile.suite", "r") as f:
        for line in f:
            line = line.strip("\n")
            original = True
            for k, v in replacement.items():
                if k in line:
                    if verbose:
                        print("Makefile.suite:", line)
                        print("Replacement:", v)
                        print("-"*20)
                    new_lines.append(v)
                    original = False
            if original:
                new_lines.append(line)

    with open(fp + "makefile", "w") as f:
        for line in new_lines:
            f.write(line + "\n")
        
def make_tpch():
    """Using the installed C compiler, build TPCDS.  This assumes an 
    installed C compiler is available on the host OS.
    
    Security note: This is also directly running the command line where ever
    config.fp_ds_src is set so be careful.
    """
    #cmd = ["make", "-C", config.fp_h_src + config.sep + "dbgen"]
    cmd = ["make"]
    pipe = subprocess.run(cmd, 
                          stdout=subprocess.PIPE, 
                          stderr=subprocess.PIPE,
                          cwd=config.fp_h_src + config.sep + "dbgen")
    
    if len(pipe.stderr) > 0:
        print(pipe.stderr.decode("utf-8"))
    if len(pipe.stdout) > 0:
        print(pipe.stdout.decode("utf-8"))
    
def run_dbgen(scale=1):
    """Run TPC-DS dsdgen with a subprocess for each cpu core 
    on the host machine
    
    Parameters
    ----------
    scale : int, scale factor in GB, acceptable values:
        1, 100, 1000, 10000
    seed : int, random seed value
    """
    if scale not in config.tpc_scale:
        raise ValueError("Scale must be one of:", config.tpc_scale)
    
    cmd = ["./dbgen", "-vf", "-s", str(scale)]

    total_cpu = config.cpu_count
    binary_folder = config.fp_h_src + config.sep + "dbgen"
    
    pipe_outputs = []
    for n in range(1, total_cpu+1):
        child_cpu = str(n)
        total_cpu = str(total_cpu)
        n_cmd = cmd + ["-C", total_cpu,
                       "-S", child_cpu]
    
        pipe = subprocess.run(n_cmd, 
                              stdout=subprocess.PIPE, 
                              stderr=subprocess.PIPE, 
                              cwd=binary_folder)
        pipe_outputs.append(pipe)

    for pipe in pipe_outputs:
        if len(pipe.stderr) > 0:
            print(pipe.stderr.decode("utf-8"))
        if len(pipe.stdout) > 0:
            print(pipe.stdout.decode("utf-8"))
    
def move_data(scale, verbose=False):
    """Move TPC-H dbgen files to output folder
    
    Parameters
    ----------
    scale : int, scale factor of generated data in GB
    verbose : bool, print debug statements
    """
    source_files = glob.glob(config.fp_h_src + config.sep + 
                      "dbgen" + config.sep + "*.tbl*")
    dest = config.fp_h_data_out + config.sep + str(scale) + "GB"
    destination_files = [(dest + config.sep + 
                          f.split(config.sep)[-1]) for f in source_files]
    for s, d in zip(source_files, destination_files):
        if verbose:
            print("Moving: ", s)
            print("To:     ", d)
            print("--------")
        shutil.move(s, d)
    
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

