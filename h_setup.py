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
    
    env_vars = dict(os.environ)
    env_vars["DSS_PATH"] = config.fp_h_data_out + config.sep + str(scale) + "GB"
    
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
                              cwd=binary_folder,
                              env=env_vars)
        pipe_outputs.append(pipe)

    for pipe in pipe_outputs:
        if len(pipe.stderr) > 0:
            print(pipe.stderr.decode("utf-8"))
        if len(pipe.stdout) > 0:
            print(pipe.stdout.decode("utf-8"))


def run_qgen(n, scale=1, seed=None, verbose=False):
    """Run TPC-H qgen with a subprocess to create the sql query files
    
    Parameters
    ----------
    n : int, query number (from 1 to 22)
    scale : int, scale factor in GB, acceptable values:
        1, 100, 1000, 10000
    seed : int, random seed value
    verbose : bool, print debug statements
    """
    
    fp = config.fp_h_src + config.sep + "dbgen"

    env_vars = dict(os.environ)
    env_vars["DSS_PATH"] = config.fp_h_data_out + config.sep + str(scale) + "GB"
    env_vars["DSS_QUERY"] = fp + config.sep + "queries"

    cmd = ["./qgen"]

    if seed is not None:
        cmd = cmd + ["-r", str(seed)]
    
    cmd = cmd + [str(n)]
    
    pipe = subprocess.run(cmd, 
                          stdout=subprocess.PIPE, 
                          stderr=subprocess.PIPE, 
                          cwd=fp,
                          env=env_vars)

    std_out = pipe.stdout.decode("utf-8")
    err_out = pipe.stderr.decode("utf-8")

    if verbose:
        if len(std_out) > 0:
            print("Standard Out:")
            print("=============")
            print(std_out)
        if len(err_out) > 0:
            print("Error Out")
            print("=========")
            print(err_out)
    
    std_out = std_out.split("\n")
    std_out_new = []
    keep = False
    for line in std_out:
        line = line.rstrip("\r")
        line = line.rstrip("\n")
        if line == "select":
            keep = True
        if keep:
            std_out_new.append(line)
    std_out = std_out_new
    std_out = "\n".join(std_out)
    return std_out, err_out


def move_data(scale, verbose=False):
    """TODO: remove, not needed when using env_vars["DSS_PATH"]
    
    Move TPC-H dbgen files to output folder
    
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


def rewrite_schema(filepath_in, filepath_out, dataset_name):
    """Convert the sample implementation of the logical schema as described in TPC-DS Specification V1.0.0L , specifications.pdf, pg 14, and contained in  tpc_root/dbgen/dss.ddl.

    Parameters
    ----------
    filepath_in : str, path to dss.ddl file
    filepath_out : str, path to write BigQuery formatted table schema, named 'tpch_bq.ddl'
    dataset_name : str, name of BigQuery Dataset to append to existing table names

    Returns
    -------
    None, only writes to file
    """

    # note that leading and trailing whitespace is used to find only table datatype strings
    dtype_mapper = {r' DECIMAL\(\d+,\d+\)': r' FLOAT64',
                    r' VARCHAR\(\d+\)': r' STRING',
                    r' CHAR\(\d+\)': r' STRING',
                    r' INTEGER': r' INT64',
                    # the following are just to have consistent UPPERCASE formatting
                    r' time': r' TIME',
                    r' date': r' DATE'
                    }

    text = open(filepath_in).read()

    for k, v in dtype_mapper.items():
        regex = re.compile(k)
        text = regex.sub(v, text)

    text_list_in = text.split("\n")
    text_list_out = []

    # return text_list_in

    for line in text_list_in:
        # if "primary key" in line:
        #    continue

        if "CREATE TABLE" in line:
            split_line = line.split()  # split on whitespace of n length
            table_name = split_line[2]
            dataset_table_name = dataset_name + "." + table_name
            split_line[2] = dataset_table_name
            new_line = " ".join(split_line)
            text_list_out.append(new_line)
        else:
            text_list_out.append(line)

    text = "\n".join(text_list_out)

    open(filepath_out, "w").write(text)

    return text


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
            inventory.append([file_name, fp])
            if verbose:
                print("...done!")
        elif verbose:
            print("Skipping {} @ {} MB".format(file_name, fp_size))
    return inventory
