"""Setup TPC-H

Colin Dietrich, SADA, 2020
"""

import os
import subprocess
import zipfile
import glob

from google.cloud import storage

import config, gcp_storage


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
    """Extract downloaded TPC-H .zip to the location set in config.fp_h
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


def make_tpch(verbose=False):
    """Using the installed C compiler, build TPC-DS.  This assumes an
    installed C compiler is available on the host OS.
    
    Security note: This is also directly running the command line where ever
    config.fp_ds_src is set so be careful.

    Parameters
    ----------
    verbose : bool, print stdout and stderr output
    """
    cmd = ["make"]
    pipe = subprocess.run(cmd, 
                          stdout=subprocess.PIPE, 
                          stderr=subprocess.PIPE,
                          cwd=config.fp_h_src + config.sep + "dbgen")

    stdout = pipe.stdout.decode("utf-8")
    stderr = pipe.stderr.decode("utf-8")

    if verbose:
        if len(stdout) > 0:
            print(stdout)
        if len(stderr) > 0:
            print(stderr)

    return stdout, stderr

# alignment filler


def run_dbgen(scale=1, verbose=False):
    """Create data for TPC-H using the binary dbgen with
    a subprocess for each cpu core on the host machine
    
    Parameters
    ----------
    scale : int, scale factor in GB, acceptable values:
        1, 100, 1000, 10000
    verbose : bool, print stdout and stderr output
    """
    if scale not in config.tpc_scale:
        raise ValueError("Scale must be one of:", config.tpc_scale)
    
    env_vars = dict(os.environ)
    env_vars["DSS_PATH"] = config.fp_h_data_out + config.sep + str(scale) + "GB"
    
    cmd = ["./dbgen", "-vf", "-s", str(scale)]

    total_cpu = config.cpu_count
    binary_folder = config.fp_h_src + config.sep + "dbgen"
    
    pipe_outputs = []
    stdout = ""
    stderr = ""
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
        stdout += pipe.stdout.decode("utf-8")
        stderr += pipe.stderr.decode("utf-8")

    if verbose:
        if len(stdout) > 0:
            print(stdout)
        if len(stderr) > 0:
            print(stderr)

    return stdout, stderr


def run_qgen(n, scale=1, seed=None, verbose=False):
    """Create queries for TPC-H using the binary qgen with
    a subprocess
    
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
