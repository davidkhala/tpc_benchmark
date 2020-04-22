"""Setup TPC-H

Colin Dietrich, SADA, 2020
"""

import os
import shutil
import threading
import subprocess
import concurrent.futures
import zipfile
import glob

from datetime import datetime
from google.cloud import storage

import pandas as pd

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


def modify_dbgen_source(verbose=False):
    """Modify the source file dss.h to prevent the generation of 
    trailing delimiters in the output data.  BigQuery does not support trailing
    delimiters and other methods would be more complex.  
    
    The file is located at: (TPC-H source dir)/dbgen/dss.h
    
    The alteration is done at line 482. Specifically:
    481 #define  PR_STRT(fp)   /* any line prep for a record goes here */
    482 #define  PR_END(fp)    fprintf(fp, "\n")   /* finish the record here */
    
    After alteration, line 482 is:
    482 #define  PR_END(fp) {fseek(fp, -1, SEEK_CUR);fprintf(fp, "\n");}   /* finish the record here */

    Alteration method derived from:
    https://github.com/gregrahn/tpch-kit/commit/abfbdd352fecabc69baea2244cf43ba184b261d3
    
    Parameters
    ----------
    verbose : bool, print debug statements
    """
    
    fp = config.fp_h_src + config.sep + "dbgen" + config.sep + "dss.h"
    
    # make a backup of the original
    f_out = shutil.copyfile(fp, fp[:-2]+".h_backup")
    
    # alter line 482 based on the function signature
    text = open(fp).read()
    old_define = '#define  PR_END(fp)    fprintf(fp, "\\n")'
    new_define = '#define  PR_END(fp) {fseek(fp, -1, SEEK_CUR);fprintf(fp, "\\n");}'
    new_text = text.replace(old_define, new_define)
    open(fp, 'w').write(new_text)
        
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


def run_dbgen(scale=1, total_cpu=None, verbose=False):
    """Create data for TPC-H using the binary dbgen with
    a subprocess for each cpu core on the host machine
    
    Parameters
    ----------
    scale : int, scale factor in GB, acceptable values:
        1, 100, 1000, 10000
    verbose : bool, print stdout and stderr output
    """
    if scale not in config.scale_factors:
        raise ValueError("Scale must be one of:", config.scale_factors)
    
    env_vars = dict(os.environ)
    env_vars["DSS_PATH"] = config.fp_h_data_out + config.sep + str(scale) + "GB"
    
    cmd = ["./dbgen", "-vf", "-s", str(scale)]
    
    if total_cpu is None:
        total_cpu = config.cpu_count
    binary_folder = config.fp_h_src + config.sep + "dbgen"
    
    pipe_outputs = []
    stdout = ""
    stderr = ""
    for n in range(1, total_cpu+1):
        child_cpu = str(n)
        total_cpu = str(total_cpu)
        
        if total_cpu is None:
            n_cmd = cmd + ["-C", total_cpu,
                           "-S", child_cpu]
        else:
            n_cmd = cmd
            
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


def copy_tpl():
    """Move query templates and make copies for modification """
    old_dir = (config.fp_h_src + config.sep + 
               "dbgen" + config.sep + "queries")
    
    # copy of output templates
    new_dir = config.fp_h_query_template_dir
    if not os.path.exists(new_dir):
        tools.copy_recursive(old_dir, new_dir)
    
    # for BigQuery templates
    new_dir = config.fp_h_bq_template_dir
    if not os.path.exists(new_dir):
        tools.copy_recursive(old_dir, new_dir)
    
    # for Snowflake templates
    new_dir = config.fp_h_sf_template_dir
    if not os.path.exists(new_dir):
        tools.copy_recursive(old_dir, new_dir)

        
def edit_tpl():
    """Edit the sqlserver.tpl file such that it will compile ANSI SQL"""
    
    tpl = config.fp_ds_src + config.sep + "query_templates" + config.sep + "sqlserver.tpl"
    
    def modified():
        with open(tpl, "r") as f:
            for line in f:
                if "define _END" in line:
                    return True
            return False
    
    if not modified():
        new_def = '''define _END = "";\n'''
        with open(tpl, "a") as f:
            f.write(new_def)


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

class DGenPool:
    def __init__(self, scale=1, seed=None, n=None, verbose=False):
        
        self.scale = scale
        self.seed = seed
        if self.seed is None:
            self.seed = config.random_seed
        
        self.n = n
        if self.n is None:
            self.n = config.cpu_count
        
        self.child = list(range(1, self.n+1))
        self.parallel = [self.n] * self.n
        
        self.verbose = verbose
        
        self.lock = threading.Lock()
        
        self.results = []
        self.dfr = None

    def run(self, child, parallel):
        """Create data for TPC-DS using the binary dsdgen with
        a subprocess for each cpu core on the host machine

        Parameters
        ----------
        child : int, cpu child thread number
        parallel : int, total number of cpu threads being used
        """
        if self.scale not in config.scale_factors:
            raise ValueError("Scale must be one of:", config.scale_factors)
        
        env_vars = dict(os.environ)
        env_vars["DSS_PATH"] = (config.fp_h_output + 
                                config.sep + str(self.scale) + "GB")
        
        # -f for force overwrite, 
        # will spam stdout with overwrite questions if not used
        cmd = ["./dbgen", "-f", "-s", str(self.scale)]
        
        # random seed - not used in TPC-H?
        
        binary_folder = config.fp_h_src + config.sep + "dbgen"
    
        stdout = ""
        stderr = ""
        
        n_cmd = cmd + ["-C", str(parallel),
                       "-S", str(child)]
        
        t0 = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
        
        pipe = subprocess.run(n_cmd,
                              stdout=subprocess.PIPE, 
                              stderr=subprocess.PIPE, 
                              cwd=binary_folder,
                              env=env_vars)
        
        stdout = pipe.stdout.decode("utf-8")
        stderr = pipe.stderr.decode("utf-8")

        t1 = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
        
        if self.verbose:
            if len(stdout) > 0:
                print(stdout)
            if len(stderr) > 0:
                print(stderr)
        with self.lock:
            self.results.append([child, parallel, t0, t1, stdout, stderr])
        return child
    
    def generate(self):
        with concurrent.futures.ThreadPoolExecutor(max_workers=self.n) as executor:
            exe_results = executor.map(self.run, self.child, self.parallel)
        return exe_results
    
    def save_results(self):
        csv_fp = (config.fp_h_output + config.sep + 
          "datagen-" + str(self.scale) + 
          "GB-" + datetime.utcnow().strftime("%Y%m%d-%H%M%S") + ".csv")
        
        columns = ["child", "parallel", "t0", "t1", "stdout", "stderr"]
        data = list(self.results)
        self.dfr = pd.DataFrame(data, columns=columns)
        self.dfr.to_csv(csv_fp)