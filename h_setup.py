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

import config, tools, gcp_storage


log_column_names = ["test", "scale", "status",
                    "child", "parallel", 
                    "t0", "t1", "stdout", "stderr"]

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

def edit_tpcd_h():
    """Edit the SET_ROWCOUNT value under the SQLSERVER section
    of the C header file.  
    
    Note: the file references TPC-D, 
    this was a previous TPC benchmark tool and probably 
    this source is derivative."""
    
    fp = config.fp_h_src + config.sep + "dbgen" + config.sep + "tpcd.h"
    
    # make a backup of the original
    f_out = shutil.copyfile(fp, fp[:-2]+".h_backup")
    
    d = []
    section = False
    with open(fp, "r") as f:
        for line in f:
            if "#ifdef 	SQLSERVER" in line:
                section = True
            if ("#define SET_ROWCOUNT" in line) & section:
                line = '''#define SET_ROWCOUNT    "limit %d\\n"''' + '\n'
            d.append(line)
    with open(fp, "w") as f:
        for line in d:
            f.write(line)

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
    env_vars["DSS_PATH"] = config.fp_h_output + config.sep + str(scale) + "GB"
    
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


def copy_tpl(verbose=False):
    """Move query templates and make copies for modification """
    old_dir = (config.fp_h_src + config.sep + 
               "dbgen" + config.sep + "queries")
    if verbose:
        print("Source directory:", old_dir)
        
    template_directories = [
        # ANSI templates, as generated, for references
        config.fp_h_ansi_gen_template_dir,
        
        # BigQuery templates, as generated, for references
        config.fp_h_bq_gen_template_dir,
        
        # Snowflake templates, as generated, for references
        config.fp_h_sf_gen_template_dir,
        
        # BigQuery templates, to be edited by hand before query generation
        config.fp_h_bq_template_dir,
        
        # Snowflake templates, to be edited by hand before query generation
        config.fp_h_sf_template_dir
        ]
    
    for new_dir in template_directories:
        if not os.path.exists(new_dir):
            os.mkdir(new_dir)
            tools.copy_recursive(old_dir, new_dir)
            if verbose:
                print("Moved all files to:", new_dir)

    if verbose:
        print("Done.  Note: If none printed above, there were no new templates to write.")
        
def sqlserver_bq_defines(template_root):
    """Edit the sqlserver.tpl file such that it will compile ANSI SQL"""
    
    dialect = "sqlserver_bq"
    tpl = template_root + config.sep + dialect + ".tpl"
    
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


def qgen(path_dir=None, config_dir=None, templates_dir=None,
         n=None,
         a=None,
         b=None,
         c=None,
         d=None,
         #h=None,
         i=None,
         l=None,
         #n=None,
         #N=None,
         o=None,
         p=None,
         r=None,
         s=None,
         v=None,
         t=None,
         x=None,
         verbose=False):
    """Run the TPC-H Query substitution program and generate
    SQL queries
    
    TPC-H Parameter Substitution (v. 2.18.0 build 0)
    Copyright Transaction Processing Performance Council 1994 - 2010
    USAGE: ./qgen <options> [ queries ]
    Options:
        -a        -- use ANSI semantics.
        -b <str>  -- load distributions from <str>
        -c        -- retain comments found in template.
        -d        -- use default substitution values.
        -h        -- print this usage summary.
        -i <str>  -- use the contents of file <str> to begin a query.
        -l <str>  -- log parameters to <str>.
        -n <str>  -- connect to database <str>.
        -N        -- use default rowcounts and ignore :n directive.
        -o <str>  -- set the output file base path to <str>.
        -p <n>    -- use the query permutation for stream <n>
        -r <n>    -- seed the random number generator with <n>
        -s <n>    -- base substitutions on an SF of <n>
        -v        -- verbose.
        -t <str>  -- use the contents of file <str> to complete a query
        -x        -- enable SET EXPLAIN in each query.

    Environment variables are used to control features of DBGEN and QGEN
    which are unlikely to change from one execution to another.

    Variable    Default     Action
    -------     -------     ------
    DSS_PATH    .           Directory in which to build flat files
    DSS_CONFIG  .           Directory in which to find configuration files
    DSS_DIST    dists.dss   Name of distribution definition file
    DSS_QUERY   .           Directory in which to find query templates
    
    Note: this project does not alter the distributions of the generated data.
    """
    
    kwargs = []
    
    if a is not None:
        kwargs.append("-a")
        # kwargs.append(a)

    if b is not None:
        kwargs.append("-b")
        kwargs.append(str(b))

    if c is not None:
        kwargs.append("-c")
        # kwargs.append(file)
        
    if d is not None:
        kwargs.append("-d")
        
    # if h is not None:
    #     kwargs.append("-h")
        
    if i is not None:
        kwargs.append("-i")
        kwargs.append(i)
        
    if l is not None:
        kwargs.append("-l")
        kwargs.append(str(l))
        
    # if n is not None:
    #     kwargs.append("-n")
    #     kwargs.append(n)
        
    # if N is not None:
    #     kwargs.append("-N")
    #     kwargs.append(N)
        
    if o is not None:
        kwargs.append("-o")
        kwargs.append(o)
        
    if p is not None:
        kwargs.append("-p")
        kwargs.append(str(p))
        
    if r is not None:
        kwargs.append("-r")
        kwargs.append(str(r))
        
    if s is not None:
        kwargs.append("-s")
        kwargs.append(str(s))
        
    if v:
        kwargs.append("-v")
        
    if t is not None:
        kwargs.append("-t")
        kwargs.append(str(t))
    
    if x is not None:
        kwargs.append("-x")
            
    if type(n) == int:
        n = str(n)
        kwargs.append(n)
    elif type(n) == list:
        n = " ".join([str(_) for _ in x])
        kwargs.append(n)
   
    fp = config.fp_h_src + config.sep + "dbgen"

    env_vars = dict(os.environ)
    if path_dir is not None:
        env_vars["DSS_PATH"] = path_dir
    # if config_dir is not None:
    #     env_vars["DSS_CONFIG"] = config_dir
    if templates_dir is not None:
        env_vars["DSS_QUERY"] = templates_dir
    
    cmd = ["./qgen"] + kwargs

    if verbose:
        print("="*40)
        print("TPC-H qgen parameters")
        print("---------------------")
        print("cmd & kwargs:", cmd)
        print("path_dir:", path_dir)
        print("templates_dir:", templates_dir)
        print("cwd:", fp)
        print()

    pipe = subprocess.run(cmd, 
                          stdout=subprocess.PIPE, 
                          stderr=subprocess.PIPE, 
                          cwd=fp,
                          env=env_vars)

    std_out = pipe.stdout.decode("utf-8")
    err_out = pipe.stderr.decode("utf-8")
    
    return std_out, err_out


def std_out_filter(std_out):
    """Filter off the beginning cruft of a std_out query 
    generated by qgen"""
    
    std_out = std_out.split("\n")
    std_out_new = []
    keep = False
    for line in std_out:
        line = line.rstrip("\r")
        line = line.rstrip("\n")
        if line[:6] in ["select", "create"]:
            keep = True
        if keep:
            std_out_new.append(line)
    std_out = std_out_new
    std_out = "\n".join(std_out)
    return std_out


def std_err_print(std_out, err_out):
    if len(std_out) > 0:
        print("Standard Out:")
        print("=============")
        print(std_out)
    if len(err_out) > 0:
        print("Error Out")
        print("=========")
        print(err_out)


def qgen_template(n, templates_dir, scale=1, qual=None, verbose=False):
    """Generate H query text for query template number n
    
    Parameters
    ----------
    n : int, query number to generate SQL
    templates_dir : str, absolute path to directory of query templates
        to draw from for n.
    scale : int, scale factor of db being queried
    qual : bool, generate qualification queries in ascending order
    verbose : bool, print debug statements

    Returns
    -------
    std_out : str, BigQuery SQL query
    std_err : str, error message if generation fails
    """

    if config.random_seed is not None:
        r = config.random_seed
    else:
        r = None

    std_out, err_out = qgen(n=n, 
                            r=r,
                            d=qual,
                            s=scale,
                            templates_dir=templates_dir,
                            verbose=verbose,
                            x=True
                            )
    
    std_out = std_out_filter(std_out)
    
    if verbose:
        std_err_print(std_out, err_out)
    
    return std_out, err_out


def qgen_stream_single(p, templates_dir, output_dir, scale=1, qual=None, verbose=False):
    """Generate TPC-H query number n and write it to disk.
    
    Parameters
    ----------
    p : int, query stream number to generate BigQuery SQL where:
        p = -1 = queries in order 1-22
        p = 0 = power test
        p = 1+ = throughput tests 1-40
    templates_dir : str, absolute path to directory of query templates
        to draw from for n.
    output_dir : str, absolute path to directory to write compiled sql query streams
    scale : int, scale factor of db being queried
    qual : bool, generate qualification queries in ascending order
    verbose : bool, print debug statements

    Returns
    -------
    std_out : str, terminal messages if generation succeeds
    std_err : str, error message if generation fails
    """

    if config.random_seed is not None:
        r = config.random_seed
    else:
        r = None

    if p == -1: 
        p = None
    
    std_out, err_out = qgen(p=p,
                            r=r,
                            d=qual,
                            s=scale,
                            o=output_dir,
                            templates_dir=templates_dir,
                            verbose=verbose
                            )

    std_out = std_out_filter(std_out)

    return std_out, err_out


def qgen_streams(m, templates_dir, output_dir, scale=1, qual=None, verbose=False):
    """Generate TPC-H query number 0-m and write them to disk.

    Parameters
    ----------
    m : int, number of query streams to generate SQL, 0-m combinations
    templates_dir : str, absolute path to directory of query templates
        to draw from for n.
    output_dir : str, absolute path to directory to write compiled sql query streams
    scale : int, scale factor of db being queried
    qual : bool, generate qualification queries in ascending order
    verbose : bool, print debug statements

    Returns
    -------
    std_out : str, terminal messages if generation succeeds
    std_err : str, error message if generation fails
    """

    std_out = ""
    err_out = ""
    for _p in range(1, m):

        _so, _eo = qgen_stream_single(p=_p,
                                      templates_dir=templates_dir,
                                      output_dir=output_dir,
                                      scale=scale,
                                      qual=qual,
                                      verbose=verbose)
        std_out += _so + "\n"
        err_out += _eo + "\n"

    return std_out, err_out


def parse_log(fp):
    
    return pd.read_csv(fp, names=log_column_names)


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
        
        t0 = pd.Timestamp.now()
        
        with self.lock:
            self.results.append(["h", str(self.scale), "start",
                                 child, parallel, 
                                 str(t0), "", "", ""])
        
        pipe = subprocess.run(n_cmd,
                              stdout=subprocess.PIPE, 
                              stderr=subprocess.PIPE, 
                              cwd=binary_folder,
                              env=env_vars)
        
        stdout = pipe.stdout.decode("utf-8")
        stderr = pipe.stderr.decode("utf-8")

        t1 = pd.Timestamp.now()
        
        if self.verbose:
            if len(stdout) > 0:
                print(stdout)
            if len(stderr) > 0:
                print(stderr)
                        
        with self.lock:
            self.results.append(["h", str(self.scale), "end",
                                 child, parallel, 
                                 str(t0), str(t1), stdout, stderr])
        return child
    
    def generate(self):
        with concurrent.futures.ThreadPoolExecutor(max_workers=self.n) as executor:
            exe_results = executor.map(self.run, self.child, self.parallel)
        return exe_results
    
    def save_results(self):
        
        csv_fp = (config.fp_h_output + config.sep + 
                  "datagen-h_" + str(self.scale) + "GB-" + 
                  str(pd.Timestamp.now()) + ".csv"
                  )
        
        data = list(self.results)
        self.dfr = pd.DataFrame(data, columns=log_column_names)
        self.dfr.to_csv(csv_fp)