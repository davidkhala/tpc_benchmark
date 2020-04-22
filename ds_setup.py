"""Setup TPC-DS

Colin Dietrich, SADA, 2020
"""

import os
import re
import threading
import subprocess
import concurrent.futures
import zipfile
import glob

from google.cloud import storage

import config, gcp_storage, tools


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


def extract_zip():
    """Extract downloaded TPC-DS .zip to the location set in config.fp_ds
    """
    with zipfile.ZipFile(config.fp_ds_zip) as z:
        z.extractall(config.fp_ds)

""" create_makefile filler





























"""


def make_tpcds(verbose=False):
    """Using the installed C compiler, build TPC-DS.  This assumes an
    installed C compiler is available on the host OS.

    Security note: This is also directly running the command line where ever
    config.fp_ds_src is set so be careful.

    Parameters
    ----------
    verbose : bool, print stdout and stderr output
    """
    #subprocess.run(["make", "-C", config.fp_ds_src + config.sep + "tools"])

    cmd = ["make"]
    pipe = subprocess.run(cmd,
                          stdout=subprocess.PIPE,
                          stderr=subprocess.PIPE,
                          cwd=config.fp_ds_src + config.sep + "tools")

    stdout = pipe.stdout.decode("utf-8")
    stderr = pipe.stderr.decode("utf-8")

    if verbose:
        if len(stdout) > 0:
            print(stdout)
        if len(stderr) > 0:
            print(stderr)

    return stdout, stderr


def run_dsdgen(scale=1, seed=None, verbose=False):
    """Create data for TPC-DS using the binary dsdgen with
    a subprocess for each cpu core on the host machine
    
    Parameters
    ----------
    scale : int, scale factor in GB, acceptable values:
        1, 100, 1000, 10000
    seed : int, random seed value
    verbose : bool, print stdout and stderr output
    """
    if scale not in config.scale_factors:
        raise ValueError("Scale must be one of:", config.scale_factors)

    _data_out = config.fp_ds_data_out + config.sep + str(scale) + "GB"

    cmd = ["./dsdgen", "-DIR", _data_out, "-SCALE", str(scale),
           "-DELIMITER", "|", "-TERMINATE", "N"]

    if seed is not None:
        cmd = cmd + ["-RNGSEED", str(seed)]

    total_cpu = config.cpu_count
    binary_folder = config.fp_ds_src + config.sep + "tools"
    pipe_outputs = []
    stdout = ""
    stderr = ""
    for n in range(1, total_cpu+1):
        child_cpu = str(n)
        total_cpu = str(total_cpu)
        n_cmd = cmd + ["-PARALLEL", total_cpu,
                       "-CHILD", child_cpu]
        pipe = subprocess.run(n_cmd,
                              stdout=subprocess.PIPE, 
                              stderr=subprocess.PIPE, 
                              cwd=binary_folder)
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


def upload_tpc_data_DUP(folder, bucket_name, limit=2000, verbose=False):
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

def copy_tpl(verbose=False):
    """Move query templates and make copies for modification """
    old_dir = config.fp_ds_src + config.sep + "query_templates"
    if verbose:
        print("Source directory:", old_dir)
        
    # ANSI templates, as generated, for references
    new_dir = config.fp_ds_ansi_gen_template_dir
    if not os.path.exists(new_dir):
        os.mkdir(new_dir)
        tools.copy_recursive(old_dir, new_dir)
        if verbose:
            print("Moved all files to:", new_dir)
            
    # BigQuery templates, as generated, for references
    new_dir = config.fp_ds_bq_gen_template_dir
    if not os.path.exists(new_dir):
        os.mkdir(new_dir)
        tools.copy_recursive(old_dir, new_dir)
        if verbose:
            print("Moved all files to:", new_dir)
            
    # Snowflake templates, as generated, for references
    new_dir = config.fp_ds_sf_gen_template_dir
    if not os.path.exists(new_dir):
        os.mkdir(new_dir)
        tools.copy_recursive(old_dir, new_dir)
        if verbose:
            print("Moved all files to:", new_dir)
            
   # BigQuery templates, to be edited by hand before query generation
    new_dir = config.fp_ds_bq_template_dir
    if not os.path.exists(new_dir):
        os.mkdir(new_dir)
        tools.copy_recursive(old_dir, new_dir)
        if verbose:
            print("Moved all files to:", new_dir)
            
    # Snowflake templates, to be edited by hand before query generation
    new_dir = config.fp_ds_sf_template_dir
    if not os.path.exists(new_dir):
        os.mkdir(new_dir)
        tools.copy_recursive(old_dir, new_dir)
        if verbose:
            print("Moved all files to:", new_dir)
    if verbose:
        print("Done.  Note: If none printed above, there were no new templates to write.")

def sqlserver_defines(template_root):
    """Edit the sqlserver.tpl file such that it will compile ANSI SQL"""
    
    tpl = template_root + config.sep + "sqlserver.tpl"
    
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

            
def sqlserver_bq_defines(template_root):
    """Edit the sqlserver.tpl so that dsqgen will generate *more* 
    compilable SQL for BigQuery - hand modification might still be
    required
    
    Parameters
    ----------
    template_root : str, absolute path to 
    """
    dialect = "sqlserver_bq"
    tpl = template_root + config.sep + dialect + ".tpl"
    
    defines = '''define __LIMITA = "";
define __LIMITB = "";
define __LIMITC = "limit %d";
define _END = "";
'''
    with open(tpl, "w") as f:
        f.write(defines)
        
    return dialect

    
def dsqgen(file=None, 
           verbose=None, 
           help=None,
           output_dir=None,
           quiet=None,
           streams=None,
           input=None,
           scale=None,
           log=None,
           qualify=None,
           distributions=None,
           path_sep=None,
           rngseed=None,
           release=None,
           template=None,
           count=None,
           debug=None,
           filter=None,
           dialect=None,
           directory=None,
           ):
    """
    Run TPC-DS query generator.  Keyword arguments are converted to command line options
    as available directly from the program on the terminal, see below for definitions
    which are output from 
    
    $ ./dsqgen -HELP Y
    
    General Options
    ===============
    FILE =  <s>              -- read parameters from file <s>
    VERBOSE =  [Y|N]         -- enable verbose output
    HELP =  [Y|N]            -- display this message
    OUTPUT_DIR =  <s>        -- write query streams into directory <s>
    QUIET =  [Y|N]           -- suppress all output (for scripting)
    STREAMS =  <n>           -- generate <n> query streams/versions
    INPUT =  <s>             -- read template names from <s>
    SCALE =  <n>             -- assume a database of <n> GB
    LOG =  <s>               -- write parameter log to <s>
    QUALIFY =  [Y|N]         -- generate qualification queries in ascending order

    Advanced Options
    ===============
    DISTRIBUTIONS =  <s>     -- read distributions from file <s>
    PATH_SEP =  <s>          -- use <s> to separate path elements
    RNGSEED =  <n>           -- seed the RNG with <n>
    RELEASE =  [Y|N]         -- display QGEN release info
    TEMPLATE =  <s>          -- build queries from template <s> ONLY
    COUNT =  <n>             -- generate <n> versions per stream (used with TEMPLATE)
    DEBUG =  [Y|N]           -- minor debugging outptut
    FILTER =  [Y|N]          -- write generated queries to stdout
    DIALECT =  <s>           -- include query dialect defintions found in <s>.tpl
    DIRECTORY =  <s>         -- look in <s> for templates
    """
    
    kwargs = []
    
    if file is not None:
        kwargs.append("-FILE")
        kwargs.append(file)
        
    if verbose is not None:
        kwargs.append("-VERBOSE")
        kwargs.append(verbose)
        
    if help is not None:
        kwargs.append("-HELP")
        #kwargs.append("Y")
        
    if output_dir is not None:
        kwargs.append("-OUTPUT_DIR")
        kwargs.append(output_dir)
    
    if quiet is not None:
        kwargs.append("-QUIET")
        kwargs.append(quiet)
        
    if streams is not None:
        kwargs.append("-STREAM")
        kwargs.append(str(streams))
        
    if input is not None:
        kwargs.append("-INPUT")
        kwargs.append(input)
        
    if scale is not None:
        kwargs.append("-SCALE")
        kwargs.append(str(scale))
        
    if log is not None:
        kwargs.append("-LOG")
        kwargs.append(log)
        
    if qualify is not None:
        kwargs.append("-QUALIFY")
        kwargs.append(qualify)
        
    if distributions is not None:
        kwargs.append("-DISTRIBUTIONS")
        kwargs.append(distributions)
        
    if path_sep is not None:
        kwargs.append("-PATH_SEP")
        kwargs.append(path_sep)
        
    if rngseed is not None:
        kwargs.append("-RNGSEED")
        kwargs.append(rngseed)
        
    if release is not None:
        kwargs.append("-RELEASE")
        kwargs.append(release)
        
    if template is not None:
        kwargs.append("-TEMPLATE")
        kwargs.append(str(template))
        
    if count is not None:
        kwargs.append(-"COUNT")
        kwargs.append(count)
        
    if debug is not None:
        kwargs.append("-DEBUG")
        kwargs.append(debug)
        
    if filter is not None:
        kwargs.append("-FILTER")
        kwargs.append(filter)
        
    if dialect is not None:
        kwargs.append("-DIALECT")
        kwargs.append(dialect)
        
    if directory is not None:
        kwargs.append("-DIRECTORY")
        kwargs.append(directory)
        
    fp = config.fp_ds_src + config.sep + "tools"

    env_vars = dict(os.environ)
    #env_vars["DSS_PATH"] = config.fp_h_data_out + config.sep + str(scale) + "GB"
    #env_vars["DSS_QUERY"] = fp + config.sep + "queries"

    cmd = ["./dsqgen"]
    
    cmd = cmd + kwargs
    
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
    
    #std_out = std_out.split("\n")
    #std_out_new = []
    #keep = False
    #for line in std_out:
    #    line = line.rstrip("\r")
    #    line = line.rstrip("\n")
    #    if line == "select":
    #        keep = True
    #    if keep:
    #        std_out_new.append(line)
    #std_out = std_out_new
    #std_out = "\n".join(std_out)
    return std_out, err_out


def tpl_bq_regex_OLD(text):
    # note that leading and trailing whitespace is used to find only table datatype strings
    dtype_mapper = {#r' UNION': r' UNION ALL',
                    #r' AS DECIMAL\(\d+,\d+\)\)': r'',
                    #r'CAST\(': r'',
                    r' union': r' union all',
                    r' as decimal\(\d+,\d+\)\)': r'',
                    r'cast\(': r''
                    }
    
    for k, v in dtype_mapper.items():
        regex = re.compile(k)
        text = regex.sub(v, text)
    
    return text

def tpl_bq_regex_file_OLD(filepath_in, filepath_out):
    """Apply """
    text = open(filepath_in).read()
    
    text = tpl_bq_regex(text)
    
    open(filepath_out, "w").write(text)

def tpl_bq_regex_dir_OLD(tpl_dir):
    """Alter all query templates in a directory"""
    files = glob.glob(tpl_dir + config.sep + "query*.tpl")
    for fp in files:
        ds_setup.tpl_bq_regex_file(fp, fp)
        file_name = os.path.basename(fp)
        print(file_name)
        
def tpl_bq_regex(tpl_dir, verbose=False):
    dtype_mapper = {r' UNION\n': r' UNION ALL\n',
                    r' union\n': r' union all\n',
                    r' AS DECIMAL\(\d+,\d+\)': r' AS FLOAT64',
                    r' as decimal\(\d+,\d+\)': r' as float64',
                    #r' AS DECIMAL\(\d+,\d+\)\)': r'',
                    #r'CAST\(': r'',
                    #r' as decimal\(\d+,\d+\)\)': r'',
                    #r'cast\(': r'',
                    #r' as decimal\(\d+,\d+\) ': r''
                    }
    
    tools.regex_dir(filepath_dir=tpl_dir,
                    file_signature="query*.tpl",
                    replace_mapper=dtype_mapper,
                    verbose=verbose)
    
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

        _data_out = config.fp_ds_data_out + config.sep + str(self.scale) + "GB"

        cmd = ["./dsdgen", "-DIR", _data_out, "-SCALE", str(self.scale),
               "-DELIMITER", "|", "-TERMINATE", "N"]

        if self.seed is not None:
            cmd = cmd + ["-RNGSEED", str(self.seed)]

        total_cpu = config.cpu_count
        binary_folder = config.fp_ds_src + config.sep + "tools"
        pipe_outputs = []
        stdout = ""
        stderr = ""
        
        child = str(child)
        parallel = str(parallel)
        
        n_cmd = cmd + ["-PARALLEL", parallel,
                       "-CHILD", child]
        
        pipe = subprocess.run(n_cmd,
                              stdout=subprocess.PIPE, 
                              stderr=subprocess.PIPE, 
                              cwd=binary_folder)
        
        stdout = pipe.stdout.decode("utf-8")
        stderr = pipe.stderr.decode("utf-8")

        if self.verbose:
            if len(stdout) > 0:
                print(stdout)
            if len(stderr) > 0:
                print(stderr)
        with self.lock:
            self.results.append([child, parallel, stdout, stderr])
        return child
    
    def generate(self):
        with concurrent.futures.ThreadPoolExecutor(max_workers=self.n) as executor:
            results = executor.map(self.run, self.child, self.parallel)
        return results