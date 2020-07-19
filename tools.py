"""Generic tools for setup, EDA and debugging

MIT License, see LICENSE file for complete text.
Copyright (c) 2020 SADA Systems, Inc.
"""

import os
import re
import math
import zipfile
import shutil
import glob
import numpy as np

import pandas as pd

import config


def make_directories():
    """Make local directories for TPC DS & H tests"""
    fp_list = [
        config.fp_download,
        config.fp_ds, 
        config.fp_h, 
        config.fp_ds_output,
        config.fp_h_output,
        config.fp_results,
        config.fp_schema,
        config.fp_query,
        config.fp_results
        ]
    
    fp_list += [config.fp_h_output +
                config.sep + str(i) + "GB" for i in config.scale_factors]
    
    fp_list += [config.fp_ds_output +
                config.sep + str(i) + "GB" for i in config.scale_factors]
    
    # query files
    fp_list += [config.fp_query]
    
    # only generate the folder if it doesn't exist
    for fp in fp_list:
        if not os.path.exists(fp):
            print("making directory:", fp)
            os.mkdir(fp)
    
    # externally mounted persistent disk output
    if os.path.exists(config.fp_output_mnt):
        fp_list_2 = [config.fp_ds_output_mnt,
                           config.fp_h_output_mnt]

        fp_list_2 += [config.fp_h_output_mnt +
                      config.sep + str(i) + "GB" for i in config.scale_factors]

        fp_list_2 += [config.fp_ds_output_mnt +
                      config.sep + str(i) + "GB" for i in config.scale_factors]
        
        # only generate the folder if it doesn't exist
        for fp in fp_list_2:
            if not os.path.exists(fp):
                print("Making data directories on the persistent disk:")
                os.mkdir(fp)


def mkdir_safe(fp):
    """Make a directory only if it does not currently exist"""
    if not os.path.exists(fp):
        os.mkdir(fp)


def extract_zip(zip_filepath, target):
    """Extract downloaded TPC-DS test .zip to a standard location as set
    in config.py

    Note: it may be better for repeatability to do this step manually

    Parameters
    ----------
    zip_filepath : str, file location of .zip file
    target : str, directory to expand files to
    """
    with zipfile.ZipFile(zip_filepath) as z:
        z.extractall(target)


def extract_table_name(f_name):
    """Extract the table name target for a TPC-DS data file
    
    Parameters
    ----------
    f_name : str, name of file as generated with dsdgen
    
    Returns
    -------
    table_name : str, name of table the file's data should be
        loaded in
    """
    f_name = f_name.split(".")[0]
    f_list = f_name.split("_")
    f_list_new = []
    for x in f_list:
        try:
            int(x)
        except ValueError:
            f_list_new.append(x)
    return "_".join(f_list_new)


def pathlist(directory_path, pattern="*"):
    """Get all files in a directory, non-recursively"""
    from pathlib import Path
    return [str(fp) for fp in Path(directory_path).rglob(pattern)]


def pathlist_recursive(directory_path, extension="*"):
    """Get all files in a directory, recursively"""
    import glob
    files = glob.glob(directory_path + '/**/*.' + extension, recursive=True)
    return files


def line_counter(filepath, verbose=False):
    """Count the number of lines in a TPC-DS dsdgen created data file, 
    as well as the number reported in the file
    
    Parameters
    ----------
    filepath : str, filepath to dsdgen file
    verbose : bool, print debug statements
    
    Returns
    -------
    count : int, lines actually counted in file
    count_read : int, line indexes read from the first
        and last line of the file as:
        line_n - line_0 + 1
    """
    with open(filepath, "br") as f:
        try:
            x0 = f.readline().decode("utf-8").split("|")[0]
        except:
            print()
        count = 1
        for line in f:
            count += 1
        xn = line.split(b"|")[0]
    
    count_read = int(xn) - int(x0) + 1
    
    if verbose:    
        print("File: {}".format(filepath))
        print("Seen Count: {}".format(count))
        print("Read Count: {}".format(count_read))
    
    return count, count_read


def file_inventory(directory):
    files = pathlist_recursive(directory)
    inv = []
    for f in files:
        if "dbgen_version" in f:
            continue
        f_basename = os.path.basename(f)
        f_size = os.path.getsize(f) / 1000000        
        f_table_name = extract_table_name(f_basename)
        f_count, f_count_read = line_counter(f)
        inv.append([f_basename, f_size, f_table_name, f_count, f_count_read, f])
    return inv


def print_inventory(directory):
    """Print the results of a directory inventory
    Note: only uses the 0 and 1 indexes of the output from
    file_inventory()
    
    """
    inv = file_inventory(directory)
    l_max = 0
    size_count = 0
    width_max = 0
    rows = []
    
    for i in inv:
        l_max = max(l_max, len(i[0]))
    
    for i in inv:
        size_count += i[1]
        f_basename = i[0]
        f_size = i[1]
        f_count = str(i[3])
        f_count_read = str(i[4])
        f_size = "{:.3f}".format(f_size)
        line = "{} {} MB {}".format(f_basename.ljust(l_max+1), 
                                    f_size.rjust(12),
                                    f_count.rjust(14))
        width_max = max(width_max, len(line))
        rows.append(line)
    
    print("="*width_max)
    print(directory)
    print("="*width_max)

    print("{} {}    {}".format("File".ljust(l_max+1),
                                "Size".rjust(12),
                                "Line Count".rjust(14)))
    print("="*width_max)

    for r in rows:
        print(r)

    print("="*width_max)
    units = "MB"
    
    if size_count > 1000000:
        units = "TB"
        size_count /= 1000000
    
    elif size_count > 1000: 
        units = "GB"
        size_count /= 1000
        
    size_count = "{:.3f}".format(size_count)
    print("{} {} {}".format("Total Size:".ljust(l_max+1), 
                            size_count.rjust(12),
                            units))
    print("="*width_max)
    print()


def move_recursive(old_dir, new_dir):
    """Move all files in a directory to another
    
    Parameters
    ----------
    old_dir : str, directory to move FROM
    new_dir : str, directory to move TO
    """
    
    files = os.listdir(old_dir)

    for f in files:
        shutil.move(old_dir + config.sep + f, new_dir + config.sep + f)


def copy_recursive(old_dir, new_dir):
    """Copy all files in a directory to another
    
    Parameters
    ----------
    old_dir : str, directory to move FROM
    new_dir : str, directory to move TO
    """
    
    files = os.listdir(old_dir)

    for f in files:
        shutil.copyfile(old_dir + config.sep + f, new_dir + config.sep + f)

    
def regex_replace(text, replace_mapper):
    """Replace characters in text using regex
    
    Parameters
    ----------
    text : str, text to replace characters
    replace_mapper : dict, keys = old text, values = new replacement text
    
    Returns
    -------
    str, text replaced
    """
    
    for k, v in replace_mapper.items():
        regex = re.compile(k)
        text = regex.sub(v, text)
    
    return text


def regex_file(filepath_in, filepath_out, replace_mapper):
    """Apply regex_replace to a file.  If filepath_in == filepath_out,
    replaces the file's contents.
    
    Parameters
    ----------
    filepath_in : str, absolute filepath to file to read text
    filepath_out : str, absolute filepath to file write replacement text
    replace_mapper : dict, keys = old text, values = new replacement text
    
    Returns
    -------
    None, operates on files
    """
    text = open(filepath_in).read()
    
    text = regex_replace(text, replace_mapper)
    
    open(filepath_out, "w").write(text)


def regex_dir(filepath_dir, file_signature, replace_mapper, verbose=False):
    """Alter all query templates in a directory"""
    
    """Apply regex_replace to a file.  If filepath_in == filepath_out,
    replaces the file's contents.
    
    Parameters
    ----------
    filepath_dir : str, absolute filepath to folder to read text files
    file_signature : str, file selection, for glob.glob input
    replace_mapper : dict, keys = old text, values = new replacement text
    
    Returns
    -------
    None, operates on files in directory
    """
    
    files = glob.glob(filepath_dir + config.sep + file_signature)
    for fp in files:
        regex_file(fp, fp, replace_mapper)
        file_name = os.path.basename(fp)
        if verbose:
            print(file_name)


def parse_h_stream_seq():
    """Parse the TPC-H query stream ordering data, as listed in 
    Appendix A of the specification
    
    Returns
    -------
    Pandas DataFrame, where:
        rows = query stream number
        columns = query number to execute
    """
    fp = config.fp_h_stream_order
    c = 0
    d_str = []
    skip = [0, 1, 3]
    with open(fp, "r") as f:
        for line in f:
            if c not in skip:
                d_str.append(line)
            c += 1
    d = []
    for line in d_str:
        d.append(line.strip().split("\t"))
    _df = pd.DataFrame(d, columns=["stream"]+list(range(1, 23)))
    _df.drop("stream", axis=1, inplace=True)
    return _df


def parse_ds_seq_stream():
    """Parse the TPC-DS query ordering data, as listed in 
    Appendix D of the specification
    
    Returns
    -------
    Pandas DataFrame, where:
        rows = query stream number
        columns = query number to execute
    """
    fp = config.fp_ds_stream_order
    _df = pd.read_csv(fp, skiprows=2, names=["seq"]+list(range(0, 21)))
    _df.drop("seq", axis=1, inplace=True)
    _df = _df.transpose()
    return _df


def tpc_stream(test, n):
    """Generate the correct TPC query stream sequence

    Parameters
    ----------
    test : str, TPC test name, either 'ds' or 'h'
    n : int, query stream number for the test, 1-99 for ds, 1-22 for h
    
    Returns
    -------
    list of str, query numbers to execute in order index 0 to index -1
    """
    
    assert test in ["ds", "h"], "Must be valid TPC test name"
    
    if test == "h":
        _df = parse_h_stream_seq()
    if test == "ds":
        _df = parse_ds_seq_stream()
    query_sequence = [int(str(v)) for v in _df.loc[n].values]
    return list(query_sequence)


def make_name(db, test, cid, kind, datasource, desc, ext, timestamp=None):
    """Make a name for query results to be saved.  If parameters
    'ext' is set to blank, '', can be used to name folders.

    Parameters
    ----------
    db : str, data base name, either 'bq' or 'sf'
    test : str, test being done, either 'h' or 'ds'
    cid : str, config id, i.e. '02A' for the experiment config number
    kind : str, kind of record, either 'results' or 'times'
    datasource : str, dataset if bq or database if snowflake
    desc : str, description of experiment self.desc
    ext : str, extension including '.' i.e. '.csv'
    timestamp : pandas Timestamp object

    Returns
    folder : str, folder name to save data to
    file : str, file name to save timing data to

    # TODO: remove test, cid references
    """
    if timestamp is None:
        timestamp = pd.Timestamp.now("UTC")
        timestamp = str(timestamp).replace(" ", "_")

    folder = (f'{config.fp_results}{config.sep}' +
              f'{kind}_{db}_{datasource}_' +
              f'{desc}_{str(timestamp)}')
    file =   (f'benchmark_' +
              f'{kind}_{db}_{datasource}_' +
              f'{desc}_{str(timestamp)}{ext}')
    #file =  (f'benchmark_result_{db}_{test}_{cid}_{kind}-' +
    #         f'{datasource}-{desc}-{timestamp}{ext}')
    return folder, file


def to_numeric(df):
    """Convert columns to numeric types if possible

    Parameters
    ----------
    df : Pandas DataFrame

    Returns
    -------
    df : Pandas DataFrame
    """
    for col in df.columns:
        try:
            df[col] = pd.to_numeric(df[col])
        except:
            pass
    return df


def truncate_old(x, n):
    """Truncate a float to a certain number of decimal places
    
    Parameters
    ----------
    x : float, value to truncate
    n : int, number of decimal places to keep
    
    Returns
    -------
    float : truncated value
    """
    if pd.isnull(x):
        return np.nan
    else:
        return math.trunc(x * math.pow(10, n)) / math.pow(10, n)


def truncate(s, n):
    """Vectorized version of float truncation"""
    s = s * np.power(10, n)
    s = s.astype(int)
    s = s / np.power(10, n)
    return s


def to_truncated(df, n=None):
    """Truncate all float values in DataFrame
    
    Parameters
    ----------
    df : Pandas Dataframe
    n : int, number of decimal places to keep
    
    Returns
    -------
    Pandas DataFrame with float values converted to int * 1000
    """
    for col in df.columns:
        try:
            df[col] = df[col].apply(lambda x: truncate(x, n))
        except ValueError:
            pass
    return df


def to_str(df, n):
    dec_str = "{:." + str(n) + "f}"
    for col in df.columns:
        try:
            df[col] = df[col].apply(lambda x: dec_str.format(x))
        except ValueError:
            pass
    return df


def to_consistent(df, n):
    """Convert Pandas DataFrame to consistent representation

    Parameters
    ----------
    df : Pandas DataFrame
    n : int, number of decimal places to truncate float to

    Returns
    -------
    df : Pandas DataFrame
    """
    if n is None:
        n = config.float_precision

    df.columns = map(str.lower, df.columns)
    drop_columns = ["lochierarchy"]
    df = df[[c for c in df.columns if c not in drop_columns]].copy()
    df = to_numeric(df)
    df = df.sort_values(by=list(df.columns), na_position='last').reset_index(drop=True)
    df.fillna(value=-9999.99, inplace=True)
    #df = to_truncated(df=df, n=config.float_precision)
    df = to_str(df=df, n=n)
    return df
