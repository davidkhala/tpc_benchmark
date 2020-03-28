"""Generic tools for setup, EDA and debugging

Colin Dietrich, SADA 2020
"""

import os
import zipfile

import config


def make_directories():
    """Make local directories for TPC DS & H tests"""
    filepath_list_1 = [
        config.fp_ds, config.fp_ds_output,
        config.fp_h, config.fp_h_output,
        config.fp_download
    ]

    filepath_list_1 += [config.fp_h_output +
                        config.sep + str(i) + "GB" for i in config.scale_factors]

    filepath_list_1 += [config.fp_ds_output +
                        config.sep + str(i) + "GB" for i in config.scale_factors]

    for fp in filepath_list_1:
        if not os.path.exists(fp):
            os.mkdir(fp)

    if os.path.exists(config.fp_output_mnt):
        filepath_list_2 = [config.fp_ds_output_mnt,
                           config.fp_h_output_mnt]

        filepath_list_2 += [config.fp_h_output_mnt +
                            config.sep + str(i) + "GB" for i in config.scale_factors]

        filepath_list_2 += [config.fp_ds_output_mnt +
                            config.sep + str(i) + "GB" for i in config.scale_factors]

        for fp in filepath_list_2:
            if not os.path.exists(fp):
                os.mkdir(fp)

    with open(config.fp_download + config.sep +
              "place_tpc_zip_here.txt", "w") as f:
        f.write("# Place TCP-DS and TPC-H zip files in this directory\n")
        f.write("# Then run ds_setup.py or h_setup.py\n")


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
