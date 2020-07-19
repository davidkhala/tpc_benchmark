"""SQL Schema Definitions

MIT License, see LICENSE file for complete text.
Copyright (c) 2020 SADA Systems, Inc.
"""

import re
import shutil

import config


def copy_ds_ansi(filepath_out):
    """Make a copy and move the source ANSI schema file to have for
    reference in the filepath_out directory

    Parameters
    ----------
    filepath_out : str, absolute path to directory to move file to
    """
    original_file = config.ds_schema_ansi_sql_filepath
    shutil.copyfile(original_file, filepath_out + config.sep + "ansi_ds.sql")


def rewrite_ds_bq_basic(filepath_out, dataset_name, prefix=False):
    """Convert the sample implementation of the logical schema as described in TPC-DS Specification V1.0.0L ,
    specifications.pdf, pg 99, Appendix A and contained in  tpc_root/tools/tpcds.sql.

    Parameters
    ----------
    #filepath_in : str, path to tpcds.sql file
    filepath_out : str, path to write BigQuery formatted table schema, named 'tpcds_bq.sql'
    dataset_name : str, name of BigQuery Dataset to append to existing table names.
    prefix : bool, if True dataset_name prefix is added to table names, otherwise table names
        are left as is.

    Returns
    -------
    None, only writes to file
    """

    # note that leading and trailing whitespace is used to find only table datatype strings
    dtype_mapper = {r'  decimal\(\d+,\d+\)  ': r'  FLOAT64  ',
                    r'  varchar\(\d+\)  ':     r'  STRING  ',
                    r'  char\(\d+\)  ':        r'  STRING  ',
                    r'  integer  ':            r'  INT64  ',
                    # the following are just to have consistent UPPERCASE formatting
                    r'  time  ':               r'  TIME  ',
                    r'  date  ':               r'  DATE  '
                    }

    # read in the base ANSI SQL schema generated by dsdgen
    text = open(config.ds_schema_ansi_sql_filepath).read()

    for k, v in dtype_mapper.items():
        regex = re.compile(k)
        text = regex.sub(v, text)

    text_list_in = text.split("\n")
    text_list_out = []

    for line in text_list_in:
        if "primary key" in line:
            continue
        if ("create table" in line) & prefix:
            split_line = line.split()
            table_name = split_line[2]
            new_line = split_line[:2] + [dataset_name + "." + table_name]
            new_line = " ".join(new_line)
            text_list_out.append(new_line)
        else:
            text_list_out.append(line)

    text = "\n".join(text_list_out)

    open(filepath_out, "w").write(text)


def copy_h_ansi(filepath_out):
    """Make a copy and move the source ANSI schema file to have for 
    reference in the filepath_out directory
    
    Parameters
    ----------
    filepath_out : str, absolute path to directory to move file to
    """
    original_file = config.h_schema_ddl_filepath
    shutil.copyfile(original_file, filepath_out + config.sep + "ansi_h.sql")


def rewrite_h_basic(filepath_out, dataset_name, 
                    lower=False, prefix=False):
    """Convert the sample implementation of the logical schema as described in TPC-DS Specification V1.0.0L , specifications.pdf, pg 14, and contained in  tpc_root/dbgen/dss.ddl.

    Parameters
    ----------
    filepath_out : str, path to write BigQuery formatted table schema, named 'tpch_bq.ddl'
    dataset_name : str, name of BigQuery Dataset to append to existing table names
    lower : bool, convert all text in dss.ddl to lowercase
    prefix : bool, if True prepend dataset_name to table names

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

    # read in the base ANSI SQL schema generated by qgen
    text = open(config.h_schema_ddl_filepath).read()

    for k, v in dtype_mapper.items():
        regex = re.compile(k)
        text = regex.sub(v, text)

    text_list_in = text.split("\n")
    text_list_out = []

    for line in text_list_in:
        # if "primary key" in line:
        #    continue

        if ("CREATE TABLE" in line) & prefix:
            split_line = line.split()  # split on whitespace of n length
            table_name = split_line[2]
            dataset_table_name = dataset_name + "." + table_name
            split_line[2] = dataset_table_name
            new_line = " ".join(split_line)
            if lower:
                new_line = new_line.lower()
            text_list_out.append(new_line)
        else:
            if lower:
                line = line.lower()
            text_list_out.append(line)

    text = "\n".join(text_list_out)

    open(filepath_out, "w").write(text)
