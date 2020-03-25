"""Prepare TPC-H data for BigQuery

Colin Dietrich, SADA, 2020

## Implementation  
Based on the definitions in ./dbgen/dss.ddl, the following datatype definitions mapping was used.  Note `time` and `date` are converted to UPPER for code formatting consistency, no performance difference is intended.

| TPC-DS ANSI SQL | BigQuery SQL |
| --------------- | ------------ |
| decimal         | FLOAT64      |  
| integer         | INT64        |  
| char(N)         | STRING       |  
| varchar(N)      | STRING       |
| time            | TIME         |  
| date            | DATE         |  

See
https://cloud.google.com/bigquery/docs/reference/standard-sql/data-types
for BigQuery datatype specifications in standard SQL

"""


import re
from google.cloud import bigquery

import config


def rewrite_schema_DUP(filepath_in, filepath_out, dataset_name):
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
                    r' VARCHAR\(\d+\)':     r' STRING',
                    r' CHAR\(\d+\)':        r' STRING',
                    r' INTEGER':            r' INT64',
                    # the following are just to have consistent UPPERCASE formatting
                    r' time':               r' TIME',
                    r' date':               r' DATE'
                    }
    
    text = open(filepath_in).read()
    
    for k, v in dtype_mapper.items():
        regex = re.compile(k)
        text = regex.sub(v, text)

    text_list_in = text.split("\n")
    text_list_out = []

    # return text_list_in

    for line in text_list_in:
        #if "primary key" in line:
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

def create_dataset_DUP(verbose=False):
    """Create a dataset on the project
    
    See:
    https://cloud.google.com/bigquery/docs/reference/standard-sql/data-definition-language#create_table_statement  
    https://googleapis.dev/python/bigquery/latest/generated/google.cloud.bigquery.client.Client.html#google.cloud.bigquery.client.Client.create_dataset  
    https://googleapis.dev/python/bigquery/latest/generated/google.cloud.bigquery.job.CopyJob.html#google.cloud.bigquery.job.CopyJob
    for details
    
    Parameters
    ----------
    verbose : bool, print debug statements
    
    Returns
    -------
    A new copy job instance
    """
    
    dataset_name = config.gcp_project + "." + config.gcp_dataset
    dataset = bigquery.Dataset(dataset_name)
    dataset.location = config.gcp_location
    
    client = bigquery.Client.from_service_account_json(config.gcp_cred_file)
    copy_job = client.create_dataset(dataset)
    if verbose:
        print("Created dataset {}.{}".format(client.project, dataset.dataset_id))    
    return copy_job

def create_schema_DUP(verbose=False):
    """Apply the schema .sql file as reformatted from 
    config.tpcds_schema_ansi_sql_filepath
    to 
    config.tpcds_schema_bq_filepath
    using schema() method in this module.
    """
    client = bigquery.Client.from_service_account_json(config.gcp_cred_file)
    with open(config.h_schema_bq_filepath, 'r') as f:
        query_txt = f.read()
        
    query_job = client.query(query_txt)  # API request
    rows = query_job.result()  # Waits for query to finish
    
    if verbose:
        for r in rows:
            print(r.name)
            
    