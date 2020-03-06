"""Prepare TPC-DS tests for BigQuery

Colin Dietrich, SADA, 2020

## Data Types
From TPC-DS Specifications, V1.0.0L, specifications.pdf, pg 21:

2.2.2.1 Each column employs one of the following datatypes:

a) Identifier means that the column shall be able to hold any key value generated for that column.

b) Integer means that the column shall be able to exactly represent integer values (i.e., values in increments of 1) in the range of at least ( − 2 n − 1 ) to (2 n − 1 − 1), where n is 64.

c) Decimal(d, f) means that the column shall be able to represent decimal values up to and including d digits, of which f shall occur to the right of the decimal place; the values can be either represented exactly or interpreted to be in this range.

d) Char(N) means that the column shall be able to hold any string of characters of a fixed length of N.

Comment: If the string that a column of datatype char(N) holds is shorter than N characters, then trailing spaces shall be stored in the database or the database shall automatically pad with spaces upon retrieval such that a CHAR_LENGTH() function will return N.

e) Varchar(N) means that the column shall be able to hold any string of characters of a variable length with a maximum length of N. Columns defined as "varchar(N)" may optionally be implemented as "char(N)".

f) Date means that the column shall be able to express any calendar day between January 1, 1900 and December 31, 2199.

2.2.2.2
The datatypes do not correspond to any specific SQL-standard datatype. The definitions are provided to highlight the properties that are required for a particular column. The benchmark implementer may employ any internal representation or SQL datatype that meets those requirements.

## Implementation  
Based on the above definitions, the following datatype definitions mapping was used.  Note `time` and `date` are converted to UPPER for code formatting consistency, no performance difference is intended.  

| TPC-DS ANSI SQL | BigQuery SQL |
| --------------- | ------------ |
| decimal         | FLOAT64      |  
| integer         | INT64        |  
| varchar(N)      | STRING       |  
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


dtype_mapper = {r'  decimal\(\d+,\d+\)  ': r'  FLOAT64  ',
                r'  varchar\(\d+\)  ':     r'  STRING  ',
                r'  char\(\d+\)  ':        r'  STRING  ',
                r'  integer  ':            r'  INT64  ',
                # the following are just to have consistent UPPERCASE formatting
                r'  time  ':               r'  TIME  ',
                r'  date  ':               r'  DATE  '
               }



def schema(filepath_in, filepath_out, dataset_name):
    """Convert the sample implementation of the logical schema as described in TPC-DS Specification V1.0.0L , specifications.pdf, pg 99, Appendix A and contained in  tpc_rool/tools/tpcds.sql.
    
    Parameters
    ----------
    file_path_in : str, path to tpcds.sql file
    file_path_out : str, path to write BigQuery formatted table schema, named 'tpcds_bq.sql'
    dataset_name : str, name of BigQuery Dataset to append to existing table names
    
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
    
    text = open(filepath_in).read()
    
    for k, v in dtype_mapper.items():
        regex = re.compile(k)
        text = regex.sub(v, text)

    text_list_in = text.split("\n")
    text_list_out = []
    
    for line in text_list_in:
        if "primary key" in line:
            continue
        if "create table" in line:
            split_line = line.split()
            table_name = split_line[2]
            new_line = split_line[:2] + [dataset_name + "." + table_name]
            new_line = " ".join(new_line)
            text_list_out.append(new_line)
        else:
            text_list_out.append(line)
    
    text = "\n".join(text_list_out)
    
    open(filepath_out, "w").write(text)
    
def create_dataset(verbose=False):
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

def apply_schema(verbose=False):
    """Apply the schema .sql file as reformatted from 
    config.tpcds_schema_ansi_sql_filepath
    to 
    config.tpcds_schema_bq_filepath
    using schema() method in this module.
    """
    
    