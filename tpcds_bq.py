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

import config, tools

"""
dtype_mapper = {r'  decimal\(\d+,\d+\)  ': r'  FLOAT64  ',
                r'  varchar\(\d+\)  ':     r'  STRING  ',
                r'  char\(\d+\)  ':        r'  STRING  ',
                r'  integer  ':            r'  INT64  ',
                # the following are just to have consistent UPPERCASE formatting
                r'  time  ':               r'  TIME  ',
                r'  date  ':               r'  DATE  '
               }
"""


def rewrite_schema(filepath_in, filepath_out, dataset_name):
    """Convert the sample implementation of the logical schema as described in TPC-DS Specification V1.0.0L , specifications.pdf, pg 99, Appendix A and contained in  tpc_root/tools/tpcds.sql.
    
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
    
def create_dataset_old(verbose=False):
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

def create_dataset(verbose=False):
    return bq.create_dataset(verbose=verbose)

def create_schema(verbose=False):
    """Apply the schema .sql file as reformatted from 
    config.tpcds_schema_ansi_sql_filepath
    to 
    config.tpcds_schema_bq_filepath
    using schema() method in this module.
    """
    client = bigquery.Client.from_service_account_json(config.gcp_cred_file)
    with open(config.tpcds_schema_bq_filepath, 'r') as f:
        query_txt = f.read()
        
    query_job = client.query(query_txt)  # API request
    rows = query_job.result()  # Waits for query to finish
    
    if verbose:
        for r in rows:
            print(r.name)

def table_size(client, project, dataset, table, verbose=False):
    """Apply the schema .sql file as reformatted from 
    config.tpcds_schema_ansi_sql_filepath
    to 
    config.tpcds_schema_bq_filepath
    using schema() method in this module.
    
    Returns
    -------
    size of table in bytes
    """
    
    client = bigquery.Client.from_service_account_json(config.gcp_cred_file)
    
    query_txt = """
    select 
      sum(size_bytes) as size
    from
      {}.{}.__TABLES__
    where 
      table_id = '{}'
        """.format(project, dataset, table)
        
    query_job = client.query(query_txt)  # API request
    rows = query_job.result()  # Waits for query to finish
    _df = rows.to_dataframe()
    size = _df.loc[0, "size"]
    return size
            
def extract_table_name(f_name):
    """Extract the table name target for a TPC-DS data file
    
    Parameters
    ----------
    fname : str, name of file as generated with dsdgen
    
    Returns
    -------
    table_name : str, name of table the file's data should be
        loaded in
    """
    f_name = f_name.split(config.sep)[-1]
    f_name = f_name.split(".")[0]
    f_list = f_name.split("_")
    f_list_new = []
    for x in f_list:
        try:
            int(x)
        except:
            f_list_new.append(x)
    return "_".join(f_list_new)
    
def upload_all_local(directory, dataset, validate=True, verbose=False):
    
    files = tools.file_inventory(directory)
    
    client = bigquery.Client.from_service_account_json(config.gcp_cred_file)
    
    b = BQUpload(client=client,
                 project=config.gcp_project,
                 dataset=dataset)
    
    for f in files:
        table = f[2]
        filepath = f[5]
        load_job = b.upload_local_csv(table=table, filepath=filepath, verbose=verbose)
        
    df = validate(directory=folder, dataset=dataset)
    return df
    
def validate(directory, dataset, byte_multiplier=1):
    dir_files = tools.file_inventory(directory)
    table_names = set([f[2] for f in dir_files])
    table_sizes = {f[2]:f[1] for f in dir_files}
    
    client = bigquery.Client.from_service_account_json(config.gcp_cred_file)
    
    
    b = BQUpload(client=client,
                 project=config.gcp_project,
                 dataset=config.gcp_dataset)
    table_attrs = b.get_all_table_attributes()
    
    data = []
    for t in table_names:
        # count rows across all files for this table
        local_rows = 0
        for f in dir_files:
            if f[2] == t:
                local_rows += int(f[3])
        local_size = table_sizes[t]
        bq_size, bq_rows = table_attrs[t]
        bq_size = bq_size / 10**6
        data.append([t, local_size, local_rows, bq_size, bq_rows])
        
    df = pd.DataFrame(date, columns=["table", 
                                     "local_size", "local_rows",
                                     "bq_size", "bq_rows"])
    df["bq_percent"] = (df.bq_rows / df.local_rows) * 100
    return df

class BQUpload:
    """Upload CSV data from a file location"""
    def __init__(self, client, project, dataset):
        """
        Parameters
        ----------
        client : GCP storage client instance
        project : str, GCP project name
        dataset : str, BigQuery dataset name
        """
        self.client = client
        self.project = project
        self.dataset = dataset
        
        self.job_config = None
        
        self.tables = None
        
        # apply CSV specific setup
        # values can be altered as parameters of self.job_config
        self.setup()
        
    def setup(self):
        """General setup for CSV upload"""
        
        # https://googleapis.dev/python/bigquery/latest/generated/google.cloud.bigquery.job.LoadJob.html#google.cloud.bigquery.job.LoadJob
        self.job_config = bigquery.LoadJobConfig()

        # https://googleapis.dev/python/bigquery/latest/generated/google.cloud.bigquery.job.WriteDisposition.html#google.cloud.bigquery.job.WriteDisposition
        self.job_config.write_disposition = bigquery.WriteDisposition.WRITE_APPEND

        # Number of rows to skip when reading data (CSV only)
        self.job_config.skip_leading_rows = 0

        # The separator for fields in a CSV file
        self.job_config.field_delimiter = "|"

        # The character encoding of the data
        # default is utf-8 so this does not need to be set
        # the other option is ISO-8859-1
        #job_config.encoding = "UTF-8"
        
        # The source format defaults to CSV, so the line below is optional.
        self.job_config.source_format = bigquery.SourceFormat.CSV

        self.get_all_table_ids()
        
    def get_all_table_ids(self):
        _itter = self.client.list_tables(dataset=self.dataset)
        self.tables = [_t.table_id for _t in _itter]
        
    def get_table(self, table_id):
        full_table_id = (self.project + "." +
                         self.dataset + "." +
                         table_id)
        return self.client.get_table(full_table_id)
        
    def get_all_table_attributes(self):
        """Get the size of all tables in the project dataset
        Returns
        -------
        dict : key = str, table name
               value = int, size in bytes of table
        """
        self.get_all_table_ids()
        
        _sizes = {}
        for t in self.tables:
            _s = table_size(client=self.client, 
                            project=config.gcp_project, 
                            dataset=config.gcp_dataset,
                            table=t)
            _t = self.get_table(table_id=t)
            _r = _t.num_rows
            _sizes[t] = (_s, _r)
        return _sizes
        
    def upload_local_csv(self, table, filepath, delimiter="|", verbose=False):
        """Upload a CSV file to BigQuery

        https://googleapis.dev/python/bigquery/latest/generated/google.cloud.bigquery.client.Client.html#google.cloud.bigquery.client.Client.load_table_from_file

        Parameters
        ----------
        table : str, table name to upload data to
        filepath : str, path to file to upload
        delimiter : str, delimiting character of CSV file
            default = "|"
            
        Returns
        -------
        google.cloud.bigquery.job.LoadJob
        """

        destination = ".".join([config.gcp_project, config.gcp_dataset, table])

        with open(filepath, "rb") as f_open:
            if verbose:
                print("Starting Upload...")
                print("From:", filepath)
                print("To:", destination)
            load_job = self.client.load_table_from_file(file_obj=f_open,
                                                        destination=destination,
                                                        job_config=self.job_config
                                                        )
            if verbose:
                print("Job Started: {}".format(load_job.job_id))
            load_job.result()  # Waits for table load to complete.
            if verbose:
                print("Job finished: {}".format(load_job.done()))
                
        return load_job

    def upload_uri_csv(self, table, gs_path, delimiter="|", verbose=False):
        """Upload a CSV file to BigQuery

        https://googleapis.dev/python/bigquery/latest/generated/google.cloud.bigquery.client.Client.html#google.cloud.bigquery.client.Client.load_table_from_uri

        Parameters
        ----------
        table : str, table name to upload data to
        filepath : str or sequence of str, single path to a GCS file 
            to upload, or a sequence of strings of the same
        delimiter : str, delimiting character of CSV file
            default = "|"
            
        Returns
        -------
        google.cloud.bigquery.job.LoadJob
        """
        
        destination = ".".join([config.gcp_project, config.gcp_dataset, table])
        
        with open(filepath, "rb") as f_open:
            load_job = client.load_table_from_uri(uri=gs_path,
                                                  destination=destination,
                                                  job_config=self.job_config
                                                  )
            if verbose:
                print("Starting job {}".format(load_job.job_id))
                load_job.result()  # Waits for table load to complete.
                print("Job finished.")
                
        return load_job