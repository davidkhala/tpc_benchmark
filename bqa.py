"""Prepare TPC-DS tests for BigQuery

Colin Dietrich, SADA, 2020

## Data Types
From TPC-DS Specifications, V1.0.0L, specifications.pdf, pg 21:

2.2.2.1 Each column employs one of the following datatypes:

a) Identifier means that the column shall be able to hold any key value generated for that column.

b) Integer means that the column shall be able to exactly represent integer values (i.e., values in increments of 1) in
the range of at least ( − 2 n − 1 ) to (2 n − 1 − 1), where n is 64.

c) Decimal(d, f) means that the column shall be able to represent decimal values up to and including d digits, of which
f shall occur to the right of the decimal place; the values can be either represented exactly or interpreted to be in
this range.

d) Char(N) means that the column shall be able to hold any string of characters of a fixed length of N.

Comment: If the string that a column of datatype char(N) holds is shorter than N characters, then trailing spaces shall
be stored in the database or the database shall automatically pad with spaces upon retrieval such that a CHAR_LENGTH()
function will return N.

e) Varchar(N) means that the column shall be able to hold any string of characters of a variable length with a maximum
length of N. Columns defined as "varchar(N)" may optionally be implemented as "char(N)".

f) Date means that the column shall be able to express any calendar day between January 1, 1900 and December 31, 2199.

2.2.2.2
The datatypes do not correspond to any specific SQL-standard datatype. The definitions are provided to highlight the
properties that are required for a particular column. The benchmark implementer may employ any internal representation
or SQL datatype that meets those requirements.

## Implementation  
Based on the above definitions, the following datatype definitions mapping was used.  Note `time` and `date` are
converted to UPPER for code formatting consistency, no performance difference is intended.

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

import importlib
import inspect
import pandas as pd
from google.cloud import bigquery
from google.api_core import exceptions as google_api_exceptions

import config, tools, ds_setup, h_setup, utils
from gcp_storage import inventory_bucket_df


def is_google_exception(e):
    m = importlib.import_module("google.api_core.exceptions")
    exception_list = []
    for k in m.__dict__.keys():
        if inspect.isclass(m.__dict__[k]):
            exception_list.append(k)
    print(exception_list)
    if e.__class__.__name__ in exception_list:
        return True
    else:
        return False


log_column_names = ["test", "scale", "dataset",
                    "table", "status", 
                    "t0", "t1", 
                    "size_bytes", "job_id"]


def create_dataset(dataset_name, verbose=False):
    """Create a dataset on the project
    
    See:
    https://cloud.google.com/bigquery/docs/reference/standard-sql/data-definition-language#create_table_statement  
    https://googleapis.dev/python/bigquery/latest/generated/google.cloud.bigquery.client.Client.html#google.cloud.bigquery.client.Client.create_dataset  
    https://googleapis.dev/python/bigquery/latest/generated/google.cloud.bigquery.job.CopyJob.html#google.cloud.bigquery.job.CopyJob
    for details
    
    Parameters
    ----------
    dataset_name : str, name of dataset to create
    verbose : bool, print debug statements
    
    Returns
    -------
    A new copy job instance
    """
    
    dataset_name_full = config.gcp_project.lower() + "." + dataset_name
    dataset = bigquery.Dataset(dataset_name_full)
    dataset.location = config.gcp_location
    
    client = bigquery.Client.from_service_account_json(config.gcp_cred_file)
    copy_job = client.create_dataset(dataset)
    if verbose:
        print("Created dataset {}.{}".format(client.project, dataset.dataset_id))    
    return copy_job


def create_schema(schema_file, dataset, verbose=False):
    """Apply the schema .sql file as reformatted from 
    config.tpcds_schema_ansi_sql_filepath
    to 
    config.tpcds_schema_bq_filepath
    using schema() method in this module.

    Parameters
    ----------
    schema_file : str, path to file containing DDL or sql schema query definitions
    dataset : str, dataset to create table schema in
    verbose : bool, print debug statements
    """
    client = bigquery.Client.from_service_account_json(config.gcp_cred_file)
    with open(schema_file, 'r') as f:
        query_txt = f.read()
    
    job_config = bigquery.QueryJobConfig()
    job_config.default_dataset = config.gcp_project.lower() + "." + dataset
    
    query_job = client.query(query_txt, job_config=job_config)  # API request
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
    f_name : str, name of file as generated with dsdgen
    
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
        except ValueError:
            f_list_new.append(x)
    return "_".join(f_list_new)


def upload_all_local(directory, dataset, verbose=False):
    
    files = tools.file_inventory(directory)
    
    client = bigquery.Client.from_service_account_json(config.gcp_cred_file)
    
    b = BQUpload(client=client,
                 project=config.gcp_project,
                 dataset=dataset)
    
    for f in files:
        table = f[2]
        filepath = f[5]
        load_job = b.upload_local_csv(table=table, filepath=filepath, verbose=verbose)


def validate(directory, dataset, byte_multiplier=1):
    """Validate data integrity between .csv data and data in BigQuery

    Parameters
    ----------
    directory : str, path to directory with .csv data files
    dataset : str, dataset to compare
    byte_multiplier : int, multiplier for size, default is 1, equal to bytes output

    Returns
    -------
    Pandas DataFrame with:
        columns=["table", "local_size", "local_rows", "bq_size", "bq_rows"]
    """

    import pandas as pd

    dir_files = tools.file_inventory(directory)
    table_names = set([f[2] for f in dir_files])
    table_sizes = {f[2]: f[1] for f in dir_files}
    
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
        
    df = pd.DataFrame(data, columns=["table",
                                     "local_size", "local_rows",
                                     "bq_size", "bq_rows"])
    df["bq_percent"] = (df.bq_rows / df.local_rows) * 100
    return df


def create_table_remix(schema_name, source, destination, dot=False):
    """Create table clone SQL from template file. Required to use
    CREATE TABLE with project already set in API context.

    Parameters
    ----------
    schema_name : str, name of sql schema file in /sc
    source : str, source dataset to copy
    destination : str, new dataset to create as copy
    dot : bool, insert '.' into table reference

    Returns
    -------
    str : SQL that will run on BigQuery
    """
    if dot:
        source = source + "."
        destination = destination + "."

    fp_schema = config.fp_schema + config.sep + schema_name

    with open(fp_schema, "r") as f:
        query_text = f.read()
    query_text = query_text.replace("_source_table.", source)
    query_text = query_text.replace("_destination_table.", destination)
    return query_text


def parse_log(fp):
    
    return pd.read_csv(fp, names=log_column_names)


class BQUpload:
    """Upload data from a file location"""
    def __init__(self, test, scale, dataset):
        """
        Parameters
        ----------
        test : str, TPC test name, either "ds" or "h"
        dataset : str, GCP BigQuery dataset running this query
        dataset : str, BigQuery dataset name
        """
        self.client = bigquery.Client.from_service_account_json(config.gcp_cred_file)
        
        self.project = config.gcp_project
        self.bucket_name = config.gcs_data_bucket
        
        self.test = test
        self.scale = scale
        self.dataset = dataset
        
        self.job_config = None
        
        self.tables = None
        self.table_uris = None
        self.table_size_bytes = None
        
        self.df = None
        
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
    
        self.df = inventory_bucket_df(self.bucket_name)
        
        self.df = self.df.loc[(self.df.test == self.test) & 
                              (self.df.scale == str(self.scale)+"GB")].copy()
        
        a_message = """No files in GCS found matching test {} and scale {}""".format(self.test, self.scale)
        assert len(self.df) > 0, a_message
        
        self.df["n"] = self.df.n.astype(int)
        self.df.sort_values(by=["table", "n"], inplace=True)
        self.df.reset_index(inplace=True, drop=True)
        
        #self.collate_table_uris(verbose=False)
    
    def get_all_table_ids(self):
        _tables = self.client.list_tables(dataset=self.dataset)
        self.tables = [_t.table_id for _t in _tables]
        
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
                            dataset=self.dataset,
                            table=t)
            _t = self.get_table(table_id=t)
            _r = _t.num_rows
            _sizes[t] = (_s, _r)
        return _sizes
        
    def upload_local(self, table, filepath, verbose=False):
        """Upload a file to BigQuery

https://googleapis.dev/python/bigquery/latest/generated/google.cloud.bigquery.client.Client.html#google.cloud.bigquery.client.Client.load_table_from_file

        Parameters
        ----------
        table : str, table name to upload data to
        filepath : str, path to file to upload
        verbose : bool, print debug statements

        Returns
        -------
        google.cloud.bigquery.job.LoadJob
        """

        destination = ".".join([self.project, self.dataset, table])

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

    def upload_uri(self, table, source_uris, verbose=False):
        """Upload a file to BigQuery

https://googleapis.dev/python/bigquery/latest/generated/google.cloud.bigquery.client.Client.html#google.cloud.bigquery.client.Client.load_table_from_uri

        Parameters
        ----------
        table : str, table name to upload data to
        uris : str or sequence of str, single path to a GCS file 
            to upload, or a sequence of strings of the same
        verbose : bool, print debug statements
            
        Returns
        -------
        google.cloud.bigquery.job.LoadJob
        """
        
        destination = ".".join([self.project.lower(), self.dataset, table])

        load_job = self.client.load_table_from_uri(source_uris=source_uris,
                                                   destination=destination,
                                                   job_config=self.job_config
                                                   )
        if verbose:
            print("Starting job {}".format(load_job.job_id))
        load_job.result()  # Waits for table load to complete.
        if verbose:
            print("Job finished.".format(load_job.done()))
                
        return load_job
        
    def upload(self, verbose=False):
        """Batch upload data to each table in the dataset.
        Saves a log file to config.fp_ds_output or config.fp_h_output.
        
        Note: this assumes previous methods created data into
        GCS with consistent formatting and the dataset schema
        was done the same way.
        
        Parameters
        ----------
        verbose : bool, print status
        
        Returns
        -------
        fp_log : str, filepath to log of upload process
        """
        
        t0x = pd.Timestamp.now("UTC")
        total_bytes = 0
        d_prefix = [self.test, str(self.scale), self.dataset]
        
        fp_log = ("bq_upload-" + self.test + "_" + 
                  str(self.scale) + "GB-" + 
                  self.dataset + "-" + 
                  str(pd.Timestamp.now("UTC")) + ".csv"
                  )
        
        log_dir = {"h":config.fp_h_output,
                   "ds":config.fp_ds_output}
        fp_log = log_dir[self.test] + config.sep + fp_log
        with open(fp_log, "a") as f:
            _d0 = ",".join(log_column_names) + "\n"
            f.write(_d0)
        
        tables_upload = self.df.table.unique()
        tables_upload = [t for t in tables_upload if t not in config.ignore_tables]
        
        if verbose:
            print("Tables to upload:")
            for tu in tables_upload:
                print(tu)
        
        d = []
        for table in tables_upload:
            
            _df = self.df.loc[self.df.table == table]
            size_bytes = _df.size_bytes.sum()
            total_bytes += size_bytes
            uris = _df.uri.to_list()
            
            t0 = pd.Timestamp.now("UTC")
            
            d0 = d_prefix + [table, "start", 
                             str(t0), "", 
                             str(size_bytes), ""]
            with open(fp_log, "a") as f:
                _d0 = ",".join(d0) + "\n"
                f.write(_d0)
                  
            if verbose:
                print("Loading Table: {}".format(table))
                print("t0: {}".format(t0))
                print("...")
               
            load_job = self.upload_uri(table=table, 
                                       source_uris=uris)
            
            t1 = pd.Timestamp.now("UTC")
            done = load_job.done()
            job_id = load_job.job_id
            
            if verbose:
                print("t1: {}".format(t1))
                print("Load Job Done: {}".format(done))
                print("ID: {}".format(job_id))
                dt = t1-t0
                print("dt: {}".format(dt))
                GBs = (size_bytes/1e9)/dt.total_seconds()
                print("GB/s: {:.2f}".format(GBs))
                print("-"*30)
                
            d1 = d_prefix + [table, "end", 
                             str(t0), str(t1), 
                             str(size_bytes), job_id]
            with open(fp_log, "a") as f:
                _d1 = ",".join(d1) + "\n"
                f.write(_d1)
            
            d.append(d1)
        
        if verbose:
            t1x = pd.Timestamp.now("UTC")
            dtx = t1x-t0x
            print("="*40)
            print("Total load time: {}".format(dtx))
            print("Total size: {:.3f} GB".format(total_bytes/1e9))
            GBsx = (total_bytes/1e9)/dtx.total_seconds()
            print("Speed: {:.3f} GB/s".format(GBsx))
                  
        return fp_log


class BQTPC:
    def __init__(self, test, scale, cid, desc="",
                 timestamp=None, verbose=False, verbose_query=False):
        """Snowflake Connector query class

        Parameters
        ----------
        test : str, TPC test being executed, either "ds" or "h"
        scale : int, database scale factor (i.e. 1, 100, 1000 etc)
        cid : str, config identifier, i.e. "01" or "03A"
        desc : str, description of current data collection effort
        timestamp : Pandas Timestamp object, optional
        verbose : bool, print debug statements
        verbose_query : bool, print query text
        """

        self.test = test
        self.scale = scale
        self.cid = cid

        self.scale_str = str(scale)+"GB"

        self.project = config.gcp_project.lower()
        self.dataset = f"{self.test}_{self.scale_str}_{self.cid}"
        self.desc = desc

        self.df_gcs_full = None  # all files in bucket, as fyi
        self.df_gcs = None       # just files for this dataset

        self.verbose = verbose
        self.verbose_query = verbose_query
        self.verbose_query_n = False  # print line numbers in query text
        self.verbose_iter = False

        self.client = bigquery.Client.from_service_account_json(config.gcp_cred_file)
        self.job_config = bigquery.QueryJobConfig()
        self.job_config.default_dataset = self.project + "." + self.dataset

        self.q_label_base = self.dataset + "-xx-" + self.desc
        self.q_label_base = self.q_label_base.lower()
        self.set_query_label(self.q_label_base)

        self.dry_run = False
        self.cache_set("off")

        self.timestamp = timestamp
        self.results_dir, _ = tools.make_name(db="bq", test=self.test, cid=self.cid,
                                              kind="result", datasource=self.dataset,
                                              desc=self.desc, ext="", timestamp=self.timestamp)
        self.results_csv_fp = None

        if verbose:
            service_account = self.client.get_service_account_email()
            print("BigQuery configuration")
            print("=======================")
            print(f'Service Account:  {service_account}')
            print()

    def cache_set(self, state="off"):
        """Set BigQuery user cache, API defaults to True, here we default to False

        Parameters
        ----------
        state : str, 'on' = cache on, anything else = cache off
        """
        if state == "on":
            self.job_config.use_query_cache = True
        else:
            self.job_config.use_query_cache = False

    def dry_run(self, use=False):
        self.job_config.dry_run = use  # only approximate the time and cost

    def set_query_label(self, query_label):
        self.job_config.labels = {"label": query_label}

    def add_view(self, query_text):
        """Handle the unhelpful behavior of the Python DDL API and views:
        The API allows setting a default dataset but does not honor that
        attribute when creating views"""

        pdt = self.project + "." + self.dataset + "."
        return query_text.replace(config.p_d_id, pdt)

    def dataset_create(self):
        """ Create a dataset on a project

        Returns
        -------
        A new copy job instance
        """

        dataset_class = bigquery.Dataset(self.dataset)
        dataset_class.location = config.gcp_location
        copy_job = self.client.create_dataset(dataset_class)
        if self.verbose:
            print("Created dataset {}.{}".format(self.client.project, self.dataset))
        return copy_job

    def create_schema(self, schema_file):
        """Apply the schema .sql file as reformatted from
        config.tpcds_schema_ansi_sql_filepath
        to
        config.tpcds_schema_bq_filepath

        Parameters
        ----------
        schema_file : str, path to file containing DDL or sql schema query definitions
        """

        with open(schema_file, 'r') as f:
            query_txt = f.read()

        query_job = self.query(query_txt)

        if self.verbose:
            rows = query_job.result()  # Waits for query to finish
            for r in rows:
                print(r.name)

    def gcs_inventory(self):
        """Inventory files in GCS that match this class' test and scale"""
        self.df_gcs_full = inventory_bucket_df(config.gcs_data_bucket)
        self.df_gcs_full.sort_values(by=["test", "scale", "table", "n"], inplace=True)
        self.df_gcs = self.df_gcs_full.loc[(self.df_gcs_full.test == self.test) &
                                           (self.df_gcs_full.scale == self.scale_str)].copy()
        self.df_gcs.uri = self.df_gcs.uri.str.replace("gs:", "gcs:")

    def query(self, query_text):
        """Run a DDL SQL query on BigQuery

        Parameters
        ----------
        query_text : str, query text to execute

        Returns
        -------
        query_result : bigquery.query_job object
        """

        if self.dry_run is True:
            self.job_config.dry_run = True  # only approximate the time and cost

        query_text = self.add_view(query_text)

        if self.verbose_query:
            if self.verbose_query_n:
                qt = "\n".join([str(n) + "  " + line for n, line in enumerate(query_text.split("\n"))])
            else:
                qt = query_text
            print("BIGQUERY QUERY TEXT")
            print("===================")
            print(qt)
            print()

        query_result = self.client.query(query_text, job_config=self.job_config)
        return query_result

    def parse_query_result(self, query_result):
        """
        Parameters
        ----------
        query_result : bigquery.query_job object

        Returns
        -------
        df : Pandas DataFrame containing results of query
        qid : str, query id - unique id of query on BigQuery platform

        t0 : datetime object, time query started
        t1 : datetime object, time query ended
        bytes_processed : int, bytes processed with query
        bytes_billed : int, bytes billed for query
        """
        #result = query_result.result()
        df_result = query_result.to_dataframe()
        t0 = query_result.started
        t1 = query_result.ended
        bytes_processed = query_result.total_bytes_processed
        bytes_billed = query_result.total_bytes_billed
        qid = query_result.job_id
        query_plan = {k: v.__dict__ for k, v in enumerate(query_result.query_plan)}

        return df_result, qid, t0, t1, bytes_processed, bytes_billed, query_plan

    def query_n(self, n, qual=None, std_out=False):
        """Query BigQuery with a specific nth query

        Parameters
        ----------
        n : int, query number to execute
        qual : None, or True to use qualifying values (to test 1GB qualification db)
        std_out : bool, print std_out and std_err output

        Returns
        -------
        t0 : datetime object, time query started
        t1 : datetime object, time query ended
        query_result : bigquery.query_job object result
        query_text : str, query text generated for query
        """
        tpl_dir = f"{config.fp_query_templates}{config.sep}{'bq'}_{self.test}"

        if self.test == "ds":
            query_text = ds_setup.qgen_template(n=n,
                                                templates_dir=tpl_dir,
                                                dialect="sqlserver_tpc",
                                                scale=self.scale,
                                                qual=qual,
                                                verbose=self.verbose,
                                                verbose_std_out=std_out)
        elif self.test == "h":
            query_text = h_setup.qgen_template(n=n,
                                               templates_dir=tpl_dir,
                                               scale=self.scale,
                                               qual=qual,
                                               verbose=self.verbose,
                                               verbose_std_out=std_out)
        else:
            return None

        t0 = pd.Timestamp.now("UTC")

        # BigQuery will process multiple queries in one query statement
        # However, TPC-DS is completely single-queries, TPC-H has a view created and
        # deleted in #15, the creation and delete steps don't have data to capture
        query_list = [q + ";" for q in query_text.split(";") if len(q.strip()) > 0]

        # if query includes a view (make view, query, delete view)
        if len(query_list) == 3:
            query_1_result = self.query(query_list[0])
            if self.verbose:
                print("Non-query reply:", query_1_result.result())

            query_2_result = self.query(query_list[1])
            df_result = query_2_result.result().to_dataframe()
            qid = query_2_result.job_id

            query_3_result = self.query(query_list[2])
            if self.verbose:
                print("Non-query reply:", query_3_result.result())

        # two query statements in query file
        # TPC-DS #39 is actually two query statements
        elif len(query_list) == 2:

            # the first query will not be captured for qc comparison
            _ = self.query(query_list[0])
            # second query is captured
            query_2_result = self.query(query_list[1])
            df_result = query_2_result.result().to_dataframe()
            qid = query_2_result.job_id

        # single query statement
        else:
            query_result = self.query(query_text)
            try:
                df_result = query_result.result().to_dataframe()
                qid = query_result.job_id
            except google_api_exceptions.BadRequest as e:
                error_data = e.errors[0]
                error_data["exception"] = e.__class__.__name__
                df_result = pd.DataFrame([error_data])
                qid = "Exception - " + e.__class__.__name__

        t1 = pd.Timestamp.now("UTC")

        return t0, t1, df_result, query_text, qid

    def query_history(self, t0, t1):
        """Get the query history for the current BigQuery project, bound by
        time.

        Note: for access to information_schema, the account needs role
        'BigQuery Resource Admin' (or perhaps a lower level)

        https://cloud.google.com/bigquery/docs/information-schema-jobs

        Parameters
        ----------
        Both parameters can be either datetime, pd.Timestamp, or str objects
        that can be parsed by pd.to_datetime
        t0 : start time
        t1 : end time
        """
        t0 = pd.to_datetime(t0)
        t1 = pd.to_datetime(t1)
        t0 = t0.strftime("%Y-%m-%d %H:%M:%S")
        t1 = t1.strftime("%Y-%m-%d %H:%M:%S")

        query_text = ("select * from `region-us`.INFORMATION_SCHEMA.JOBS_BY_PROJECT " +
                      "where job_type = 'QUERY' " +
                      f"and end_time between '{t0}' AND '{t1}'")
        query_result = self.query(query_text=query_text)
        df_result, qid, _, _, _, _, _ = self.parse_query_result(query_result)
        return df_result, qid

    def query_seq(self, seq, seq_n=None, qual=None, save=False, verbose_iter=False):
        """Query BigQuery with TPC-DS or TPC-H query template number n

        Parameters
        ----------
        seq : iterable sequence int, query numbers to execute between
            1 and 99 for ds and 1 and 22 for h
        seq_n : int, stream sequence number for test - i.e. 0 or 4 etc
        qual : None, or True to use qualifying values (to test 1GB qualification db)
        save : bool, save data about this query sequence to disk
        verbose_iter : bool, print per iteration status statements

        Returns
        -------
        n_time_data : list, timing data for query stream, with:
            db : str, database system under test name ("sf" or "bq")
            test : str, test name ("ds" or "h")
            scale : int, TPC scale factor in GB
            source : str, source dataset/database
            cid : str, configuration id
            desc : str, description of stream test
            query_n : int, benchmark query number
            seq_n : int, benchmark query sequence/stream number
            driver_t0 : datetime, time on the driver when query was started
            driver_t1 : datatime, time on the driver when query returned
            qid : str, database system under test query id for the query run
        """
        if seq_n is None:
            seq_n = "sNA"
        else:
            seq_n = str(seq_n)
        n_time_data = []
        columns = ["db", "test", "scale", "source", "cid", "desc",
                   "query_n", "seq_n", "driver_t0", "driver_t1", "qid"]

        t0_seq = pd.Timestamp.now("UTC")
        i_total = len(seq)
        for i, n in enumerate(seq):
            qn_label = self.dataset + "-q" + str(n) + "-" + seq_n + "-" + self.desc
            qn_label = qn_label.lower()

            if verbose_iter:
                print("="*40)
                print("BigQuery Start Query:", n)
                print("-"*20)
                print("Stream Completion: {} / {}".format(i+1, i_total))
                print("Query Label:", qn_label)
                print("-"*20)
                print()

            self.set_query_label(qn_label)

            (t0, t1,
             df_result, query_text, qid) = self.query_n(n=n,
                                                        qual=qual,
                                                        std_out=False
                                                        )

            _d = ["bq", self.test, self.scale, self.dataset, self.cid, self.desc,
                  n, seq_n, t0, t1, qid]
            n_time_data.append(_d)

            # write results as collected by each query
            if save:
                self.write_query_text(query_text=query_text, query_n=n)

                if len(df_result) > 0:
                    self.write_results_csv(df=df_result, query_n=n)
                else:
                    # filler for statistics when the query returns no values
                    df_result.loc[0, :] = ["filler"] * df_result.shape[1]
                    if verbose_iter:
                        print("No result rows, FILLER DataFrame created.")
                    self.write_results_csv(df=df_result, query_n=n)

            if verbose_iter:
                dt = t1 - t0
                print("Query ID: {}".format(qid))
                print("Total Time Elapsed: {}".format(dt))
                print("-"*40)
                print()

            if self.verbose:
                if len(df_result) < 25:
                    print("Result:")
                    print("-------")
                    print(df_result)
                    print()
                else:
                    print("Head of Result:")
                    print("---------------")
                    print(df_result.head())
                    print()

        t1_seq = pd.Timestamp.now("UTC")

        #if self.verbose:
        dt_seq = t1_seq - t0_seq
        print()
        print("="*40)
        print("BigQuery Query Stream Done!")
        print("Total Time Elapsed: {}".format(dt_seq))
        print()

        # write local timing results to file
        self.write_times_csv(results_list=n_time_data, columns=columns)

        return pd.DataFrame(n_time_data, columns=columns)

    def write_query_text(self, query_text, query_n):
        """Write query text executed to a specific folder

        Parameters
        ----------
        query_text : str, TPC query SQL executed
        query_n : int, TPC query number
        """
        fd = self.results_dir + config.sep
        tools.mkdir_safe(fd)
        fp = fd + "query_text_bq_{0:02d}.sql".format(query_n)
        with open(fp, "w") as f:
            f.write(query_text)

    def write_results_csv(self, df, query_n):
        """Write the results of a TPC query to a CSV file in a specific
        folder

        Parameters
        ----------
        df : Pandas DataFrame
        query_n : int, query number in TPC test
        """

        fd = self.results_dir + config.sep
        tools.mkdir_safe(fd)
        fp = fd + "query_result_bq_{0:02d}.csv".format(query_n)
        df = tools.to_consistent(df, n=config.float_precision)
        df.to_csv(fp, index=False, float_format="%.3f")

    def write_times_csv(self, results_list, columns):
        """Write a list of results from queries to a CSV file

        Parameters
        ----------
        results_list : list, data as recorded on the local machine
        columns : list, column names for output CSV
        """
        _, fp = tools.make_name(db="bq", test=self.test, cid=self.cid,
                                kind="times",
                                datasource=self.dataset, desc=self.desc,
                                ext=".csv",
                                timestamp=self.timestamp)
        self.results_csv_fp = self.results_dir + config.sep + fp
        df = pd.DataFrame(results_list, columns=columns)
        tools.mkdir_safe(self.results_dir)
        df.to_csv(self.results_csv_fp, index=False)


class Stats(BQTPC):
    def __init__(self, *args, **kwargs):
        super(BQTPC, self).__init__(*args, **kwargs)

    def get_table_names(self):
        client = bigquery.Client.from_service_account_json(config.gcp_cred_file)
        tables = list(client.list_tables(self.dataset))
        table_names = [t.table_id for t in tables]
        return table_names

    def count_distinct(self, table, column):
        query_text = """
        SELECT COUNT(DISTINCT {}) as _result
        FROM `{}`.{}.{}
        """.format(column, self.project, self.dataset, table)

        query_job = self.query(query_text=query_text)
        return list(query_job)

    def count_approx_distinct(self, project, dataset, table, column):
        query_text = """
        SELECT APPROX_COUNT_DISTINCT({}) as _result
        FROM `{}`.{}.{}
        """.format(column, self.project, self.dataset, table)

        query_job = self.query(query_text=query_text)
        return list(query_job)

    def hll(self, project, dataset, table, column):
        query_text = """
        SELECT HLL_COUNT.MERGE(sketch) approx
        FROM (
          SELECT HLL_COUNT.INIT({}) sketch
          FROM `{}`.{}.{}
        )
        """.format(column, self.project, self.dataset, table)

        query_job = self.query(query_text=query_text)
        return list(query_job)

    def count_rows(self, table):
        query_text = """
        SELECT count(*) FROM `{}`.{}.{}
        """.format(self.project, self.dataset, table)
        query_job = self.query(query_text=query_text)
        return list(query_job)

    def get_table_columns(self, table):

        query_text = """SELECT * 
        FROM `{}.{}.{}`
        LIMIT 1;
        """.format(self.project, self.dataset, table)

        query_job = self.query(query_text=query_text)

        cols = list(query_job.result())
        if len(cols) < 1:
            return []
        else:
            cols = cols[0]
            cols = list(cols.keys())
            return cols

    def distinct_table(self, table):

        columns = self.get_table_columns(table=table)

        d = []
        for col in columns:
            x = self.count_approx_distinct(
                    table=table,
                    column=col)
            n = x[0][0]

            y = self.count_rows(table=table)
            c = y[0][0]
            d.append([table, col, n, c])
        return d

    def collect_cardinality(self):

        table_names = self.get_table_names()

        d = []
        for table in table_names:
            y = self.distinct_table(table=table)
            d += y
        return d
