"""Prepare TPC-DS tests for BigQuery

MIT License, see LICENSE file for complete text.
Copyright (c) 2020 SADA Systems, Inc.

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

import re
import pandas as pd
from google.cloud import bigquery

import config, tools, ds_setup, h_setup, utils
from gcp_storage import inventory_bucket_df


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
        
        t0x = pd.Timestamp.now()
        total_bytes = 0
        d_prefix = [self.test, str(self.scale), self.dataset]
        
        fp_log = ("bq_upload-" + self.test + "_" + 
                  str(self.scale) + "GB-" + 
                  self.dataset + "-" + 
                  str(pd.Timestamp.now()) + ".csv"
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
            
            t0 = pd.Timestamp.now()
            
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
            
            t1 = pd.Timestamp.now()
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
            t1x = pd.Timestamp.now()
            dtx = t1x-t0x
            print("="*40)
            print("Total load time: {}".format(dtx))
            print("Total size: {:.3f} GB".format(total_bytes/1e9))
            GBsx = (total_bytes/1e9)/dtx.total_seconds()
            print("Speed: {:.3f} GB/s".format(GBsx))
                  
        return fp_log


def add_view(query_text, project, dataset):
    """Handle the unhelpful behavior of the Python DDL API and views:
    The API allows setting a default dataset but does not honor that
    attribute when creating views"""

    pdt = project + "." + dataset + "."
    return query_text.replace(config.p_d_id, pdt)


def query(query_text, project, dataset, dry_run=False, use_cache=False):
    """Run a DDL SQL query on BigQuery

    Parameters
    ----------
    query_text : str, query text to execute
    project : str, GCP project running this query
    dataset : str, GCP BigQuery dataset running this query
    dry_run : bool, execute query as a dry run
        Default: False
    use_cache : bool, attempt to use cached results from previous queries
        Default: False

    Returns
    -------
    query_job : bigquery.query_job object
    """

    client = bigquery.Client.from_service_account_json(config.gcp_cred_file)
    job_config = bigquery.QueryJobConfig()

    default_dataset = project + "." + dataset

    job_config.default_dataset = default_dataset

    job_config.dry_run = dry_run  # only approximate the time and cost
    job_config.use_query_cache = use_cache  # API default is True, here False

    query_text = add_view(query_text, project, dataset)

    query_job = client.query(query_text, job_config=job_config)

    return query_job


def parse_query_job(query_job, verbose=False):
    """

    Parameters
    ----------
    query_job : bigquery.query_job object
    verbose : bool, print results

    Returns
    -------
    t0 : datetime object, time query started
    t1 : datetime object, time query ended
    bytes_processed : int, bytes processed with query
    bytes_billed : int, bytes billed for query
    df : Pandas DataFrame containing results of query
    """

    result = query_job.result()
    df_n = result.to_dataframe()

    t0 = query_job.started
    t1 = query_job.ended
    dt = t1 - t0
    bytes_processed = query_job.total_bytes_processed
    bytes_billed = query_job.total_bytes_billed
    job_id = query_job.job_id
    query_plan = {k: v.__dict__ for k, v in enumerate(query_job.query_plan)}

    if verbose:
        print("Query Statistics")
        print("================")
        print("Total Time Elapsed: {}".format(dt))
        print("Bytes Processed: {}".format(bytes_processed))
        print("Bytes Billed: {}".format(bytes_billed))
        print()
        if len(df_n) < 25:
            print("Result:")
            print("=======")
            print(df_n)
        else:
            print("Head of Result:")
            print("===============")
            print(df_n.head())

    return t0, t1, bytes_processed, bytes_billed, query_plan, df_n, job_id


def query_n(n, test, templates_dir, scale,
            project, dataset,
            qual=None,
            dry_run=False, use_cache=False,
            verbose=False, verbose_out=False):
    """Query BigQuery with TPC-DS query template number n

    Parameters
    ----------
    n : int, query number to execute
    test : str, TPC test being executed, either "ds" or "h"
    templates_dir : str, abs path to templates to use for query generation
    scale : int, database scale factor (i.e. 1, 100, 1000 etc)
    project : str, GCP project running this query
    dataset : str, GCP BigQuery dataset running this query
    qual : None, or True to use qualifying values (to test 1GB qualification db)
    dry_run : bool, have BigQuery perform a dry run on the query
        Default: False
    use_cache : bool, False to disable BigQuery cached results
        Default: False
    verbose : bool, print debug statements
    verbose_out : bool, print std_out and std_err output

    Returns
    -------
    n : int, query number executed
    t0 : datetime object, time query started
    t1 : datetime object, time query ended
    bytes_processed : int, bytes processed with query
    bytes_billed : int, bytes billed for query
    query_text : str, query text generated for query
    df : Pandas DataFrame containing results of query
    """

    assert test in ["ds", "h"], "'{}' not a TPC test".format(test)

    if test == "ds":

        query_text = ds_setup.qgen_template(n=n,
                                            templates_dir=templates_dir,
                                            dialect="sqlserver_bq",
                                            scale=scale,
                                            qual=qual,
                                            verbose=verbose,
                                            verbose_out=verbose_out)
    elif test == "h":
        query_text = h_setup.qgen_template(n=n,
                                           templates_dir=templates_dir,
                                           scale=scale,
                                           qual=qual,
                                           verbose=verbose,
                                           verbose_out=verbose_out)
    else:
        return None

    query_job = query(query_text=query_text,
                      project=project,
                      dataset=dataset,
                      dry_run=dry_run,
                      use_cache=use_cache)

    return parse_query_job(query_job=query_job, verbose=verbose)


def query_seq(desc, test, seq, templates_dir, scale,
              project, dataset,
              qual=None, save=False,
              dry_run=False, use_cache=False,
              verbose=False, verbose_iter=False, verbose_query=False):
    """Query BigQuery with TPC-DS or TPC-H query template number n

    Parameters
    ----------
    desc : str, description of sequence for record keeping
    test : str, TPC test being executed, either "ds" or "h"
    seq : iterable sequence int, query numbers to execute between 1 and 99
    templates_dir : str, abs path to templates to use for query generation
    scale : int, database scale factor (i.e. 1, 100, 1000 etc)
    project : str, GCP project running this query
    dataset : str, GCP BigQuery dataset running this query
    qual : None, or True to use qualifying values (to test 1GB qualification db)
    save : bool, save data about this query sequence to disk
    dry_run : bool, have BigQuery perform a dry run on the query
        Default: False
    use_cache : bool, False to disable BigQuery cached results
        Default: False
    verbose : bool, print debug statements
    verbose_iter : bool, print per iteration status statements

    Returns
    -------
    n : int, query number executed
    t0 : datetime object, time query started
    t1 : datetime object, time query ended
    bytes_processed : int, bytes processed with query
    bytes_billed : int, bytes billed for query
    query_text : str, query text generated for query
    df : Pandas DataFrame containing results of query
    """

    assert test in ["ds", "h"], "'{}' not a TPC test".format(test)

    query_measured_results = []
    df_out = pd.DataFrame(None)
    for n in seq:
        if verbose_iter:
            print("===============")
            print("START QUERY:", n)

        (n, t0, t1,
         bytes_processed,
         bytes_billed, query_text,
         query_plan, df) = query_n(n=n,
                                   test=test,
                                   templates_dir=templates_dir,
                                   scale=scale,
                                   qual=qual,
                                   project=project,
                                   dataset=dataset,
                                   dry_run=dry_run,
                                   use_cache=use_cache,
                                   verbose=verbose,
                                   verbose_out=False
                                   )
        _d = ["bq", test, scale, dataset, desc, n,
              t0, t1, bytes_processed, bytes_billed, query_plan, ""]
        query_measured_results.append(_d)

        df_out = pd.concat([df_out, df])

        if verbose_query:
            print()
            print("QUERY EXECUTED")
            print("==============")
            print(query_text)

        if verbose_iter:
            dt = t1 - t0
            print("-" * 40)
            print("Total Time Elapsed: {}".format(dt))
            print("Bytes Processed: {}".format(bytes_processed))
            print("Bytes Billed: {}".format(bytes_billed))
            print("-" * 40)
            print("QUERY:", n)
            print("=========")
            print()

    columns = ["db", "test", "scale", "bq_dataset", "desc", "query_n",
               "t0", "t1", "bytes_processed", "bytes_billed", "query_plan", "cost"]

    # write results to csv file
    utils.write_to_csv("bq", test, dataset, desc, columns, query_measured_results, kind="query")
    if save:
        df_fp = utils.result_namer("bq", test, dataset, desc, kind="query")
        df_out.to_csv(df_fp, index=False)

    return True


def stream_p(p, test, templates_dir, scale,
             project, dataset,
             qual=None,
             dry_run=False, use_cache=False,
             verbose=False, verbose_out=False):
    """Query BigQuery with TPC-D query permutation number p.
    See specification.pdf Appendix A for orders.

    Parameters
    ----------
    p : int, query order permutation number to execute
    test : str, TPC test being executed, either "ds" or "h"
    templates_dir : str, abs path to templates to use for query generation
    scale : int, database scale factor (i.e. 1, 100, 1000 etc)
    project : str, GCP project running this query
    dataset : str, GCP BigQuery dataset running this query
    qual : None or True, use qualifying values (to test 1GB qualification db)
    dry_run : bool, have BigQuery perform a dry run on the query
        Default: False
    use_cache : bool, False to disable BigQuery cached results
        Default: False
    verbose : bool, print debug statements
    verbose_out : bool, print std_out and std_err output

    Returns
    -------
    p : int, query stream number of generated SQL query stream
    t0 : datetime object, time query started
    t1 : datetime object, time query ended
    bytes_processed : int, bytes processed with query
    bytes_billed : int, bytes billed for query
    query_text : str, query text generated for query
    df : Pandas DataFrame containing results of query
        note: in the case of stream permutations, this will
        be the query results for the last query in the stream.
    """

    assert test in ["ds", "h"], "'{}' not a TPC test".format(test)

    if test == "ds":

        query_text = ds_setup.qgen_stream(p=p,
                                          templates_dir=templates_dir,
                                          dialect='sqlserver_bq',
                                          scale=scale,
                                          qual=qual,
                                          verbose=verbose,
                                          verbose_out=verbose_out)

    elif test == "h":
        query_text = h_setup.qgen_stream(p=p,
                                         templates_dir=templates_dir,
                                         scale=scale,
                                         qual=qual,
                                         verbose=verbose,
                                         verbose_out=verbose_out)
    else:
        return None

    query_job = query(query_text=query_text,
                      project=project,
                      dataset=dataset,
                      dry_run=dry_run,
                      use_cache=use_cache)

    (t0, t1,
     bytes_processed, bytes_billed,
     query_plan, df) = parse_query_job(query_job=query_job, verbose=verbose)

    return p, t0, t1, bytes_processed, bytes_billed, query_text, query_plan, df


def stream_seq(desc, test, seq, templates_dir, scale,
               project, dataset,
               qual=None,
               dry_run=False, use_cache=False,
               verbose=False, verbose_iter=False):
    """Query BigQuery with TPC-D query permutation number p.
    See specification.pdf Appendix A for orders.

    Parameters
    ----------
    desc : str, description of sequence for record keeping
    test : str, TPC test being executed, either "ds" or "h"
    seq : iterable sequence int, query numbers to execute between 1 and 99
    templates_dir : str, abs path to templates to use for query generation
    scale : int, database scale factor (i.e. 1, 100, 1000 etc)
    project : str, GCP project running this query
    dataset : str, GCP BigQuery dataset running this query
    qual : None or True, use qualifying values (to test 1GB qualification db)
    dry_run : bool, have BigQuery perform a dry run on the query
        Default: False
    use_cache : bool, False to disable BigQuery cached results
        Default: False
    verbose : bool, print debug statements
    verbose_iter : bool, print std_out and std_err output

    Returns
    -------
    p : int, query stream number of generated SQL query stream
    t0 : datetime object, time query started
    t1 : datetime object, time query ended
    bytes_processed : int, bytes processed with query
    bytes_billed : int, bytes billed for query
    query_text : str, query text generated for query
    df : Pandas DataFrame containing results of query
        note: in the case of stream permutations, this will
        be the query results for the last query in the stream.
    """

    stream_data = []
    for p in seq:
        (p, t0, t1,
         bytes_processed, bytes_billed, query_text,
         query_plan, df) = stream_p(p=p,
                                    test=test,
                                    templates_dir=templates_dir,
                                    scale=scale,
                                    project=project,
                                    dataset=dataset,
                                    qual=qual,
                                    dry_run=dry_run,
                                    use_cache=use_cache,
                                    verbose=verbose,
                                    verbose_out=False
                                    )
        _s = ["bq", test, scale, dataset, desc, p,
              t0, t1, bytes_processed, bytes_billed, query_plan, "NA"]
        stream_data.append(_s)

        if verbose_iter:
            dt = t1 - t0
            print("STREAM:", p)
            print("============")
            print("Total Time Elapsed: {}".format(dt))
            print("Bytes Processed: {}".format(bytes_processed))
            print("Bytes Billed: {}".format(bytes_billed))
            print("-" * 40)
            print()

    columns = ["db", "test", "scale", "dataset", "desc", "stream_p",
               "t0", "t1", "bytes_processed", "bytes_billed", "query_plan", "cost"]

    # write data to file
    utils.write_to_csv("sf", test, dataset, desc, columns, stream_data, kind="stream")

    return True


def get_table_names(dataset):
    client = bigquery.Client.from_service_account_json(config.gcp_cred_file)
    tables = list(client.list_tables(dataset))
    table_names = [t.table_id for t in tables]
    return table_names


def count_distinct(project, dataset, table, column):
    query_text = """
    SELECT COUNT(DISTINCT {}) as _result
    FROM `{}`.{}.{}
    """.format(column, project, dataset, table)
    
    query_job = query(query_text=query_text,
                      project=project,
                      dataset=dataset,
                      dry_run=False,
                      use_cache=False)
    return list(query_job)


def count_approx_distinct(project, dataset, table, column):
    query_text = """
    SELECT APPROX_COUNT_DISTINCT({}) as _result
    FROM `{}`.{}.{}
    """.format(column, project, dataset, table)
    
    query_job = query(query_text=query_text,
                      project=project,
                      dataset=dataset,
                      dry_run=False,
                      use_cache=False)
    return list(query_job)

def hll(project, dataset, table, column):
    query_text = """
    SELECT HLL_COUNT.MERGE(sketch) approx
    FROM (
      SELECT HLL_COUNT.INIT({}) sketch
      FROM `{}`.{}.{}
    )
    """.format(column, project, dataset, table)
    
    query_job = query(query_text=query_text,
                      project=project,
                      dataset=dataset,
                      dry_run=False,
                      use_cache=False)
    
    return list(query_job)


def count_rows(project, dataset, table):
    query_text = """
    SELECT count(*) FROM `{}`.{}.{}
    """.format(project, dataset, table)
    query_job = query(query_text=query_text,
                      project=project,
                      dataset=dataset,
                      dry_run=False,
                      use_cache=False)
    
    return list(query_job)


def get_table_columns(project, dataset, table):
    
    query_text = """SELECT * 
    FROM `{}.{}.{}`
    LIMIT 1;
    """.format(project, dataset, table)    
    
    query_job = query(query_text=query_text,
                      project=project,
                      dataset=dataset,
                      dry_run=False,
                      use_cache=False)
    
    cols = list(query_job.result())
    if len(cols) < 1:
        return []
    else: 
        cols = cols[0]
        cols = list(cols.keys())
        return cols


def distinct_table(project, dataset, table):
    
    columns = get_table_columns(project, dataset, table)
    
    d = []
    for col in columns:
        x = count_approx_distinct(project=project, 
                dataset=dataset, 
                table=table,
                column=col)
        n = x[0][0]
        
        y = count_rows(project=project, 
                       dataset=dataset, 
                       table=table)
        c = y[0][0]
        d.append([table, col, n, c])
    return d


def collect_cardinality(project, dataset):
    
    table_names = get_table_names(dataset)

    d = []
    for table in table_names:
        y = distinct_table(project=project,
                                   dataset=dataset,
                                   table=table)
        d += y
    return d