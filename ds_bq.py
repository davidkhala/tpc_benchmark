"""TPC-H BigQuery Query Methods

Colin Dietrich 2020"""

from google.cloud import bigquery

import config, tools, ds_setup


def add_view(query, project, dataset):
    """Handle the unhelpful behavior of the Python DDL API and views: 
    The API allows setting a default dataset but does not honor that 
    attribute when creating views"""
    
    pdt = project + "." + dataset + "."
    return query.replace(config.p_d_id, pdt)


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

    job_config.dry_run = dry_run         # only approximate the time and cost
    job_config.use_query_cache = use_cache  # default is True, (try to used cached results)    
    
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
    df = result.to_dataframe()

    t0 = query_job.started
    t1 = query_job.ended
    dt = t1 - t0
    bytes_processed = query_job.total_bytes_processed
    bytes_billed = query_job.total_bytes_billed

    if verbose:
        print("Total Time Elapsed: {}".format(dt))
        print("Bytes Processed: {}".format(bytes_processed))
        print("Bytes Billed: {}".format(bytes_billed))

        if len(df) < 25:
            print("Result:")
            print(df)
        else:
            print("Head of Result:")
            print(df.head())

    return t0, t1, bytes_processed, bytes_billed, df

def query_n(n, project, dataset, scale, 
            template_dir, qual=None, dry_run=False, use_cache=False, verbose=False):
    """Query BigQuery with TPC-DS query #n
    
    Parameters
    ----------
    n : int, query number to execute
    project : str, GCP project running this query
    dataset : str, GCP BigQuery dataset running this query
    scale : int, database scale factor (i.e. 1, 100, 1000 etc)
    template_dir : str, abs path to templates to use for query generation
    qual : None or True, use qualifying values (to test 1GB qualification db)
    dry_run : bool, have BigQuery perform a dry run on the query
        Default: False
    use_cache : bool, False to disable BigQuery cached results
        Default: False
    
    Returns
    -------
    t0 : datetime object, time query started
    t1 : datetime object, time query ended
    bytes_processed : int, bytes processed with query
    bytes_billed : int, bytes billed for query
    df : Pandas DataFrame containing results of query
    """
    query_text, err_out = h_setup.qgen_template(n=n, 
                                                scale=scale, 
                                                templates_dir=template_dir,
                                                qual=qual,
                                                verbose=verbose)
    
    query_job = query(query_text=query_text, 
                      project=project, 
                      dataset=dataset, 
                      dry_run=dry_run,
                      use_cache=use_cache)
    
    (t0, t1, 
     bytes_processed, bytes_billed, 
     df) = parse_query_job(query_job=query_job, verbose=verbose)
    
    return t0, t1, bytes_processed, bytes_billed, df

def stream_p(p, project, dataset, scale, 
            template_dir, qual=None, dry_run=False, use_cache=False, verbose=False):
    """Query BigQuery with TPC-D query permutation number p.  
    See specification.pdf Appendix A for orders.
    
    Parameters
    ----------
    p : int, query order permutation number to execute
    project : str, GCP project running this query
    dataset : str, GCP BigQuery dataset running this query
    scale : int, database scale factor (i.e. 1, 100, 1000 etc)
    template_dir : str, abs path to templates to use for query generation
    qual : None or True, use qualifying values (to test 1GB qualification db)
    dry_run : bool, have BigQuery perform a dry run on the query
        Default: False
    use_cache : bool, False to disable BigQuery cached results
        Default: False
    
    Returns
    -------
    t0 : datetime object, time query started
    t1 : datetime object, time query ended
    bytes_processed : int, bytes processed with query
    bytes_billed : int, bytes billed for query
    df : Pandas DataFrame containing results of query
        note: in the case of stream permutations, this will
        be the query results for the last query in the stream.
    """

    query_text, err_out = h_setup.qgen_stream(p=p, 
                                              scale=scale, 
                                              templates_dir=template_dir, 
                                              verbose=verbose)
    
    query_job = query(query_text=query_text, 
                      project=project, 
                      dataset=dataset, 
                      dry_run=dry_run,
                      use_cache=use_cache)
    
    (t0, t1, 
     bytes_processed, bytes_billed, 
     df) = parse_query_job(query_job=query_job, verbose=verbose)
    
    return t0, t1, bytes_processed, bytes_billed, df