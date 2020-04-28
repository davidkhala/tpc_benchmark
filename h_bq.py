"""TPC-H BigQuery Query Methods

Colin Dietrich 2020"""

from google.cloud import bigquery

import config, tools, h_setup, bq


def query_n(n, project, dataset, scale, 
            template_dir, qual=None, dry_run=False, use_cache=False, verbose=False):
    """Query BigQuery with TPC-H query #n
    
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
                                                templates_dir=template_dir,
                                                scale=scale, 
                                                qual=qual,
                                                verbose=verbose)
    
    query_job = bq.query(query_text=query_text, 
                         project=project, 
                         dataset=dataset, 
                         dry_run=dry_run,
                         use_cache=use_cache)
    
    (t0, t1, 
     bytes_processed, bytes_billed, 
     df) = bq.parse_query_job(query_job=query_job, verbose=verbose)
    
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
    
    query_job = bq.query(query_text=query_text, 
                         project=project, 
                         dataset=dataset, 
                         dry_run=dry_run,
                         use_cache=use_cache)
    
    (t0, t1, 
     bytes_processed, bytes_billed, 
     df) = bq.parse_query_job(query_job=query_job, verbose=verbose)
    
    return t0, t1, bytes_processed, bytes_billed, df
