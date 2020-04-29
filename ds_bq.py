"""TPC-H BigQuery Query Methods

Colin Dietrich 2020"""

import bq, ds_setup


def query_n(n, templates_dir, scale,
            project, dataset,
            qual=None,
            dry_run=False, use_cache=False,
            verbose=False):
    """Query BigQuery with TPC-DS query template number n
    
    Parameters
    ----------
    n : int, query number to execute
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
    query_text = ds_setup.qgen_template(n=n,
                                        templates_dir=templates_dir,
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
    
    return n, t0, t1, bytes_processed, bytes_billed, query_text, df


def stream_p(p, templates_dir, scale,
             project, dataset,
             qual=None,
             dry_run=False, use_cache=False,
             verbose=False, verbose_out=False):
    """Query BigQuery with TPC-D query permutation number p.  
    See specification.pdf Appendix A for orders.
    
    Parameters
    ----------
    p : int, query order permutation number to execute
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

    query_text = ds_setup.qgen_stream(p=p,
                                      templates_dir=templates_dir,
                                      scale=scale,
                                      qual=qual,
                                      verbose=verbose,
                                      verbose_out=verbose_out)
    
    query_job = bq.query(query_text=query_text,
                         project=project,
                         dataset=dataset,
                         dry_run=dry_run,
                         use_cache=use_cache)
    
    (t0, t1,
        bytes_processed, bytes_billed, df
     ) = bq.parse_query_job(query_job=query_job, verbose=verbose)
    
    return p, t0, t1, bytes_processed, bytes_billed, query_text, df
