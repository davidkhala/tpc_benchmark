
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
    job_config.use_query_cache = use_cache  # API default is True

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
        print("Query Statistics")
        print("================")
        print("Total Time Elapsed: {}".format(dt))
        print("Bytes Processed: {}".format(bytes_processed))
        print("Bytes Billed: {}".format(bytes_billed))
        print()
        if len(df) < 25:
            print("Result:")
            print("=======")
            print(df)
        else:
            print("Head of Result:")
            print("===============")
            print(df.head())

    return t0, t1, bytes_processed, bytes_billed, df


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

    # SF EDITS
    """"
    query_job = query(query_text=query_text,
                      project=project,
                      dataset=dataset,
                      dry_run=dry_run,
                      use_cache=use_cache)

    (t0, t1,
     bytes_processed, bytes_billed,
     df) = parse_query_job(query_job=query_job, verbose=verbose)
    """
    
    return n, t0, t1, bytes_processed, bytes_billed, query_text, df


def query_seq(name, test, seq, templates_dir, scale,
              project, dataset,
              qual=None,
              dry_run=False, use_cache=False,
              verbose=False, verbose_iter=False):
    """Query BigQuery with TPC-DS query template number n

    Parameters
    ----------
    name : str, name of sequence for record keeping
    test : str, TPC test being executed, either "ds" or "h"
    seq : iterable sequence int, query numbers to execute between 1 and 99
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

    query_data = []
    for n in seq:
        (n, t0, t1,
         bytes_processed,
         bytes_billed,
         query_text, df) = query_n(n=n,
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
        _d = ["bq", test, scale, dataset, n,
              t0, t1, bytes_processed, bytes_billed]
        query_data.append(_d)

        if verbose_iter:
            dt = t1 - t0
            print("QUERY:", n)
            print("=========")
            print("Total Time Elapsed: {}".format(dt))
            print("Bytes Processed: {}".format(bytes_processed))
            print("Bytes Billed: {}".format(bytes_billed))
            print("-" * 40)
            print()

    columns = ["db", "test", "scale", "bq_dataset", "query_n",
               "t0", "t1", "bytes_processed", "bytes_billed"]
    df = pd.DataFrame(query_data, columns=columns)
    csv_fp = (config.fp_results + config.sep +
              "bq_{}_query_times-".format(test) +
              str(scale) + "GB-" +
              dataset + "-" + name + "-" +
              str(pd.Timestamp.now()) + ".csv"
              )
    df.to_csv(csv_fp, index=False)

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
    
    # SF CHANGE
    """
    query_job = query(query_text=query_text,
                      project=project,
                      dataset=dataset,
                      dry_run=dry_run,
                      use_cache=use_cache)

    (t0, t1,
     bytes_processed, bytes_billed, df
     ) = parse_query_job(query_job=query_job, verbose=verbose)
    """
    
    return p, t0, t1, bytes_processed, bytes_billed, query_text, df


def stream_seq(name, test, seq, templates_dir, scale,
               project, dataset,
               qual=None,
               dry_run=False, use_cache=False,
               verbose=False, verbose_iter=False):
    """Query BigQuery with TPC-D query permutation number p.
    See specification.pdf Appendix A for orders.

    Parameters
    ----------
    name : str, name of sequence for record keeping
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
         bytes_processed, bytes_billed,
         query_text, df) = stream_p(p=p,
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
        _s = ["bq", "ds", scale, dataset, p,
              t0, t1, bytes_processed, bytes_billed]
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

    columns = ["db", "test", "scale", "bq_dataset", "stream_p",
               "t0", "t1", "bytes_processed", "bytes_billed"]
    df = pd.DataFrame(stream_data, columns=columns)
    csv_fp = (config.fp_results + config.sep +
              "bq_ds_stream_times-" + str(scale) + "GB-" +
              dataset + "-" + name + "-" +
              str(pd.Timestamp.now()) + ".csv"
              )
    df.to_csv(csv_fp, index=False)

    return True
