import re
import pandas as pd
import sf

# TODO: debug
import logging

logger = logging.getLogger('sf_dev')
hdlr = logging.FileHandler('/tmp/sf.log')
formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
hdlr.setFormatter(formatter)
logger.addHandler(hdlr)
logger.setLevel(logging.DEBUG)

import config, tools, ds_setup, h_setup
from gcp_storage import inventory_bucket_df

log_column_names = ["test", "scale", "dataset",
                    "table", "status",
                    "t0", "t1",
                    "size_bytes", "job_id"]


def add_view(query_text, project, dataset):
    """Handle the unhelpful behavior of the Python DDL API and views:
    The API allows setting a default dataset but does not honor that
    attribute when creating views"""

    pdt = project + "." + dataset + "."
    return query_text.replace(config.p_d_id, pdt)


# def query(query_text, project, dataset, dry_run=False, use_cache=False):
#     """Run a DDL SQL query on BigQuery
#
#     Parameters
#     ----------
#     query_text : str, query text to execute
#     project : str, GCP project running this query
#     dataset : str, GCP BigQuery dataset running this query
#     dry_run : bool, execute query as a dry run
#         Default: False
#     use_cache : bool, attempt to use cached results from previous queries
#         Default: False
#
#     Returns
#     -------
#     query_job : bigquery.query_job object
#     """
#
#     client = bigquery.Client.from_service_account_json(config.gcp_cred_file)
#     job_config = bigquery.QueryJobConfig()
#
#     default_dataset = project + "." + dataset
#
#     job_config.default_dataset = default_dataset
#
#     job_config.dry_run = dry_run  # only approximate the time and cost
#     job_config.use_query_cache = use_cache  # API default is True
#
#     query_text = add_view(query_text, project, dataset)
#
#     query_job = client.query(query_text, job_config=job_config)
#
#     return query_job

def parse_query_job(query_job_tuple, verbose=False):
    """

    Parameters
    ----------
    query_job_tuple : results from snowflake
    verbose : bool, print results

    Returns
    -------
    t0 : datetime object, time query started
    t1 : datetime object, time query ended
    bytes_processed : int, bytes processed with query
    bytes_billed : int, bytes billed for query
    df : Pandas DataFrame containing results of query
    """

    start_ts, end_ts, bytes, row_count, cost, data_rows = query_job_tuple
    df = pd.DataFrame(data_rows)

    if verbose:
        print("Query Statistics")
        print("================")
        print("Total Billed Time: {}".format(end_ts-start_ts))
        print("Bytes Processed: {}".format(bytes))
        print("Rows Processed: {}".format(row_count))
        print("Cost: {}".format(cost))
        print()
        if len(df) < 25:
            print("Result:")
            print("=======")
            print(df)
        else:
            print("Head of Result:")
            print("===============")
            print(df.head())

    return start_ts, end_ts, bytes, row_count, cost, df


def query_n(sf_helper, n, test, templates_dir, scale,
            project, dataset,
            qual=None,
            dry_run=False, use_cache=False,
            verbose=False, verbose_out=False):

    #logger.debug(f'n: {n}, test: {test}, templates_dir: {templates_dir}, project: {project}')

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
                                            dialect="sqlserver",
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

    print("XXXXXXXXXXX")
    print(query_text)


    # brute force fix:
    query_text = query_text.replace('set rowcount', 'LIMIT').strip()
    query_text = query_text.replace('top 100;', 'LIMIT 100;').strip()
    query_text = query_text.replace('\n top 100', '\n LIMIT 100').strip()


    # cleanup trailing go
    if query_text.endswith('go'):
        query_text = query_text[:len(query_text)-2]

    # SF EDITS
    logger.debug(f'query idx: {n}, q: "{query_text}"')

    # check if we're running a single query or a batch
    if query_text.count(";") > 1:
        batch = query_text.split(';')
        batch = [b.strip() for b in batch if len(b.strip()) != 0]
        logger.debug(f'batch: {batch}')
        query_result = sf_helper.run_queries(batch)
    else:
        query_result = sf_helper.run_query(query_text)

    logger.debug(f'results: {query_result}')

    (start, end, bytes, rows, cost, df) = parse_query_job(query_job_tuple=query_result, verbose=verbose)

    return n, query_text, start, end, bytes, rows, cost, df


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

    # TODO: debug
    TEST = sf.TEST_DS  # we want to run TPC-H
    SIZE = '100GB'  # dataset size to use in test
    sf_helper = sf.SnowflakeHelper(TEST, SIZE, config)
    # start Warehouse
    sf_helper.warehouse_start()

    query_data = []
    for n in seq:
        print("\n\n\n=========")
        print("START QUERY:", n)
        (n, query_text, start_ts, end_ts, bytes, rows_count, cost, df) = query_n(sf_helper=sf_helper, n=n,
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
        _d = ["sf", test, scale, dataset, n, start_ts, end_ts, bytes, cost]
        query_data.append(_d)

        if verbose_iter:
            print("END QUERY:", n)
            print("=========")
            print("Total Billed Time: {}".format(end_ts-start_ts))
            print("Bytes Processed: {}".format(bytes))
            print("Rows Processed: {}".format(rows_count))
            print("-" * 40)
            print()

    columns = ["db", "test", "scale", "bq_dataset", "query_n",
               "t0", "t1", "bytes_processed", "cost"]
    df = pd.DataFrame(query_data, columns=columns)
    csv_fp = (config.fp_results + config.sep +
              "sf_{}_query_times-".format(test) +
              str(scale) + "GB-" +
              dataset + "-" + name + "-" +
              str(pd.Timestamp.now()) + ".csv"
              )
    df.to_csv(csv_fp, index=False)

    return True


def stream_p(sf_helper, p, test, templates_dir, scale,
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
    # TODO: Duplicate code
    assert test in ["ds", "h"], "'{}' not a TPC test".format(test)

    if test == "ds":

        query_text = ds_setup.qgen_stream(p=p,
                                          templates_dir=templates_dir,
                                          dialect="sqlserver",
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

    # brute force fix:
    # brute force fix:
    query_text = query_text.replace('set rowcount', 'LIMIT').strip()
    query_text = query_text.replace('top 100;', 'LIMIT 100;').strip()
    query_text = query_text.replace('\n top 100', '\n LIMIT 100').strip()

    # SF EDITS
    #logger.debug(f'query idx: {p}, q: "{query_text}"')

    # first split query stream on ";"
    batch = query_text.split(';')
    batch = [b.strip() for b in batch if len(b.strip()) != 0]

    # then split on "go"
    final_query_list = []
    if test == "h":
        for query in batch:
            if 'go' in query:
                split_on_go = query.split('go')
                for item in split_on_go:
                    if item.strip() != '':
                        final_query_list.append(item)
            else:
                final_query_list.append(query)
    else:
        final_query_list = batch

    logger.debug(f'batch_count: {len(final_query_list)}')
    query_result = sf_helper.run_queries(final_query_list)
    logger.debug(f'results: {query_result}')

    (start, end, bytes, rows, cost, df) = parse_query_job(query_job_tuple=query_result, verbose=verbose)

    return p, query_text, start, end, bytes, rows, cost, df


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

    # TODO: debug
    TEST = sf.TEST_DS  # we want to run TPC-H
    SIZE = '100GB'  # dataset size to use in test
    sf_helper = sf.SnowflakeHelper(TEST, SIZE, config)
    # start Warehouse
    sf_helper.warehouse_start()


    stream_data = []
    for p in seq:
        (n, query_text, start_ts, end_ts, bytes, rows_count, cost, df) = stream_p(sf_helper=sf_helper,
                                    p=p,
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
        _s = ["sf", test, scale, dataset, n, start_ts, end_ts, bytes, cost]
        stream_data.append(_s)

        if verbose_iter:
            print("STREAM:", p)
            print("============")
            print("Total Billed Time: {}".format(end_ts-start_ts))
            print("Bytes Processed: {}".format(bytes))
            print("Rows Processed: {}".format(rows_count))
            print("-" * 40)
            print()

    columns = ["db", "test", "scale", "bq_dataset", "query_n",
               "t0", "t1", "bytes_processed", "cost"]
    df = pd.DataFrame(stream_data, columns=columns)
    csv_fp = (config.fp_results + config.sep +
              "sf_{}_stream_times-".format(test) + "_" + str(scale) + "GB-" +
              dataset + "-" + name + "-" +
              str(pd.Timestamp.now()) + ".csv"
              )
    df.to_csv(csv_fp, index=False)

    return True
