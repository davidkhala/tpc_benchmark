import pandas as pd
import sf
import utils

import logging

import config, ds_setup, h_setup

# log_column_names = ["test", "scale", "dataset",
#                     "table", "status",
#                     "t0", "t1",
#                     "size_bytes", "job_id"]
#
#
# def add_view(query_text, project, dataset):
#     """Handle the unhelpful behavior of the Python DDL API and views:
#     The API allows setting a default dataset but does not honor that
#     attribute when creating views"""
#
#     pdt = project + "." + dataset + "."
#     return query_text.replace(config.p_d_id, pdt)


# TODO: remove this function
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
    start_ts, end_ts, bytes_processed, row_count, cost, data_rows = query_job_tuple
    df = pd.DataFrame(data_rows)

    if verbose:
        print("Query Statistics")
        print("================")
        print("Total Billed Time: {}".format(end_ts-start_ts))
        print("Bytes Processed: {}".format(bytes_processed))
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

    return start_ts, end_ts, bytes_processed, row_count, cost, df

# TODO: we should pass a structure, too many vars
def query_n(sf_helper, n, test, templates_dir, scale,
            project, dataset,
            qual=None,
            dry_run=False, use_cache=False,
            verbose=False, verbose_out=False):

    """Query Snowflake with a specific Nth query

    ...
    """

    assert test in config.tests, "'{}' not a TPC test".format(test)

    # generate query text
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

    # clean query
    query_text = sf_helper.brute_force_clean_query(query_text)
    #logging.debug(f'query idx: {n}, q: "{query_text}"')

    # check if we're running a single query or a batch
    if query_text.count(";") > 1:
        batch = query_text.split(';')
        batch = [b.strip() for b in batch if len(b.strip()) != 0]
        #logging.debug(f'batch: {batch}')
        query_result = sf_helper.run_queries(batch)
    else:
        query_result = sf_helper.run_query(query_text)

    #logging.debug(f'results: {query_result}')

    (start, end, bytes_processed, rows, cost, df) = parse_query_job(query_job_tuple=query_result, verbose=verbose)
    return n, query_text, start, end, bytes_processed, rows, cost, df


# TODO: we should pass a structure, too many vars
def query_seq(name, test, seq, templates_dir, scale, project, dataset,
              qual=None, dry_run=False, use_cache=False, verbose=False, verbose_iter=False):
    """Query Snowflake with TPC DS/H test sequence

    ...
    """

    assert test in config.tests, "'{}' not a TPC test".format(test)

    # connect to Snowflake
    sf_helper = sf.SnowflakeHelper(test, f'{scale}GB', config)
    # enable warehouse and target database based on "test" and "size"
    sf_helper.warehouse_start()

    # run all queries in sequence
    query_data = []
    for n in seq:
        print("\n\n\n=========")
        print("START QUERY:", n)
        # run query
        # TODO: we should pass a structure, too many vars
        results = query_n(sf_helper=sf_helper,
                          n=n,
                          test=test,
                          templates_dir=templates_dir,
                          scale=scale,
                          qual=qual,
                          project=project,
                          dataset=dataset,
                          dry_run=dry_run,
                          use_cache=use_cache,
                          verbose=verbose,
                          verbose_out=False)
        # unpack results
        (n, query_text, start_ts, end_ts, bytes_processed, rows_count, cost, df) = results

        # append results
        query_data.append(["sf", test, scale, dataset, n, start_ts, end_ts, bytes_processed, cost])

        if verbose_iter:
            print("END QUERY:", n)
            print("=========")
            print("Total Billed Time: {}".format(end_ts-start_ts))
            print("Bytes Processed: {}".format(bytes_processed))
            print("Rows Processed: {}".format(rows_count))
            print("-" * 40)
            print()

    # set of columns to write to csv file
    columns = ["db", "test", "scale", "bq_dataset", "query_n", "t0", "t1", "bytes_processed", "cost"]

    # write results to csv file
    utils.write_to_csv("sf", test, scale, dataset, name, columns, query_data)

    # suspend warehouse
    sf_helper.warehouse_suspend()
    return True


def stream_p(sf_helper, p, test, templates_dir, scale,
             project, dataset,
             qual=None,
             dry_run=False, use_cache=False,
             verbose=False, verbose_out=False):
    """...
    """
    # TODO: Duplicate code
    assert test in config.tests, "'{}' not a TPC test".format(test)

    # generate query text
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
    query_text = sf_helper.brute_force_clean_query(query_text)
    #logging.debug(f'query idx: {n}, q: "{query_text}"')

    # with "stream" we always run in "batch" mode.
    # first split query stream on ";"
    batch = query_text.split(';')
    batch = [b.strip() for b in batch if len(b.strip()) != 0]

    # since dsqgen can use both semicolon and "go" for query termination: split on "go" also if this is a TPC-H test
    if test == "h":
        final_query_list = []
        for query in batch:
            if 'go' in query:
                split_on_go = query.split('go')
                for item in split_on_go:
                    if item.strip() != '':
                        final_query_list.append(item)
            else:
                final_query_list.append(query)
        batch = final_query_list

    # run batch of queries
    logging.debug(f'batch_count: {len(batch)}')
    query_result = sf_helper.run_queries(batch)

    (start, end, bytes_received, rows, cost, df) = parse_query_job(query_job_tuple=query_result, verbose=verbose)

    return p, query_text, start, end, bytes_received, rows, cost, df


def stream_seq(name, test, seq, templates_dir, scale,
               project, dataset,
               qual=None,
               dry_run=False, use_cache=False,
               verbose=False, verbose_iter=False):
    """...
    """

    # connect to Snowflake
    sf_helper = sf.SnowflakeHelper(test, f'{scale}GB', config)

    # enable warehouse and target database based on "test" and "size"
    sf_helper.warehouse_start()

    stream_data = []
    for p in seq:
        results = stream_p( sf_helper=sf_helper,
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
                            verbose_out=False)
        # unpack results
        (n, query_text, start_ts, end_ts, bytes_processed, rows_count, cost, df) = results
        stream_data.append(["sf", test, scale, dataset, n, start_ts, end_ts, bytes_processed, cost])

        if verbose_iter:
            print("STREAM:", p)
            print("============")
            print("Total Billed Time: {}".format(end_ts-start_ts))
            print("Bytes Processed: {}".format(bytes_processed))
            print("Rows Processed: {}".format(rows_count))
            print("-" * 40)
            print()

    # set csv file columns
    columns = ["db", "test", "scale", "bq_dataset", "query_n", "t0", "t1", "bytes_processed", "cost"]

    # write data to file
    utils.write_to_csv("sf", test, scale, dataset, name, columns, stream_data)

    # suspend warehouse
    sf_helper.warehouse_suspend()
    return True
