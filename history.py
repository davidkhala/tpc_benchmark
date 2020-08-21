"""Query history from BigQuery and Snowflake

MIT License, see LICENSE file for complete text.
Copyright (c) 2020 SADA Systems, Inc.
"""

import os
import glob
import pandas as pd

import config, sf_tpc, bq_tpc


def exp_log_t0(results_dir):
    """Get query history for experiment"""
    fps_time = glob.glob(results_dir + config.sep + "benchmark_times*")
    source_csv = {k: v for k, v in zip([os.path.basename(x).split("_")[2] for x in fps_time], fps_time)}
    dfsf = pd.read_csv(source_csv["sf"])
    dfbq = pd.read_csv(source_csv["bq"])
    df = pd.concat([dfsf, dfbq])
    t0 = df.driver_t0.min()
    t0 = pd.to_datetime(t0)
    return t0


def sf_results(results_dir: str, t0: str, buffer_time: str = "20 minutes", verbose: bool = False):
    """Get query history for an already collected query result directory

    Parameters
    ----------
    results_dir : str, directory to data collected
    t0 : str, optional, start time to bound query results
    buffer_time : str, time interval for Pandas.Timedelta, amount of time before t0
    verbose : bool, print debug statements

    Returns
    -------
    df_sf_history_sq : Pandas DataFrame, containing query history
    df_sf_history_av : Pandas DataFrame, containing query history
    """

    if t0 is None:
        t0 = exp_log_t0(results_dir)

    t_buffer = pd.Timedelta(buffer_time)
    t0 = pd.to_datetime(t0) - t_buffer

    df_sf_history, qid_sf = sf_tpc.usage_account(warehouse=config.sf_warehouse[0],
                                                 t0=t0, t1=None, verbose=verbose)
    df_sf_history.to_csv(results_dir + config.sep + "query_history_sf.csv")

    return df_sf_history


def bq_results(results_dir, t0=None, buffer_time="20 minutes", verbose: bool = False):
    """Get query history for an already collected query result directory

    Parameters
    ----------
    results_dir : str, directory to data collected
    t0 : str, optional, start time to bound query results
    buffer_time : str, time interval for Pandas.Timedelta, amount of time before t0
    verbose : bool, print debug statements

    Returns
    -------
    df_bq_history : Pandas DataFrame, containing query history
    """

    if t0 is None:
        t0 = exp_log_t0(results_dir)

    t_buffer = pd.Timedelta(buffer_time)
    t0 = pd.to_datetime(t0) - t_buffer

    bq = bq_tpc.BQTPC(test="ds",
                      scale=1,
                      cid="01",
                      desc="query_history",
                      verbose=verbose,
                      verbose_query=verbose)

    df_bq_history, qid_bq = bq.query_history(t0=t0, t1=pd.Timestamp.utcnow())

    df_bq_history.to_csv(results_dir + config.sep + "query_history_bq.csv")

    return df_bq_history


def test_t0(results_dir):
    """Get query history for experiment"""
    x = glob.glob(results_dir + config.sep + "benchmark_times*")
    df = pd.read_csv(x[0])
    t0 = df.driver_t0.min()
    t0 = pd.to_datetime(t0)
    return t0
