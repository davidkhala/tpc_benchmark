"""Query history from BigQuery and Snowflake

MIT License, see LICENSE file for complete text.
Copyright (c) 2020 SADA Systems, Inc.
"""

import os
import glob
import pandas as pd

import config, sfa, bqa


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


def sf_results(results_dir, buffer_time="20 minutes"):
    """Get query history for an already collected query result directory

    Parameters
    ----------
    results_dir : str, directory to data collected
    buffer_time : str, time interval for Pandas.Timedelta

    Returns
    -------
    df_sf_history_sq : Pandas DataFrame, containing query history
    df_sf_history_av : Pandas DataFrame, containing query history
    """

    t0 = exp_log_t0(results_dir)
    t_buffer = pd.Timedelta(buffer_time)

    sf = sfa.SFTPC(test="ds",
                   scale=1,
                   cid="01",
                   warehouse="TEST9000",
                   desc="query_history",
                   verbose=False,
                   verbose_query=False)
    sf.connect()
    df_sf_history_sq, qid_sf_sq = sf.query_history(t0=pd.to_datetime(t0) - t_buffer,
                                                   t1=pd.Timestamp.now())

    sf.close()

    sf = sfa.AU(warehouse="TEST9000")
    sf.connect()

    df_sf_history_av, qid_sf_av = sf.query_history_view(t0=pd.to_datetime(t0) - t_buffer,
                                                        t1=pd.Timestamp.utcnow())

    sf.close()

    df_sf_history_av.to_csv(results_dir + config.sep + "query_history_sf.csv")

    return df_sf_history_sq, df_sf_history_av


def bq_results(results_dir, buffer_time="20 minutes"):
    """Get query history for an already collected query result directory

    Parameters
    ----------
    results_dir : str, directory to data collected
    buffer_time : str, time interval for Pandas.Timedelta

    Returns
    -------
    df_bq_history : Pandas DataFrame, containing query history
    """

    t0 = exp_log_t0(results_dir)
    t_buffer = pd.Timedelta(buffer_time)

    bq = bqa.BQTPC(test="ds",
                   scale=1,
                   cid="01",
                   desc="query_history",
                   verbose=False,
                   verbose_query=True)

    df_bq_history, qid_bq = bq.query_history(t0=pd.to_datetime(t0) - t_buffer,
                                             t1=pd.Timestamp.utcnow())

    df_bq_history.to_csv(results_dir + config.sep + "query_history_bq.csv")

    return df_bq_history
