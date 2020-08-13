"""Quality Control methods on query results

MIT License, see LICENSE file for complete text.
Copyright (c) 2020 SADA Systems, Inc.
"""

import pandas as pd
from pandas.testing import assert_frame_equal

import config, tools


def assert_equal(df1, df2, check_less_precise=True):
    if (len(df1) == 0) & len(df2 == 0):
        return False

    try:
        assert_frame_equal(df1, df2,
                           check_names=False,
                           check_exact=False,
                           check_less_precise=check_less_precise)
        return True
    except AssertionError:
        return False


def equal_percent(df1, df2):
    diff = df1.eq(df2)
    return diff.sum().sum() / (diff.shape[0] * diff.shape[1])


def csv_consistent(fp_df1, fp_df2):
    """Print head middle and tail values of Pandas Dataframe"""
    df1 = pd.read_csv(fp_df1)
    df2 = pd.read_csv(fp_df2)
    df1 = tools.to_consistent(df=df1, n=config.float_precision)
    df2 = tools.to_consistent(df=df2, n=config.float_precision)
    return df1, df2


def assert_equal_csv(fp1, fp2):
    df1, df2 = csv_consistent(fp_df1=fp1, fp_df2=fp2)
    return assert_equal(df1, df2)


def percent_equal_csv(fp1, fp2):
    df1, df2 = csv_consistent(fp_df1=fp1, fp_df2=fp2)
    return equal_percent(df1, df2)


def apply_assert_equal(df):
    """Compare the CSV results from a dual SF/BQ query sequence

    Parameters
    ----------
    df : Pandas Dataframe, filepaths to each results file

    Returns
    -------
    result : Pandas Series, bool if results were identical according
        to assert_equal_csv function
    """

    return df.apply(lambda r: assert_equal_csv(r.fp_bq, r.fp_sf), axis=1)


def apply_percent_equal(df):
    return df.apply(lambda r: percent_equal_csv(r.fp_bq, r.fp_sf), axis=1)
