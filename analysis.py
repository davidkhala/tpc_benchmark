"""Compile and analyze TPC benchmark results

MIT License, see LICENSE file for complete text.
Copyright (c) 2020 SADA Systems, Inc.
"""

import glob
import math

import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

import config, history


def bq_reserve_cost(ts, slots):
    """Calculate the cost of a flex-slot reservation on BigQuery

    Note:
        ts is rounded UP to a integer second
        slots is rounded UP to the next multiple of 100

    Parameters
    ----------
    ts : float, milliseconds of slot consumption
    slots : int, number of slots to reserve

    Returns
    -------
    float : dollars of billed cost
    """
    return 4.00 * math.ceil(slots / 100) * (math.ceil(ts) / (1000 * 60 * 60))


def plot_dual(df1, df2, y1_label="", y2_label="", drop=None, save_dir=None, suffix=None):
    """Plot two aggregate pivot plots

    Parameters
    ----------
    df1 : Pandas Dataframe, pivoted data with:
        `query_n` row index
        (`db`, `desc`) column multi-index
    y1_label : str, label for df1 y axis
    df2 : Pandas Dataframe, pivoted data with:
        `query_n` row index
        (`db`, `desc`) column multi-index
    y2_label : str, label for df2 y axis
    drop : list of int, index rows (as `query_n`) to drop from plot
    save_dir : str, path to directory to save plot to. If not set, nothing is saved.
    suffix : str, suffix for save_dir file name

    Returns
    -------
    matplotlib.figure.Figure
    """

    if drop is not None:
        df1 = df1[~(df1.index.isin(drop))]
        df2 = df2[~(df2.index.isin(drop))]

    color_palette = sns.hls_palette(n_colors=12)
    fig, (ax1, ax2) = plt.subplots(2, 1)
    ax1 = df1.plot.bar(ax=ax1, legend=False, color=color_palette)

    # invert bytes processed for plotting
    _df2 = df2.copy() * -1
    ax2 = _df2.plot.bar(ax=ax2, legend=False, color=color_palette)

    # set the figure size BEFORE adjusting ticks
    fig.set_size_inches(30, 8, forward=True)

    handles, labels = ax1.get_legend_handles_labels()

    ax1.set_ylabel(y1_label)
    ax1.set_xlabel(None)
    ax1.xticklabels = ax1.get_xticklabels()
    ax1.set_xticklabels(labels=[])

    ax2.set_ylabel(y2_label)
    ax2.set_xlabel("Query Number")
    ticks = ax2.get_yticks()
    ticks = ["{:01.4f}".format(abs(tick)) for tick in ticks]
    ax2.set_yticklabels(ticks)
    ax2.xaxis.tick_top()

    plt.subplots_adjust(hspace=0.15)
    plt.subplots_adjust(right=0.93)

    fig.legend(handles, labels, loc="right")

    if save_dir is not None:
        if suffix is not None:
            suffix = "_" + suffix
        else:
            suffix = ""
        plot_fp = f"plot_query_dual{suffix}.png"
        plt.savefig(save_dir + config.sep + plot_fp, bbox_to_anchor='tight')

    return fig


class MultiResult:
    def __init__(self):
        self.results_dir = None
        self.data_folders = None
        self.df = None
        self.df_query = None

        self.df_bq_history = None
        self.df_sf_history = None

        self.dfp_TB = None
        self.dfp_dt = None
        self.dfp_cost = None
        self.df_agg = None

        self.verbose = True

    def local_inventory(self):
        self.data_folders = glob.glob(self.results_dir + config.sep + "result*")
        d = []
        for f in self.data_folders:
            f2 = f.split(config.sep)[-1]
            f3 = f2.split("_")
            d.append(f3)
        self.df = pd.DataFrame(d, columns=["data_type", "system", "test", "scale", "cid", "desc", "date", "time"])
        self.df["fp"] = self.data_folders

        query_files = glob.glob(self.results_dir + config.sep + "result*" + config.sep + "benchmark_times*")
        query_data = []
        for qf in query_files:
            _df = pd.read_csv(qf)
            query_data.append(_df)
        self.df_query = pd.concat(query_data)
        self.df_query.drop_duplicates(subset="qid", inplace=True)

    def apply_history(self, row):
        """Apply History Download per file"""
        x = glob.glob(row.fp + config.sep + "benchmark_times*")
        df = pd.read_csv(x[0])
        t0 = df.driver_t0.min()
        t0 = pd.to_datetime(t0)
        if row.system == "sf":
            _df = history.sf_results(results_dir=row.fp, t0=t0, verbose=self.verbose)
        elif row.system == "bq":
            _df = history.bq_results(results_dir=row.fp, t0=t0, verbose=self.verbose)
        return t0

    def system_inventory(self):
        """Applies a function across self.df to download query histories to:
        query_history_bq.csv
        query_history_sf.csv
        """
        self.df["t0"] = self.df.apply(self.apply_history, axis=1)

    def row_extract_sf(self, row, value):
        """Extract Arbitrary row value from
        Snowflake Account Usage table

        Parameters
        ----------
        row : Pandas Series, from Snowflake history account view
        value : str, name of column in row

        Returns
        -------
        single value or None if nothing found
        """
        mask = self.df_sf_history.QUERY_ID == row["qid"]
        out = self.df_sf_history.loc[mask, value.upper()]
        if out.values.shape == (1,):
            return out.values[0]
        else:
            return None

    def row_extract_bq(self, row, value):
        """Extract Arbitrary row value from
        BigQuery Account Usage table

        Parameters
        ----------
        row : Pandas Series, from BigQuery history account view
        value : str, name of column in row

        Returns
        -------
        single value or None if nothing found
        """
        mask = self.df_bq_history.job_id == row["qid"]
        out = self.df_bq_history.loc[mask, value]
        return out.values[0]

    def compile_history(self):
        data = []
        for folder in self.df.loc[self.df.system == "sf", "fp"].values:
            fp = folder + config.sep + "query_history_sf.csv"
            _df = pd.read_csv(fp)
            data.append(_df)
        self.df_sf_history = pd.concat(data)
        self.df_sf_history.drop_duplicates(subset="QUERY_ID", inplace=True)

        data = []
        for folder in self.df.loc[self.df.system == "bq", "fp"].values:
            fp = folder + config.sep + "query_history_bq.csv"
            _df = pd.read_csv(fp)
            data.append(_df)
        self.df_bq_history = pd.concat(data)
        self.df_bq_history.drop_duplicates(subset="job_id", inplace=True)

    def append_history(self):

        # intialize the columns before .loc into them
        for col in set(config.sf_keep + config.bq_keep):
            col = col.lower()
            self.df_query[col] = None

        for col in config.sf_keep:
            col = col.lower()
            if self.verbose:
                print("SF:", col)
            self.df_query[col] = self.df_query.apply(lambda x: self.row_extract_sf(row=x, value=col), axis=1)

        for col in config.bq_keep:
            if self.verbose:
                print("BQ:", col)
            _df_target = self.df_query.loc[self.df_query.db == "bq", col.lower()]
            _df_source = self.df_query.loc[self.df_query.db == "bq", col]
            _df_target = _df_source.apply(lambda x: self.row_extract_bq(row=x, value=col))

    def aggregate(self):
        """Aggregate
        self.df_query - query result record on local machine
        self.df_sf_history - query_history from Snowflake account usage
        self.df_bq_history - query history from BigQuery Information Schema
        """

        dfq = self.df_query[["db", "test", "scale", "source", "cid", "desc",
                             "query_n", "seq_n", "qid", "driver_t0",
                             "driver_t1"]].copy()

        # Snowflake
        row_select = self.df_sf_history.QUERY_ID.apply(lambda x: x in dfq.qid.unique())
        column_select = ["QUERY_ID", "START_TIME", "END_TIME", "TOTAL_ELAPSED_TIME", "BYTES_SCANNED",
                         "CREDITS_USED_CLOUD_SERVICES"]
        dfsfh = self.df_sf_history.loc[row_select, column_select].copy()
        dfsfh.reset_index(inplace=True, drop=True)
        dfsfh.columns = ["sf_" + col for col in dfsfh.columns]
        dfsfh.rename({"sf_QUERY_ID": "qid",
                      "sf_START_TIME": "start_time",
                      "sf_END_TIME": "end_time",
                      "sf_BYTES_SCANNED": "bytes",
                      "sf_TOTAL_ELAPSED_TIME": "sf_elapsed",
                      "sf_CREDITS_USED_CLOUD_SERVICES": "sf_credits"}, axis=1, inplace=True)
        dfsfh.start_time = pd.to_datetime(dfsfh.start_time)
        dfsfh.end_time = pd.to_datetime(dfsfh.end_time)

        # BigQuery
        row_select = self.df_bq_history.job_id.apply(lambda x: x in dfq.qid.unique())
        column_select = ["job_id", "start_time", "end_time", "total_bytes_processed", "total_slot_ms",
                         "total_bytes_billed"]
        dfbqh = self.df_bq_history.loc[row_select, column_select].copy()
        dfbqh.reset_index(inplace=True, drop=True)
        dfbqh.columns = ["bq_" + col for col in dfbqh.columns]
        dfbqh.rename({"bq_job_id": "qid",
                      "bq_start_time": "start_time",
                      "bq_end_time": "end_time",
                      "bq_total_bytes_processed": "bytes",
                      "bq_total_bytes_billed": "bq_bytes_billed"}, axis=1, inplace=True)
        dfbqh.start_time = pd.to_datetime(dfbqh.start_time)
        dfbqh.end_time = pd.to_datetime(dfbqh.end_time)

        # combined history relevant to the dfq records
        dfh = pd.concat([dfsfh, dfbqh], axis=0)

        # final target DataFrame for results
        df = dfq.merge(right=dfh, how='left', on='qid')

        df["dt"] = df.end_time - df.start_time
        df.dt = df.dt.dt.total_seconds()

        df.driver_t0 = pd.to_datetime(df.driver_t0)
        df.driver_t1 = pd.to_datetime(df.driver_t1)

        df["dt_local"] = df.driver_t1 - df.driver_t0
        df.dt_local = df.dt_local.dt.total_seconds()

        ## Time Elapsed
        df["dt_sys"] = pd.NaT

        # Snowflake
        # TOTAL_ELAPSED_TIME    NUMBER    Elapsed time (in milliseconds).

        # BigQuery
        # total_slot_ms    INTEGER    Slot-milliseconds for the job over its entire duration.

        # Conversion is the same for both:
        # dt is a TimeDelta converted to total seconds (dt.total_seconds() method)
        # so to create something comparable, convert both from milliseconds to seconds

        df.loc[df.db == "sf", "dt_sys"] = df.loc[df.db == "sf", "sf_elapsed"] / 1000.0
        df.loc[df.db == "bq", "dt_sys"] = df.loc[df.db == "bq", "bq_total_slot_ms"] / 1000.0

        ## TeraBytes Processed

        # Snowflake
        # BYTES_SCANNED    NUMBER    Number of bytes scanned by this statement

        # BigQuery
        # total_bytes_processed    INTEGER    Total bytes processed by the job.

        # Conversion is the same for both:
        # 1 TeraByte (TB) = 1e12 bytes
        df["TB"] = df.bytes / 1e12

        ## Cost

        # Snowflake
        # CREDITS_USED_CLOUD_SERVICES    NUMBER    Number of credits used for cloud services in the hour.
        # Conversion:
        # Enterprise account, $3.00 per Snowflake credit
        dollars_per_credit = 3.00

        # BigQuery
        # total_bytes_processed    INTEGER    Total bytes processed by the job.
        # Conversion:
        # Using the Flex-slot commit cost (60 second min commit = $4.00/60 = $0.07)
        # rate = $4.00 per (100 slots)/(1 hour)
        # number of slots reserved: `slots`
        # seconds of billed time in seconds:`ts`
        # cost of billed time and slot reservation:
        # 4.00 * math.ceil(slots / 100) * (math.ceil(ts) / (60 * 60))
        df["cost"] = 0

        df.loc[df.db == "sf", "cost"] = \
            df.loc[df.db == "sf", "sf_credits"] * dollars_per_credit

        df.loc[df.db == "bq", "cost"] = \
            df.loc[df.db == "bq", "bq_total_slot_ms"].apply(lambda x: bq_reserve_cost(x, slots=2000) / 1000)

        ## Pivot Final Results

        dfp_TB = df.pivot_table(index="query_n", columns=["db", "desc"], values="TB")
        dfp_dt = df.pivot_table(index="query_n", columns=["db", "desc"], values="dt")
        dfp_cost = df.pivot_table(index="query_n", columns=["db", "desc"], values="cost")

        ## Aggregate Sum Results
        df_agg = pd.concat({"dt": dfp_dt.sum(), "TB": dfp_TB.sum(), "cost": dfp_cost.sum()}, axis=1)

        ## Make class attributes
        self.dfp_TB = dfp_TB
        self.dfp_dt = dfp_dt
        self.dfp_cost = dfp_cost
        self.df_agg = df_agg

        ## Save results
        self.dfp_TB.to_csv(self.results_dir + config.sep + "dfp_TB.csv")
        self.dfp_dt.to_csv(self.results_dir + config.sep + "dfp_dt.csv")
        self.dfp_cost.to_csv(self.results_dir + config.sep + "dfp_cost.csv")
        self.df_agg.to_csv(self.results_dir + config.sep + "df_agg.csv")

    def query_heatmap(self, dfx, dtype):
        """

        Parameters
        ----------
        dtype : str, either "dt", "TB" or "cost
        """

        scaled_df = (dfx - dfx.min(axis=0)) / (dfx.max(axis=0) - dfx.min(axis=0))
        if len(dfx) > 25:
            h = 40
        else:
            h = 18

        w_mapper  = {"dt": 6, "TB": 12, "cost": 12}
        fmt_mapper = {"dt": ".2f", "TB": ".6f", "cost": ".6f"}

        plt.figure(figsize=(w_mapper[dtype], h))

        ax = sns.heatmap(scaled_df, annot=dfx, fmt=fmt_mapper[dtype],
                         # vmin=0, vmax=0.5,
                         cmap="viridis", cbar=False)
        ax.xaxis.set_ticks_position('top')
        ax.set_xticklabels(ax.get_xticklabels(), rotation=45)

        plot_fp = f"plot_heatmap_query_{dtype}.png"
        plt.tight_layout()
        plt.savefig(self.results_dir + config.sep + plot_fp, bbox_to_anchor='tight')

    def save_heatmaps(self):

        # aggregate heatmap
        dfx = self.df_agg
        scaled_df = (dfx - dfx.min(axis=0)) / (dfx.max(axis=0) - dfx.min(axis=0))
        # transpose both so the x-axis labels match the per-type heatmaps
        ax = sns.heatmap(scaled_df.T, annot=dfx.T, fmt=".3f",
                         linewidths=.5,
                         cmap="viridis", cbar=False)
        ax.xaxis.set_ticks_position('top')
        ax.set_xticklabels(ax.get_xticklabels(), rotation=45)

        plot_fp = "plot_query_heatmap.png"
        plt.tight_layout()
        plt.savefig(self.results_dir + config.sep + plot_fp)  #, bbox_to_anchor='tight')

        # per-type heatmaps
        for _dtype, _df in zip(["dt", "TB", "cost"], [self.dfp_dt, self.dfp_TB, self.dfp_cost]):
            self.query_heatmap(dfx=_df, dtype=_dtype)
