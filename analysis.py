"""Compile and analyze TPC benchmark results"""

import glob

import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

import config


def bytes_to_TebiByte(b):
    if pd.isnull(b):
        return None
    else:
        return b / 1099511627776


def to_int(x):
    try:
        return int(x)
    except:
        return None


def bytes_to_TeraByte(b):
    if pd.isnull(b):
        return None
    else:
        return float(b) / 1e12


def TebiBytes_to_dollars(tib):
    if pd.isnull(tib):
        return None
    else:
        return tib * config.sf_dollars_per_tebibyte


class Results:
    """Summarize data from one results directory"""
    def __init__(self, results_dir):
        self.results_dir = results_dir

        self.sf_results_csv_fp = glob.glob(self.results_dir +
                                           config.sep +
                                           "benchmark_times_sf*.csv")[0]
        self.bq_results_csv_fp = glob.glob(self.results_dir +
                                           config.sep +
                                           "benchmark_times_bq*.csv")[0]

        self.df_sf_history_av_fp = self.results_dir + config.sep + "query_history_sf.csv"
        self.df_bq_history_fp    = self.results_dir + config.sep + "query_history_bq.csv"

        self.dfsf = None
        self.dfbq = None
        self.df_sf_history_av = None
        self.df_bq_history = None
        self.dfsf_short = None
        self.dfbq_short = None
        self.df = None

        # metadata from the file names
        self.test = None
        self.scale = None
        self.query_stream_number = None
        self.meta_data = None

    def load(self):
        """Load data from csv files into Pandas DataFrames"""
        self.dfsf = pd.read_csv(self.sf_results_csv_fp)
        self.dfbq = pd.read_csv(self.bq_results_csv_fp)

        self.df_sf_history_av = pd.read_csv(self.df_sf_history_av_fp)
        self.df_bq_history = pd.read_csv(self.df_bq_history_fp)

        self.df_sf_history_av.START_TIME = pd.to_datetime(self.df_sf_history_av.START_TIME)
        self.df_sf_history_av.END_TIME = pd.to_datetime(self.df_sf_history_av.END_TIME)

        self.df_bq_history.start_time = pd.to_datetime(self.df_bq_history.start_time)
        self.df_bq_history.end_time = pd.to_datetime(self.df_bq_history.end_time)

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
        mask = self.df_sf_history_av.QUERY_ID == row["qid"]
        out = self.df_sf_history_av.loc[mask, value]
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
        if out.values.shape == (1,):
            return out.values[0]
        else:
            return None

    def append_history(self):
        """Append query history data to the basic data collected during benchmarking"""
        for col in config.sf_extended_keep:
            self.dfsf[col.lower()] = None

        for col in config.sf_extended_keep:
            self.dfsf[col.lower()] = self.dfsf.apply(lambda x: self.row_extract_sf(row=x, value=col), axis=1)

        for col in config.bq_keep:
            self.dfbq[col.lower()] = None

        for col in config.bq_keep:
            self.dfbq[col.lower()] = self.dfbq.apply(lambda x: self.row_extract_bq(row=x, value=col), axis=1)

    def calculate(self):
        """Calculate derived values from history data"""

        ## Snowflake
        self.dfsf["dt"] = self.dfsf.end_time - self.dfsf.start_time
        self.dfsf["dt_s"] = self.dfsf.dt.dt.total_seconds()
        self.dfsf["Tib"] = self.dfsf.bytes_scanned.apply(to_int)
        self.dfsf["TB"] = self.dfsf.bytes_scanned.apply(bytes_to_TeraByte)

        #  on demand billing
        self.dfsf["cost_store_od"] = (self.dfsf.scale / 1000) * 40

        # tiers of service and their costs
        self.dfsf["cost_comp_od_std"] = self.dfsf.credits_used_cloud_services * 2
        self.dfsf["cost_comp_od_ent"] = self.dfsf.credits_used_cloud_services * 3
        self.dfsf["cost_comp_od_bus"] = self.dfsf.credits_used_cloud_services * 4

        ## BigQuery
        self.dfbq["dt"] = self.dfbq.end_time - self.dfbq.start_time
        self.dfbq["dt_s"] = self.dfbq.dt.dt.total_seconds()
        self.dfbq["Tib"] = self.dfbq.total_bytes_processed.apply(bytes_to_TebiByte)
        self.dfbq["TB"] = self.dfbq.total_bytes_processed.apply(bytes_to_TeraByte)

        self.dfbq["cost_comp"] = self.dfbq.TB * config.bq_dollars_per_terabyte

        ## Combined
        self.dfsf_short = self.dfsf[config.summary_short_columns]  # + ["percentage_scanned_from_cache"]]
        self.dfbq_short = self.dfbq[config.summary_short_columns]

        self.df = pd.concat([self.dfsf_short, self.dfbq_short])
        self.df.reset_index(inplace=True, drop=True)

        # all of these records will be the same so just grab the 1st
        self.test = self.df.loc[0, "test"]
        self.scale = self.df.loc[0, "scale"]
        self.query_stream_number = self.df.loc[0, "seq_n"]
        self.meta_data = self.bq_results_csv_fp.split(config.sep)[-1].split("_")

        # save totals back into same source directory
        all_fp = (self.results_dir + config.sep +
                  "benchmark_results_" +
                  "_".join(self.meta_data[3:]))
        self.df.to_csv(all_fp, index=False)

    def total(self, drop=None, suffix=None, save=True, verbose=False):
        """Total TPC timing and processed bytes

        Parameters
        ----------
        # df : Pandas DataFrame, all timing data
        # results_dir : str, results directory to save summary to
        drop : list, queries to not include in plot
        suffix : str, additional label for summary output .csv
        save : bool, True to save to .csv
        verbose : bool, print debug statements

        Returns
        -------
        Pandas DataFrame, groupby summed numerical data
        """

        # all of these records will be the same so just grab the 1st
        #test = self.df.loc[0, "test"]
        #scale = self.df.loc[0, "scale"]
        #query_stream_number = self.df.loc[0, "seq_n"]
        #meta_data = self.bq_results_csv_fp.split(config.sep)[-1].split("_")

        if verbose:
            print("Meta Data:")
            print(self.meta_data)
            print()

        if drop is not None:
            _df = self.df[~(self.df.query_n.isin(drop))]
        else:
            _df = self.df

        # groupby and sum the numerical data
        df_summary = _df[["db", "test", "dt_s", "TB"]].groupby(by=["db", "test"]).sum()

        df_summary.reset_index(inplace=True)
        df_summary.db = df_summary.db.str.upper()
        df_summary.test = df_summary.test.str.upper()
        df_summary["scale"] = self.meta_data[4]
        df_summary["desc"] = self.meta_data[6]

        # usual printout option
        if verbose:
            print("Test: TPC-{}".format(self.test.upper()))
            print("Scale Factor: {}GB".format(self.scale))
            print("Query Stream Number: {}".format(self.query_stream_number))
            print()
            print("Key:")
            print("  ds_s: elapsed time in seconds")
            print("  TB: data processed in Terabytes")
            print()
            print("Summary:")
            print(df_summary)

        if suffix is None:
            suffix = "all_"
        else:
            suffix = "_" + suffix

        # save totals back into same source directory
        _id = '_'.join(self.meta_data[3:]).split(".")[0]
        total_fp = (self.results_dir + config.sep +
                    "benchmark_total_" + _id  + suffix + ".csv")
        if save:
            df_summary.to_csv(total_fp, index=False)

        return df_summary

    def plot_total_time(self, df_summary, suffix=None):
        # all of these records will be the same so just grab the 1st
        #test = self.df.loc[0, "test"]
        #scale = self.df.loc[0, "scale"]
        #query_stream_number = self.df.loc[0, "seq_n"]

        ax = df_summary[["db", "dt_s"]].plot.bar(x="db", rot=0, color="grey")

        if suffix is None:
            suffix = ""
        else:
            suffix = " " + suffix

        ax.set_xlabel("TPC-{} Stream {} at {}GB Scale {}".format(self.test.upper(),
                                                                 self.query_stream_number,
                                                                 self.scale,
                                                                 suffix))
        ax.set_ylabel("Time in Seconds")
        ax.get_legend().remove()
        return ax

    def plot_total_bytes(self, df_summary, suffix=None):
        # all of these records will be the same so just grab the 1st
        #test = self.df.loc[0, "test"]
        #scale = self.df.loc[0, "scale"]
        #query_stream_number = self.df.loc[0, "seq_n"]

        ax = df_summary[["db", "TB"]].plot.bar(x="db", rot=0, color="purple")

        if suffix is None:
            suffix = ""
        else:
            suffix = " " + suffix

        ax.set_xlabel("TPC-{} Stream {} at {}GB Scale {}".format(self.test.upper(),
                                                                 self.query_stream_number,
                                                                 self.scale,
                                                                 suffix))
        ax.set_ylabel("Data processed in Terabytes")
        ax.get_legend().remove()
        return ax

    def plot_total_time_bytes(self, df_plot, suffix=None, y1lim=None, y2lim=None):
        """Plot the total time and bytes processed per system under test"""

        # name file and add suffix if needed
        if suffix is None:
            suffix = ""
        else:
            suffix = "_" + suffix

        ax = df_plot.plot.bar(secondary_y="TB", rot=0, figsize=(8, 6), color=["grey", "purple"])
        ax.set_xticklabels(df_plot.db)
        ax1, ax2 = plt.gcf().get_axes()  # gets the current figure and then the axes
        ax1.set_ylabel("Time in Seconds")
        ax1.grid(False)
        ax2.set_ylabel("Data processed in Terabytes")
        ax2.grid(False)
        ax2.set_xlabel(f"Totals for TPC-{self.test.upper()} Stream " +
                       f"{self.query_stream_number} at {self.scale}GB Scale {suffix}")

        _id = '_'.join(self.meta_data[3:]).split(".")[0]
        plot_fp = f"plot_totals_{_id}{suffix}.png"
        plt.savefig(self.results_dir + config.sep + plot_fp, bbox_to_anchor='tight')

    def per_query_plot(self, df=None, drop=None, suffix=None):
        """Plot per-query information

        Parameters
        ----------
        df : Pandas DataFrame
        drop : list, queries to not include in plot
        suffix : str, additional label for summary output .csv

        Returns
        -------
        None, saves to disk and plots in Notebook
        """

        if df is None:
            df = self.df

        if drop is not None:
            df = df[~(df.query_n.isin(drop))]

        dfp_dt = df.pivot(index="query_n", columns="db", values="dt_s")
        dfp_bp = df.pivot(index="query_n", columns="db", values="TB")

        # style setup
        sns.set_style("darkgrid", {"xtick.bottom": True})

        # name file and add suffix if needed
        if suffix is None:
            suffix = ""
        else:
            suffix = "_" + suffix

        color_palette = sns.hls_palette(n_colors=12)
        fig, (ax1, ax2) = plt.subplots(2, 1)
        ax1 = dfp_dt.plot.bar(ax=ax1, legend=False, color=color_palette)

        # invert bytes processed for plotting
        _dfp_bp = dfp_bp.copy() * -1
        ax2 = _dfp_bp.plot.bar(ax=ax2, legend=False, color=color_palette)

        # set the figure size BEFORE adjusting ticks
        fig.set_size_inches(30, 8, forward=True)

        handles, labels = ax1.get_legend_handles_labels()

        ax1.set_ylabel("Time in Seconds")
        ax1.set_xlabel(None)
        ax1.xticklabels = ax1.get_xticklabels()
        ax1.set_xticklabels(labels=[])

        ax2.set_ylabel("TiB Processed")
        ax2.set_xlabel("Query Number")
        ticks = ax2.get_yticks()
        ticks = ["{:01.4f}".format(abs(tick)) for tick in ticks]
        ax2.set_yticklabels(ticks)
        ax2.xaxis.tick_top()

        plt.subplots_adjust(hspace=0.15)
        plt.subplots_adjust(right=0.97)

        fig.legend(handles, labels, loc="right")

        _id = '_'.join(self.meta_data[3:]).split(".")[0]
        plot_fp = f"plot_query_comparison_{_id}{suffix}.png"
        plt.savefig(self.results_dir + config.sep + plot_fp, bbox_to_anchor='tight')

        return fig
