"""Quality Control methods on query results 

Colin Dietrich, SADA 2020
"""

import glob
import json

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from pandas.testing import assert_frame_equal

import sfa, bqa, tools, config


def splitter(fp):
    x = fp.split(".")
    y = x[-2].split("_")[-1]
    return y


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


def collate_results(results_dir):
    """Compare the CSV results from a dual SF/BQ query sequence
    
    Parameters
    ----------
    results_dir : str, abs path to folder that contains results files
    
    Returns
    -------
    df : Pandas Dataframe, filepaths to each results file 
    """
    
    fps_query_sf = glob.glob(results_dir + config.sep + "query_result_sf*")
    fps_query_bq = glob.glob(results_dir + config.sep + "query_result_bq*")
    
    dfbq = pd.DataFrame([[fp, int(splitter(fp))] for fp in fps_query_bq],
                        columns=["fp_bq", "q_bq"])
    
    dfsf = pd.DataFrame([[fp, int(splitter(fp))] for fp in fps_query_sf],
                        columns=["fp_sf", "q_sf"])
    
    dfbq.sort_values(by="q_bq", inplace=True)
    dfbq.reset_index(inplace=True, drop=True)
    
    dfsf.sort_values(by="q_sf", inplace=True)
    dfsf.reset_index(inplace=True, drop=True)
    
    df = pd.concat([dfbq, dfsf], axis=1)
    
    df.index = df.index + 1
    
    return df


def print_dfs(df_bq, df_sf, n0, n1):
    """Print benchmark quality control calculations

    Parameters
    ----------
    df_bq : Pandas DataFrame, BigQuery query result
    df_sf : Pandas DataFrame, Snowflake query result
    n0 : int, row start for mid-DataFrame preview
    n1 : int, row end for mid-DataFrame preview

    Returns
    -------
    None, prints only
    """
    print("BQ:")
    print(df_bq.head())
    print("-"*40)
    print("SF:")
    print(df_sf.head())
    print()
    print("="*40)
    print()
    print("BQ:")
    print(df_bq.loc[n0:n1])
    print("-"*40)
    print("SF:")
    print(df_sf.loc[n0:n1])
    print()
    print("="*40)
    print()
    print("BQ:")
    print(df_bq.tail())
    print("-"*40)
    print("SF:")
    print(df_sf.tail())
    print()
    print("="*40)
    print()
    _df = df_bq.eq(df_sf)
    print()
    if len(_df) < 25:
        print("Result:")
        print("-------")
        print(_df)
        print()
    else:
        print("Head of Result:")
        print("---------------")
        print(_df.head(10))
        print()


class QueryCompare:
    def __init__(self):
        
        self.test = None
        self.scale = None
        self.cid = None
        self.stream_n = None
        self.query_sequence = None
        self.desc = None
        self.data_source = None

        self.verbose = False
        self.verbose_query = False
        self.verbose_query_n = False  # print line numbers in query text
        self.verbose_iter = False

        self.cache = False
        self.qual = False
        self.save = False
        
        self.shared_timestamp = None
        self.results_dir = None
        
        self.skip_queries = None
        
        self.results_sf_csv_fp = None
        self.results_bq_csv_fp = None

        self.result_bq = None
        self.result_sf = None

        # only used in single query comparison
        self.result_bq_csv = None
        self.result_sf_csv = None

        # useful to be able to set if running multiple benchmarks
        self.sf_warehouse_name = config.sf_warehouse[0]

        self.sf_warehouse_size = None  # strictly depends on warehouse name

        # for understanding the initial metadata record snapshot
        self.test_stage = "initialization"

    def values(self):
        """Get all class attributes from __dict__ attribute
        except those prefixed with underscore ('_')

        Returns
        -------
        dict, of (attribute: value) pairs
        """

        skip_attributes = ["result_bq", "result_sf"]
        d = {}
        for k, v in self.__dict__.items():
            if (k[0] != "_") and (k not in skip_attributes):
                d[k] = v
        return d

    def to_json(self, indent=None):
        """Return all class objects from __dict__ except
        those prefixed with underscore ('_')

        Paramters
        ---------
        indent : None or non-negative integer or string, then JSON array
        elements and object members will be pretty-printed with that indent level.

        Returns
        -------
        str, JSON formatted (attribute: value) pairs
        """
        return json.dumps(self, default=lambda o: o.values(),
                          sort_keys=True, indent=indent)

    def set_timestamp_dir(self):
        self.shared_timestamp = pd.Timestamp.now()  # "UTC"
        self.shared_timestamp = str(self.shared_timestamp).replace(" ", "_")
        self.data_source = self.test + "_" + str(self.scale) + "GB_" + self.cid
        self.results_dir, _ = tools.make_name(db="bqsf",
                                              test=self.test,
                                              cid=self.cid,
                                              kind="results",
                                              datasource=self.data_source,
                                              desc=self.desc, ext="", 
                                              timestamp=self.shared_timestamp)
        tools.mkdir_safe(self.results_dir)

        if self.verbose:
            print("Result Folder Name:")
            print(self.results_dir)
            
    def run_single(self, query_n):
        seq = [query_n]
        self.run(seq)
        
    def run(self, seq):
        """Run a benchmark comparison

        Parameters
        ----------
        seq : list of int, query numbers to execute

        Returns
        -------
        None, writes multiple files to self.results_dir location
        """

        self.query_sequence = seq

        # stage 1 - start run
        self.test_stage = "start run"
        metadata_fp = self.results_dir + config.sep + "metadata_initial.json"
        with open(metadata_fp, "w") as f:
            f.write(self.to_json(indent="  "))

        sf = sfa.SFTPC(test=self.test,
                       scale=self.scale,
                       cid=self.cid,
                       warehouse=self.sf_warehouse_name,
                       desc=self.desc,
                       verbose=self.verbose,
                       verbose_query=self.verbose_query)
        sf.verbose_query_n = self.verbose_query_n
        
        if self.verbose:
            print('Using database:', sf.database)

        sf.timestamp = self.shared_timestamp
        sf.results_dir = self.results_dir

        sf.connect()

        # record what the SF warehouse size is
        query_result = sf.show_warehouses()
        warehouse_size_mapper = {r[0]: r[3] for r in query_result.fetchall()}
        self.sf_warehouse_size = warehouse_size_mapper[self.sf_warehouse_name]

        # update initial metadata so warehouse size is captured
        # stage 2 - connected to Snowflake and got warehouse size metadata
        self.test_stage = "Snowflake connected"
        metadata_fp = self.results_dir + config.sep + "metadata_initial.json"
        with open(metadata_fp, "w") as f:
            f.write(self.to_json(indent="  "))

        if self.cache:
            sf.cache_set("on")
        else:
            sf.cache_set("off")

        self.result_sf = sf.query_seq(seq=seq,
                                      seq_n=self.stream_n,
                                      qual=self.qual,
                                      save=self.save,
                                      verbose_iter=self.verbose_iter)
        sf.close()

        self.results_sf_csv_fp = sf.results_csv_fp
        
        bq = bqa.BQTPC(test=self.test,
                       scale=self.scale,
                       cid=self.cid,
                       desc=self.desc,
                       verbose_query=self.verbose_query,
                       verbose=self.verbose)
        bq.verbose_query_n = self.verbose_query_n

        bq.timestamp = self.shared_timestamp
        bq.results_dir = self.results_dir

        if self.cache:
            bq.cache_set("on")
        else:
            bq.cache_set("off")

        self.result_bq = bq.query_seq(seq,
                                      seq_n=self.stream_n,
                                      qual=self.qual,
                                      save=self.save,
                                      verbose_iter=self.verbose_iter)

        self.results_bq_csv_fp = bq.results_csv_fp

        # stage 3 - done with both systems
        self.test_stage = "SF and BQ done"
        metadata_fp = self.results_dir + config.sep + "metadata_final.json"
        metadata_final = self.to_json(indent="  ")
        with open(metadata_fp, "w") as f:
            f.write(metadata_final)

    def compare_sum(self):

        ds_col = {"call_center": "cc_call_center_sk",  # integer
                  "catalog_page": "cp_catalog_page_sk",
                  "catalog_returns": "cr_order_number",
                  "catalog_sales": "cs_order_number",
                  "customer": "c_customer_sk",
                  "customer_address": "ca_address_sk",
                  "customer_demographics": "cd_demo_sk",
                  "date_dim": "d_date_sk",  # integer
                  # skip dbgen
                  "household_demographics": "hd_demo_sk",
                  "income_band": "ib_income_band_sk",
                  "inventory": "inv_item_sk",  # integer
                  "item": "i_item_sk",
                  "promotion": "p_promo_sk",
                  "reason": "r_reason_sk",
                  "ship_mode": "sm_ship_mode_sk",
                  "store": "s_store_sk",
                  "store_returns": "sr_item_sk",
                  "store_sales": "ss_item_sk",
                  "time_dim": "t_time_sk",
                  "warehouse": "w_warehouse_sk",
                  "web_page": "wp_web_page_sk",
                  "web_returns": "wr_item_sk",
                  "web_sales": "ws_item_sk",
                  "web_site": "web_site_sk"}

        h_col = {"customer": "c_custkey",
                 "lineitem": "l_linenumber",
                 "nation": "n_nationkey",
                 "orders": "o_orderkey",
                 "part": "p_partkey",
                 "partsupp": "ps_partkey",
                 "region": "r_regionkey",
                 "supplier": "s_suppkey"}

        col_names = {"ds": ds_col, "h": h_col}[self.test]

        sf = sfa.SFTPC(test=self.test,
                       scale=self.scale,
                       cid=self.cid,
                       warehouse="TEST9000",
                       desc=self.desc,
                       verbose=self.verbose,
                       verbose_query=self.verbose_query)

        if self.verbose:
            print('Using database:', sf.database)

        sf.timestamp = self.shared_timestamp
        sf.results_dir = self.results_dir
        sf.connect()

        bq = bqa.BQTPC(test=self.test,
                       scale=self.scale,
                       cid=self.cid,
                       desc=self.desc,
                       verbose_query=self.verbose_query,
                       verbose=self.verbose)

        bq.timestamp = self.shared_timestamp
        bq.results_dir = self.results_dir

        d = []
        for table, column in col_names.items():
            if self.verbose_iter:
                print(f"TABLE & COLUMN: {table} >> {column}")

            query_text = f"select sum({column}) from {table}"

            sf_query_result = sf.sfc.query(query_text=query_text)
            df_sf_result = sf_query_result.fetch_pandas_all()
            df_sf_result.columns = ["r"]
            sf_r = df_sf_result.loc[0, "r"]

            bq_query_result = bq.query(query_text=query_text)
            df_bq_result = bq_query_result.result().to_dataframe()
            df_bq_result.columns = ["r"]
            bq_r = df_bq_result.loc[0, "r"]

            if self.verbose_iter:
                print("RESULT: SF | BQ")
                print("SF Type:", type(sf_r))
                print("BQ Type:", type(bq_r))
                print(sf_r, "|", bq_r)
                print("-" * 40)
                print()

            # type convert to assure numerical comparison
            # is the only comparison being done
            sf_r_a = np.int64(sf_r)
            bq_r_a = np.int64(bq_r)

            try:
                equal = sf_r_a == bq_r_a
            except TypeError:
                print("Error comparing query results.")
                print("SF Reply:")
                print(sf_r)
                print("-"*30)
                print(bq_r)
                print("-"*30)

            d.append([table, column, sf_r, bq_r, equal])
        sf.close()

        df = pd.DataFrame(d, columns=["table", "column", "sf", "bq", "equal"])

        db_name = self.test + "_" + "{:02d}".format(self.scale) + "_" + self.cid
        rdir, rfp = tools.make_name(db="bqsf", test=self.test, cid=self.cid,
                                    kind="qc-comparison",
                                    datasource=db_name,
                                    desc=self.desc,
                                    ext=".csv",
                                    timestamp=None)
        tools.mkdir_safe(rdir)
        fp = rdir + config.sep + rfp
        df.to_csv(fp, index=False)
        return df

    def table_metadata(self):
        """Compare table contents on both platforms
        TODO: the BQ and SF specific methods could probably be migrated to
        sfa.py and bqa.py

        For more details see:
        https://docs.snowflake.com/en/sql-reference/account-usage/tables.html
        https://cloud.google.com/bigquery/docs/information-schema-datasets

        Returns
        -------
        df_sf_results : Pandas DataFrame, recormatted view of query
            select * from snowflake.account_usage.tables

            TABLE_ID                                     object
            TABLE_NAME                                   object
            TABLE_SCHEMA_ID                              object
            TABLE_SCHEMA                                 object
            TABLE_CATALOG_ID                             object
            TABLE_CATALOG                                object
            TABLE_OWNER                                  object
            TABLE_TYPE                                   object
            IS_TRANSIENT                                 object
            CLUSTERING_KEY                               object
            ROW_COUNT                                    object
            BYTES                                        object
            RETENTION_TIME                               object
            SELF_REFERENCING_COLUMN_NAME                 object
            REFERENCE_GENERATION                         object
            USER_DEFINED_TYPE_CATALOG                    object
            USER_DEFINED_TYPE_SCHEMA                     object
            USER_DEFINED_TYPE_NAME                       object
            IS_INSERTABLE_INTO                           object
            IS_TYPED                                     object
            COMMIT_ACTION                                object
            CREATED                         datetime64[ns, UTC]
            LAST_ALTERED                    datetime64[ns, UTC]
            DELETED                         datetime64[ns, UTC]
            AUTO_CLUSTERING_ON                           object
            COMMENT                                      object

        df_bq_results : Pandas DataFrame, reformatted query view of
            select * from dataset.table.__TABLES___

            project_id            object, project id on GCP
            dataset_id            object, dataset name
            table_id              object, table name
            creation_time          int64,
            last_modified_time     int64,
            row_count              int64, number of rows in table
            size_bytes             int64, total stored size in bytes
            type                   int64,
        """
        sf = sfa.SFTPC(test=self.test,
                       scale=self.scale,
                       cid=self.cid,
                       warehouse="TEST9000",
                       desc=self.desc,
                       verbose=self.verbose,
                       verbose_query=self.verbose_query)
        
        if self.verbose:
            print('Using database:', sf.database)

        sf.timestamp = self.shared_timestamp
        sf.results_dir = self.results_dir

        sf.connect()
        query_text = "select * from snowflake.account_usage.tables"
        sf_query_result = sf.sfc.query(query_text=query_text)
        df_sf_result = sf_query_result.fetch_pandas_all()
        sf.close()

        bq = bqa.BQTPC(test=self.test,
                       scale=self.scale,
                       cid=self.cid,
                       desc=self.desc,
                       verbose_query=self.verbose_query,
                       verbose=self.verbose)

        bq.timestamp = self.shared_timestamp
        bq.results_dir = self.results_dir

        query_text = f"SELECT * FROM `{config.gcp_project.lower()}.INFORMATION_SCHEMA.SCHEMATA`"
        bq_query_result = bq.query(query_text=query_text)
        df_bq_tables = bq_query_result.result().to_dataframe()

        d = []
        for table_name in df_bq_tables.schema_name.unique():
            # INFORMATION_SCHEMA.COLUMNS
            query_text = f"SELECT * FROM `{config.gcp_project.lower()}.{table_name}.__TABLES__`"
            bq_query_result = bq.query(query_text=query_text)
            df_bq_result = bq_query_result.result().to_dataframe()
            d.append(df_bq_result)

        df_bq_result = pd.concat(d, axis=0)

        return df_sf_result, df_bq_result

    def table_compare(self):
        """
        
        Returns
        -------
        df : Pandas DataFrame, per table level matches
        df_all : Pandas DataFrame, per dataset level matches
        """

        df_sf_result, df_bq_result = self.table_metadata()

        df_bq_result["db"] = "bq"
        df_bq_result["dataset"] = df_bq_result.dataset_id.str.lower()
        df_bq_result["table"] = df_bq_result.table_id.str.lower()
        df_bq_result["rows"] = df_bq_result.row_count.astype(int)
        df_bq_result["bytes"] = df_bq_result.size_bytes.astype(int)
        df_bq_result["dataset_table"] = df_bq_result.dataset + "_" + df_bq_result.table
        df_bq_result["rows_bq"] = df_bq_result.rows
        df_bq_result["bytes_bq"] = df_bq_result.bytes
        df_bq = df_bq_result[["dataset", "table", "rows_bq", "bytes_bq"]]

        df_sf_result = df_sf_result.loc[pd.isnull(df_sf_result.DELETED)].copy()
        df_sf_result["db"] = "sf"
        df_sf_result["dataset"] = df_sf_result.TABLE_CATALOG.str.lower()
        df_sf_result["table"] = df_sf_result.TABLE_NAME.str.lower()
        df_sf_result["rows"] = df_sf_result.ROW_COUNT.astype(int)
        df_sf_result["bytes"] = df_sf_result.BYTES.astype(int)
        df_sf_result["dataset_table"] = df_sf_result.dataset + "_" + df_sf_result.table
        df_sf_result["rows_sf"] = df_sf_result.rows
        df_sf_result["bytes_sf"] = df_sf_result.bytes
        df_sf = df_sf_result[["dataset", "table", "rows_sf", "bytes_sf"]]

        df_all = df_bq.merge(df_sf, how='outer', on=['dataset', 'table'])

        for col in ["rows_bq", "bytes_bq", "rows_sf", "bytes_sf"]:
            df_all[col] = df_all[col].astype(pd.Int64Dtype())

        # edge case for dev test tables
        df_all = df_all[~df_all.dataset.str.contains("test")].copy()

        # where the row counts for a dataset are identical
        df_all["match"] = df_all.rows_bq == df_all.rows_sf

        df = df_all.groupby(by="dataset").sum()
        df["match"] = df_all.rows_bq == df_all.rows_sf

        return df, df_all

    def compare(self, plot=True, save=True):
        """Collate and compare TPC test results

        Parameters
        ----------
        plot : bool, generate and show plot
        save : bool, save file to self.results_dir

        Returns
        -------
        df_results : Pandas DataFrame, collate query comparison report
        """

        # is comprehension is derrivative of the tools.make_name call,
        # TODO: should probably unify file naming in one place
        name = "_".join([x for x in self.results_dir.split(config.sep) if x != ""][-1].split("_")[1:6])
        df = collate_results(self.results_dir)
        df["equal"] = apply_assert_equal(df)
        df["equal_percent"] = apply_percent_equal(df)

        if len(df) == 1:

            fp_df_bq = df.loc[1, "fp_bq"]
            fp_df_sf = df.loc[1, "fp_sf"]

            df_bq, df_sf = csv_consistent(fp_df1=fp_df_bq,
                                          fp_df2=fp_df_sf)
            self.result_bq_csv = df_bq
            self.result_sf_csv = df_sf

            mid = int(len(df_bq)/2)
            print_dfs(df_bq=df_bq, df_sf=df_sf, n0=mid, n1=mid + 5)

            c1 = " ".join([c for c in df_bq.columns])  # BQ
            c2 = " ".join([c for c in df_sf.columns])  # SF

            print()
            print("Columns")
            print("-------")
            print("BQ:")
            print(c1)
            print("SF:")
            print(c2)
            print() 

        _, qc_fp = tools.make_name(db="bqsf",
                                   test=self.test,
                                   cid=self.cid,
                                   kind="qc",
                                   datasource=self.data_source,
                                   desc=self.desc, ext=".csv",
                                   timestamp=self.shared_timestamp)
        if save:
            df.to_csv(self.results_dir + config.sep + "qc_" + name + ".csv")

        if plot:
            df["q"] = df["q_sf"]
            ax = df[["q", "equal_percent"]].plot.bar(x="q", figsize=(16, 4), color="grey")
            ax.set_xlabel("")
            ax.set_xlabel("TPC-{} Query Comparison".format(name))
            ax.set_ylabel("Percent Result Agreement")
            ax.get_legend().remove()
            plot_fp = f"plot_qc_{name}.png"
            plt.savefig(self.results_dir + config.sep + plot_fp, bbox_to_anchor='tight')
            plt.show()
        return df
