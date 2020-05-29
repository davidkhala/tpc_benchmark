"""Quality Control methods on query results 

Colin Dietrich, SADA 2020
"""

import glob

import pandas as pd
from pandas.testing import assert_frame_equal

import sfa, bqa, tools, config


def splitter(fp):
    x = fp.split(".")
    y = x[-2].split("_")[-1]
    return y

def equals(df1, df2):
    try:
        assert_frame_equal(df1, df2, check_names=False)
        return True
    except AssertionError:
        return False
    
def equals_csv(fp1, fp2):
    _df1 = pd.read_csv(fp1)
    _df2 = pd.read_csv(fp2)
    try:
        _df1 = tools.to_consistent(df=_df1, n=config.truncate_float_to)
        _df2 = tools.to_consistent(df=_df2, n=config.truncate_float_to)
        assert_frame_equal(_df1, _df2, 
                           check_names=False,
                           check_exact=False,
                           check_less_precise=1)
        return True
    except AssertionError:
        return False


def compare_results(results_dir):
    """Compare the CSV results from a dual SF/BQ query sequence
    
    Parameters
    ----------
    results_dir : str, abs path to folder that contains results files
    
    Returns
    -------
    df : Pandas Dataframe, filepaths to each results file 
    result : Pandas Series, bool if results were identical according 
        to equals_csv function
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
    
    result = df.apply(lambda r: equals_csv(r.fp_bq, r.fp_sf), axis=1)
    
    return df, result


def print_head(df, n, n0, n1):
    """Print head middle and tail values of Pandas Dataframe"""
    df_bq = pd.read_csv(df.loc[n, "fp_bq"])
    df_sf = pd.read_csv(df.loc[n, "fp_sf"])

    df_bq = tools.to_consistent(df=df_bq, n=config.truncate_float_to)
    df_sf = tools.to_consistent(df=df_sf, n=config.truncate_float_to)

    #df_bq.fillna(value=-9999, inplace=True)
    #df_sf.fillna(value=-9999, inplace=True)
    #df_bq.columns = map(str.lower, df_bq.columns)
    #df_sf.columns = map(str.lower, df_sf.columns)

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
    print(df_bq.eq(df_sf))
    return df_bq, df_sf


class QueryQC:
    def __init__(self):
        
        self.test = None
        self.scale = None
        self.cid = None
        self.stream_n = None
        self.desc = None
        self.seq_id = None
        
        self.verbose = False
        self.verbose_query = False
        self.verbose_iter = False
        
        self.qual = False
        self.save = False
        
        self.shared_timestamp = None
        self.results_dir = None
        
        self.skip_queries = None
        
        self.results_sf_csv_fp = None
        self.results_bq_csv_fp = None
        
    def set_timestamp_dir(self):
        self.shared_timestamp = pd.Timestamp.now()  # "UTC"
        self.shared_timestamp = str(self.shared_timestamp).replace(" ", "_")

        self.results_dir, _ = tools.make_name(db="bqsf",
                                              test=self.test,
                                              cid=self.cid,
                                              kind="results",
                                              datasource="", 
                                              desc=self.desc, ext="", 
                                              timestamp=self.shared_timestamp)
        #if self.verbose:
        print("Result Folder Name:")
        print(self.results_dir)
            
    def run_single(self, query_n):
        seq = [query_n]
            
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
        sf.query_seq(seq=seq, 
                     seq_id=self.seq_id, 
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

        bq.timestamp = self.shared_timestamp
        bq.results_dir = self.results_dir

        bq.query_seq(seq,
                     seq_id=self.seq_id,
                     qual=self.qual,
                     save=self.save,
                     verbose_iter=self.verbose_iter)

        self.results_bq_csv_fp = bq.results_csv_fp
        
    def compare(self):
        df, result = compare_results(self.results_dir)
        
        print("-"*30)
        print(result[1], "<<< DataFrames Match")
        print("-"*30)
        print()
        
        if len(df) == 1:
            n = 1
        mid = int(len(df)/2)
        df_bq, df_sf = print_head(df=df, n=n, n0=mid, n1=mid+5)
        
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
        
        return df_bq, df_sf