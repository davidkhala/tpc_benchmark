"""Load data from GCS into BigQuery and Snowflake

Colin Dietrich, SADA 2020
"""
import sys
import threading
import concurrent.futures

import numpy as np
import pandas as pd

from google.cloud import bigquery

import config, bq
from gcp_storage import inventory_bucket_df


def calc_stats(df):
    # generate upload statistics
    df.t0 = pd.to_datetime(df.t0)
    df.t1 = pd.to_datetime(df.t1)
    df["dt"] = df.t1 - df.t0
    df["dt_s"] = df.dt.dt.seconds + df.dt.dt.microseconds / 10**6
    df["size_megabytes"] = df.size_bytes / 2**20
    df["size_gigabytes"] = df.size_bytes / 10**9
    df["MB/s"] = df.size_megabytes / df.dt_s
    df["GB/s"] = df.size_gigabytes / df.dt_s
    return df


class BQUpload:
    
    def __init__(self, dataset_name, test, scale): 
                 #local_directory, local_base_directory=None, 
                 #pattern="*"):
        """Upload files from Google Cloud Storage to BigQuery.  Assumes files
        where generated by TPC-DS or TPC-H and uploaded to GCS with the following
        file name attributes separated by an underscore (_):
        test_scale_filename
        
        Example: h_1GB_supplier.tbl.6
        TPC-H, 1GB scale factor, orginal file name was 'supplier.tbl.6'
        
        Parameters
        ----------
        dataset_name : str, BigQuery dataset to upload to
        test : str, TPC test, either 'h' or 'ds'
        scale : int, TPC scale factor, i.e. 1, 100, 1000, 10000 etc in GB
        """

        self.bucket_name = config.gcs_data_bucket
        self.dataset_name = dataset_name

        self.test = test
        self.scale = scale
        
        self.verbose = False
        
        self.bucket_blobs = None
        self.inventory_data = None
        self.df = None
        
    def inventory_bucket(self):
        self.df = inventory_bucket_df(self.bucket_name)
        self.df["t0"] = ""
        self.df["t1"] = ""
        self.df["done"] = ""
    
    def subset_df(self):
        self.df = self.df.loc[(self.df.test == self.test) & 
                              (self.df.scale == str(self.scale)+"GB")]
    
    def upload_item(self, table_name, uri):
        
        bq_client = bigquery.Client.from_service_account_json(config.gcp_cred_file)
        
        up = bq.BQUpload(client=bq_client,
                         project=config.gcp_project.lower(),
                         dataset=self.dataset_name)
        
        try:
            load_job = up.upload_uri_csv(table=table_name,
                                         gs_path=uri,
                                         verbose=self.verbose)
        except:
            e = sys.exc_info()[0]
            print("Exception processing:")
            print(table_name, uri)
            print("-"*20)
            print(e)
            
        while not load_job.done():
            print("Still loading...")
            
        return load_job
        
    def apply_upload(self, row):
        
        uri = row.uri
        table_name = row.table
        if self.test == "h":
            table_name = table_name.upper()
            
        for ig in config.ignore_files:
            if ig in row.chunk_name:
                row.table = "NA - skipped"
                return row
            
        t0 = str(pd.Timestamp.now())
        load_job = self.upload_item(table_name=table_name, uri=uri)
        t1 = str(pd.Timestamp.now())
        row.t0 = t0
        row.t1 = t1
        row.job_id = load_job.job_id
        row.done = load_job.done()
        if self.verbose:
            print("Loading: {}".format(row.chunk_name))
            print("t0: {}".format(row.t0))
            print("t1: {}".format(row.t1))
            print("Done: {}".format(row.done))
            print("-"*30)
        return row
        
    def upload(self):
        """Apply upload method"""
        self.df = self.df.apply(self.apply_upload, axis=1)
        self.df = calc_stats(self.df)

        
class BQPooledUpload:
    
    def __init__(self, dataset_name, test, scale, n=None): 
        
        self.bucket_name = config.gcs_data_bucket
        
        self.dataset_name = dataset_name
        self.test = test
        self.scale = scale
        
        self.verbose = False
        
        self.base_up = BQUpload(dataset_name=self.dataset_name, 
                                test=self.test, 
                                scale=self.scale)
        self.base_up.verbose = self.verbose
        
        # inventory, compile and subset the blob list to this test and scale
        self.base_up.inventory_bucket()
        
        #self.base_up.compile_df()
        self.base_up.subset_df()
        
        self.df = self.base_up.df
        self.df["file_n"] = self.df.index.values
        self.df.reset_index(inplace=True, drop=True)
        
        self.n = n
        if self.n is None:
            self.n = config.cpu_count
        
        self.df_chunks = None
        self.up_data = []  # will be a list of dataframes
        
        self.up_lock = threading.Lock()
        
    def chunk_df(self):
        
        ix_split = np.array_split(self.df.index, self.n)
        list_df = [self.df.loc[mask].copy() for mask in ix_split]
        return list_df
    
    def chunk_upload(self, df):
        #print("chunk_upload...")
        up = BQUpload(dataset_name=self.dataset_name, 
                      test=self.test, 
                      scale=self.scale)
        up.verbose = self.verbose
        #if self.verbose:
        #print("BQPooledUpload.chunk_upload >> ", len(df))
        if len(df) > 0:  # handle situations where there are more threads than files
            up.df = df
            up.upload()
            with self.up_lock:
                self.up_data.append(up.df)
        
    def pipeline(self):
        list_df = self.chunk_df()
        with concurrent.futures.ThreadPoolExecutor(max_workers=self.n) as executor:
            results = executor.map(self.chunk_upload, list_df)
        return results
        #self.df = pd.concat(self.up_data)
        
    