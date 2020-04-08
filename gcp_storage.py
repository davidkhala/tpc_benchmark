"""GCP Storage local and remote file sync
Requires a client be already instanced
"""

import io
import os
import time
import datetime
import threading
import concurrent.futures

import gcsfs
import pandas as pd
import numpy as np

from google.auth.transport.requests import AuthorizedSession
from google.cloud import storage
from google.resumable_media import requests, common

import config, tools


"""log formats:
upload start: dt, 'start', file_name,           ,          ,               ,            , f_size,           , bucket_name
upload chunk: dt, 'chunk', file_name, chunk_size, http_code,               ,            ,       ,           ,
upload done:  dt, 'done',  file_name,           ,          , bytes_uploaded, total_bytes,       ,           ,
upload error: dt, 'error', file_name,           ,          ,               ,            ,       , error_name,
"""

log_format = ["dt", "action", "file", "chunk_size", 
              "http_code", "bytes_uploaded", "total_bytes", 
              "f_size", "exception", "bucket"]


def parser(data_list):
    _df = pd.DataFrame(data_list, columns=log_format)
    return _df


def inventory_bucket(gcs_client, bucket_name):
    """Inventory a GCS bucket and return all blob information as 
    they relate to TPC benchmarks.
    
    Parameters
    ----------
    gcs_client : authenticated Google Cloud Storage client instance
    bucket_name : str, name of bucket within the client service domain
    
    Returns
    -------
    Pandas DataFrame
    """
    
    b = FolderSync(client=gcs_client,
                   bucket_name=bucket_name,
                   local_directory=config.fp_h_output, # just placeholder
                   local_base_directory=None)
    
    b.inventory_bucket()
    
    data = [(_b.name, _b.public_url, _b.size) for _b in b.bucket_blobs]
    
    df = pd.DataFrame(data, columns=["chunk_name", "url", "size"])
    df["uri"] = df.url.apply(lambda x: x.replace("https://storage.googleapis.com/", "gs://"))
    df["name"] = df.chunk_name.str.split(".").apply(lambda x: x[0])
    
    df_name = df["name"].str.split("_", 3, expand=True)
    df_name.columns = ["test", "scale", "table"]
    
    df = pd.concat([df, df_name], axis=1, sort=False)
    
    df["t0"] = ""
    df["t1"] = ""
    df["status"] = ""
    
    return df


class BlobSync:
    """Sync GCP Storage Blob with local file location"""
    def __init__(self, client, bucket_name, local_filepath, blob_name=None):
        """
        Parameters
        ----------
        client : GCP storage client instance
        bucket_name : str, bucket name to access
        local_filepath : str, path of local file to upload
        blob_name : str, name of blob to create
        """
        
        self.client = client
        self.bucket_name = bucket_name
        self.bucket = self.client.get_bucket(self.bucket_name)
        
        self.blob = None
        self.local_filepath = local_filepath
        if blob_name is None:
            self.blob_name = self.filename_extract(self.local_filepath)
        else:
            self.blob_name = blob_name
            
        self.content_type = "text/csv"  # override this for other files
        self.chunk_size = 256*1024  # min allowed chunk size (TODO: add reference)
        
        self.log = []
        
    def filename_extract(self, filepath):
        fp = filepath.split(config.sep)
        return fp[-1]
        
    def print_blob_info(self):
        """Get blob metadata"""
        print("Name: {}".format(self.blob.id))
        print("Size: {} bytes".format(self.blob.size))
        print("Content type: {}".format(self.blob.content_type))
        print("Public URL: {}".format(self.blob.public_url))
        print("URI: {}".format(self.uri))
    
    def upload(self, verbose=False):
        """Upload a local file to a blob on GCP Storage
        
        Parameters
        ----------
        verbose : bool, print operation status
        """
        self.blob = self.bucket.blob(self.blob_name)
        self.blob.upload_from_filename(self.local_filepath,
                                       client=self.client)
        
        self.uri = self.blob.public_url.replace("https://storage.googleapis.com/", "gs://")
        
        if verbose:
            print("File uploaded to {}.".format(self.bucket.name))
            
    def upload_resumable(self, verbose=False):
        """Upload a local file to a blob on GCP Storage using a
        resumable connection.
        
        Parameters
        ----------
        verbose : bool, print operation status
        """
        
        url = (f'https://www.googleapis.com/upload/storage/v1/b/'
               f'{self.bucket_name}/o?uploadType=resumable'
               )

        if verbose:
            print("BlobSync.upload_resumable to: {}".format(url))
            
        upload = requests.ResumableUpload(upload_url=url, chunk_size=self.chunk_size)
        
        stream = io.FileIO(self.local_filepath, mode='r')
        
        transport = AuthorizedSession(credentials=self.client._credentials)
        
        upload.initiate(transport=transport,
                        content_type='application/octet-stream',
                        stream=stream,
                        metadata={'name': self.blob_name}
                        )
        file_name = os.path.basename(self.local_filepath)
        
        # start upload
        dt = datetime.datetime.now().isoformat()
        f_size = os.path.getsize(self.local_filepath)
        log_line = [dt, "start", file_name, "", 
                    "", "", "", f_size, "" , self.bucket_name]
        self.log.append(log_line)
        if verbose:
            print(" ".join([str(s) for s in log_line]))
        
        # send one chunk of chunk_size
        while not upload.finished:
            try:
                response = upload.transmit_next_chunk(transport)
                dt = datetime.datetime.now().isoformat()
                log_line = [dt, "chunk", file_name, self.chunk_size, 
                            response.status_code, "", "", "", "", ""]
                self.log.append(log_line)
                if verbose:
                    print(" ".join([str(s) for s in log_line]))
            except common.InvalidResponse:
                upload.recover(transport)
        
        # return upload object and end time
        dt = datetime.datetime.now().isoformat()
        log_line = [dt, "done", file_name, "",
                    "", upload.bytes_uploaded, upload.total_bytes, "", "", ""]
        self.log.append(log_line)
        if verbose:
            print(" ".join([str(s) for s in log_line]))
        return upload
    
    def download(self, verbose=False):
        """Download a blob from GCP Storage to a local filepath
        
        Parameters
        ----------
        verbose : bool, print operation status
        """
        self.blob = self.bucket.blob(self.blob_name)
        self.blob.download_to_filename(self.local_filepath)
        if verbose:
            print("Downloaded blob {} to {}.".format(self.blob.name, self.local_filepath))


class FolderSync:
    
    def __init__(self, client, bucket_name, 
                 local_directory, local_base_directory=None, 
                 pattern="*"):
        """Syncronize a folder on the local machine with Google Cloud Storage
        
        Parameters
        ----------
        client : GCP storage client instance
        bucket_name : str, bucket name to access
        local_directory : str, path of local directory to upload
        local_base_directory : str, path of local directory that contains local_directory,
            to use as base directory for blob naming
        blob_name : str, name of blob to create
        """


        self.client = client
        self.bucket_name = bucket_name
        self.bucket = self.client.get_bucket(self.bucket_name)
        self.local_directory = local_directory
        if self.local_directory[-1] == config.sep:
            self.local_directory = self.local_directory[:-1]
        self.local_base_directory = local_base_directory
        if self.local_base_directory is None:
            self.local_base_directory = self.local_directory
        if self.local_base_directory[-1] == config.sep:
            self.local_base_directory = self.local_base_directory[:-1]
        self.pattern = pattern

        self.local_files = None
        self.local_blobs = None
        
        self.bucket_files = None
        self.bucket_blobs = None
            
        self.log = []

    def blob_from_path(self, filepath, root_directory):
        rd = root_directory + config.sep
        fp = filepath.replace(rd, "")
        fp = fp.replace(config.sep, "_")
        return fp
        
    def inventory_local(self):
        self.local_files = tools.pathlist(self.local_directory, pattern=self.pattern)
    
    def inventory_bucket(self):
        self.bucket_files = [x.name for x in list(self.bucket.list_blobs())]
        self.bucket_blobs = [x for x in list(self.bucket.list_blobs())]
        
    def blobify(self):
        self.local_blobs = {}
        for f in self.local_files:
            self.local_blobs[f] = self.blob_from_path(f, self.local_base_directory)    

    @property
    def df_bucket_blobs(self):
        _df = pd.DataFrame([(b.name, b.size) for b in self.bucket.list_blobs()],
                           columns=["file_name", "size_bytes"])
        return _df
    
    def sync_upload(self, verbose=False):
        if self.local_files is None:
            self.inventory_local()
        if self.bucket_files is None:
            self.inventory_bucket()
        for f in self.local_files:
            if verbose:
                print("Uploading: {}".format(f))
            if os.path.isdir(f):
                if verbose:
                    print("Skipping directory: {}".format(f))
                continue
            if f not in self.bucket_files:
                file_name = os.path.basename(f)  # TODO: remove?
                blob_name = self.blob_from_path(f, self.local_base_directory)
                try:
                    bs = BlobSync(client=self.client, 
                                  bucket_name=self.bucket_name,
                                  local_filepath=f,
                                  blob_name=blob_name)
                    bs.upload_resumable(verbose=verbose)
                    for log_line in bs.log:
                        self.log.append(log_line)
                except Exception as e:
                    dt = datetime.datetime.now().isoformat()
                    if verbose:
                        print("While uploading", f)
                        response = e.response
                        print(response)
                    self.log.append([dt, "error", f, "", 
                                     response, "", "", "", e.__class__.__name__, ""])
            if verbose:
                print("-"*30)
                
    def sync_download(self):
        self.inventory_local()
        for f in self.bucket_files:
            if f not in self.local_files:
                bs = BlobSync(client=self.client, 
                              bucket_name=self.bucket,
                              local_filepath=self.local_directory + config.sep + f)
                bs.download()
    

class PooledSync:
    def __init__(self,  client, bucket_name, 
                 local_directory, 
                 local_base_directory=None,
                 pattern="*", n=None):
        """Synchronize a folder on the local machine with Google Cloud Storage using
        a pool of threads.
        
        Parameters
        ----------
        client : GCP storage client instance
        bucket_name : str, bucket name to access
        local_directory : str, path of local directory to upload
        local_base_directory : str, path of local directory that contains local_directory,
            to use as base directory for blob naming
        """
        self.client = client
        self.bucket_name = bucket_name
        self.bucket = self.client.get_bucket(self.bucket_name)
        self.local_directory = local_directory
        self.local_base_directory = local_base_directory
        self.pattern = pattern
        self.n = n
        
        self.bucket_files = None
        self.local_files = None
        self.local_blobs = None
        
        if self.n is None:
            self.n = config.cpu_count
        
        self.log = []
            
        self.control_sync = FolderSync(client=self.client,
                                       bucket_name=self.bucket_name, 
                                       local_directory=self.local_directory,
                                       local_base_directory=self.local_base_directory,
                                       pattern="*"
                                       )
        self.control_sync.inventory_local()
        self.control_sync.inventory_bucket()
        
        self.local_files = np.array(self.control_sync.local_files)
        self.local_files_chunks = np.array_split(self.local_files, self.n)
        self.n_indexes = np.arange(len(self.local_files))
        self.n_chunks = np.array_split(self.n_indexes, self.n)
        
        self.producer_lock = threading.Lock()
        
    def sync_upload_chunk(self, local_files, n, verbose=False):
        
        fs = FolderSync(client=self.client,
                        bucket_name=self.bucket_name,
                        local_directory=self.local_directory,
                        local_base_directory=self.local_base_directory
                        )
        fs.local_files = list(local_files)
        fs.blobify()
        
        if verbose:
            print("Start thread {}".format(n))
        fs.sync_upload(verbose=False)
        
        with self.producer_lock:
            for log_line in fs.log:
                self.log.append(log_line)
        
    def pipeline(self):
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=self.n) as executor:
            executor.map(self.sync_upload_chunk, 
                         self.local_files_chunks, self.n_chunks)


def bucket_blobs(bucket_name):
    """Inventory a GCS bucket and return all blob information as
    they relate to TPC benchmarks.

    Parameters
    ----------
    #gcs_client : authenticated Google Cloud Storage client instance
    bucket_name : str, name of bucket within the client service domain

    Returns
    -------
    Pandas DataFrame
    """

    gcs_client = storage.Client.from_service_account_json(config.gcp_cred_file)

    b = FolderSync(client=gcs_client,
                   bucket_name=bucket_name,
                   local_directory=config.fp_h_output,  # just placeholder
                   local_base_directory=None)

    b.inventory_bucket()
    return b.bucket_blobs


def format_blob_inventory(blobs):
    """Get basic blob info: name, url, size in bytes"""
    return [(_b.name, _b.public_url, _b.size) for _b in blobs]


def base_split(s):
    """Base split of TPC-DS and TPC-H generaged
    data files"""
    x = s.split("_")
    test = x[0]
    scale = x[1]
    more = x[2:]
    return test, scale, more


def extract_test(s):
    """Extract the test name, either 'ds' or 'h'"""
    test, _scale, _more = base_split(s)
    return test


def extract_scale(s):
    """Extract the TPC scale factor the data file
    was generated under"""
    _test, scale, _more = base_split(s)
    return scale


def extract_table(s):
    """Extract table name from TPC-DS and TPC-H file names"""
    test, scale, more = base_split(s)
    if test == "ds":
        if len(more) == 3:
            return more[0]
        elif len(more) == 4:
            return more[0] + "_" + more[1]
        else: return "ds-non-table"
    elif test == "h":
        return more[0].split(".")[0]
    else:
        return "non-table"


def extract_chunk_number(s):
    """Extract chunk number, i.e. thread number when generated"""
    test, scale, more = base_split(s)
    if test == "ds":
        return more[-2]
    elif test == "h":
        ext = more[-1].split(".")
        n = ext[-1]
        try:
            int(n)
            return n
        except ValueError:
            return "1"
    else:
        return "non-table"


def inventory_blobs_df(blobs):
    data = format_blob_inventory(blobs)
    df = pd.DataFrame(data, columns=["chunk_name", "url", "size_bytes"])
    df["uri"] = df.url.apply(lambda x: x.replace("https://storage.googleapis.com/", "gs://"))
    df["test"] = df.chunk_name.apply(extract_test)
    df["scale"] = df.chunk_name.apply(extract_scale)
    df["table"] = df.chunk_name.apply(extract_table)
    df["n"] = df.chunk_name.apply(extract_chunk_number)
    return df


def inventory_bucket_df(bucket_name):
    blobs = bucket_blobs(bucket_name)
    df = inventory_blobs_df(blobs)
    return df


def get_last_row(uri):
    fs = gcsfs.GCSFileSystem(project=config.gcp_project,
                         token=config.gcp_cred_file)
    with fs.open(uri) as f:
        df = pd.read_csv(f, sep="|", header=None, encoding="ISO-8859-1")
    return df.iloc[-1,0]


def get_dataset_rows():
    df = inventory_bucket_df(bucket_name=config.gcs_data_bucket)
    df["str_valid"] = df.test + "_" + df.scale + "_" + df.table
    df["valid"] = df.apply(lambda x: x.str_valid in x.chunk_name, axis=1)
    assert(len(df[df.valid == False]) == 0)
    df.n = df.n.astype(int)
    df["max_n"] = 0
    
    # terrible combination finding solution
    for test in df.test.unique():
        _df1 = df[df.test == test]
        for s in _df1.scale.unique():
            _df2 = _df1[_df1.scale == s]
            for table in _df2.table.unique():
                if "version" not in table:
                    mask = ((df.test == test) & 
                            (df.scale == s) & 
                            (df.table == table))
                    _df3 = df[mask]
                    df.loc[mask, "max_n"] = _df3.n.max()
                    
    x = df.loc[df.max_n == df.n].copy()  # copy is probably unneeded
    rows = x.uri.apply(get_last_row)
    df["row_count"] = ""
    df.loc[df.max_n == df.n, "row_count"] = rows
    return df