"""GCP Storage local and remote file sync
Requires a client be already instanced
"""

import io
import os
import time
import datetime
import threading
import concurrent.futures
import numpy as np

from google.auth.transport.requests import AuthorizedSession
#from google.resumable_media.requests import ResumableUpload
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
    import pandas as pd
    _df = pd.DataFrame(data_list, columns=log_format)
    return _df


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
    
    def __init__(self, client, bucket_name, local_directory, pattern="*"):
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
        self.local_directory = local_directory
        if self.local_directory[-1] == config.sep:
            self.local_directory = self.local_directory[:-1]
        self.pattern = pattern

        self.bucket_files = None
        self.local_files = None
        self.local_blobs = None
                
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
            
    def blobify(self):
        self.local_blobs = {}
        for f in self.local_files:
            self.local_blobs[f] = self.blob_from_path(f, self.local_directory)    

    @property
    def df_bucket_blobs(self):
        import pandas as pd
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
                file_name = os.path.basename(f)
                blob_name = self.blob_from_path(f, self.local_directory)
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
    

class PooledSync():
    def __init__(self,  client, bucket_name, local_directory, pattern="*", n=None):
        
        self.client = client
        self.bucket_name = bucket_name
        self.bucket = self.client.get_bucket(self.bucket_name)
        self.local_directory = local_directory
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
                        local_directory=self.local_directory)
        fs.local_files = list(local_files)
        fs.blobify()
        #print("A >>", fs.local_directory)
        #print("B >>", fs.blob_from_path(fs.local_files[0], fs.local_directory))
        #print("1 >> ", fs.local_files)
        #print("2 >>", fs.local_blobs)
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
