"""GCP Storage local and remote file sync
Requires a client be already instanced
"""

import io
import os
import time
import datetime

from google.auth.transport.requests import AuthorizedSession
#from google.resumable_media.requests import ResumableUpload
from google.resumable_media import requests, common

import config, tools


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
        
        """log formats:
        upload chunk: iso_time, upload_chunk, file_name, chunk_size, response.status_code
        upload done:  iso_time, 'upload_done', file_name, upload.bytes_uploaded, upload.total_bytes
        """
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
        f_size = os.path.getsize(self.local_filepath) / 1000000
        log_line = [dt, "upload_start", file_name, f_size, self.bucket_name]
        self.log.append(log_line)
        if verbose:
            print(" ".join([str(s) for s in log_line]))
        
        # send one chunk of chunk_size
        while not upload.finished:
            try:
                response = upload.transmit_next_chunk(transport)
                dt = datetime.datetime.now().isoformat()
                log_line = [dt, "upload_chunk", file_name, self.chunk_size, response.status_code]
                self.log.append(log_line)
                if verbose:
                    print(" ".join([str(s) for s in log_line]))
            except common.InvalidResponse:
                upload.recover(transport)
        
        # return upload object and end time
        dt = datetime.datetime.now().isoformat()
        log_line = [dt, "upload_done", file_name, upload.bytes_uploaded, upload.total_bytes]
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

        self.local_directory = local_directory
        self.local_files = None
        self.local_blobs = None
        self.pattern = pattern
        
        self.client = client
        self.bucket_name = bucket_name
        self.bucket = self.client.get_bucket(self.bucket_name)
        self.bucket_files = None
        
        self.log = []

    def blob_from_path(self, filepath, root_directory):
        rd = root_directory + config.sep
        fp = filepath.replace(rd, "")
        fp = fp.replace(config.sep, "_")
        return fp
        
    def inventory(self):
        self.local_files = tools.pathlist(self.local_directory, pattern=self.pattern)
        self.local_blobs = {}
        for f in self.local_files:
            self.local_blobs[f] = self.blob_from_path(f, self.local_directory)
        self.bucket_files = [x.name for x in list(self.bucket.list_blobs())]
    
    def sync_upload(self, verbose=False):
        self.inventory()
        for f in self.local_files:
            t = datetime.datetime.now().isoformat()
            t0 = time.time()
            f_basename = os.path.basename(f)
            f_size = os.path.getsize(f) / 1000000
            if verbose:
                print(t, " - UPLOAD")
                print(f)
                print("to:")
                print(self.local_blobs[f])
                print("Size: {} MB".format(f_size))
                
            self.log.append([t, "up", f_basename, f_size, 0, "started"])
            if f not in self.bucket_files:
                try:
                    bs = BlobSync(client=self.client, 
                                  bucket_name=self.bucket,
                                  local_filepath=f,
                                  blob_name=self.local_blobs[f])
                    bs.upload()
                    tn = time.time() - t0
                    self.log.append([t, "up", f_basename, f_size, tn, "done"])
                except Exception as e:
                    print("While uploading", f)
                    print("-- size:", f_size)
                    print(e.__doc__)
                    #print(e.message)
                    tn = time.time() - t0
                    self.log.append([t, "ERROR", f_basename, f_size, tn]) #, e.message])
            if verbose:
                print("-"*30)
                    
    def sync_download(self):
        self.inventory()
        for f in self.bucket_files:
            if f not in self.local_files:
                bs = BlobSync(client=self.client, 
                              bucket_name=self.bucket,
                              local_filepath=self.local_directory + config.sep + f)
                bs.download()
    