"""GCP Storage local and remote file sync
Requires a client be already instanced
"""

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
        self.blob.upload_from_filename(self.local_filepath)
        
        self.uri = self.blob.public_url.replace("https://storage.googleapis.com/", "gs://")
        
        if verbose:
            print("File uploaded to {}.".format(self.bucket.name))
            
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
            
class BucketSync:
    
    def __init__(self, client, bucket_name, local_directory):
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
        
        self.client = client
        self.bucket_name = bucket_name
        self.bucket = self.client.get_bucket(self.bucket_name)
        self.bucket_files = None
    
    def inventory(self):
        self.local_files = tools.pathlist(self.local_directory)
        self.local_files = [f.split(config.sep)[-1] for f in self.local_files]
        self.bucket_files = [x.name for x in list(self.bucket.list_blobs())]
    
    def sync_upload(self):
        self.inventory()
        for f in self.local_files:
            if f not in self.bucket_files:
                bs = BlobSync(client=self.client, 
                              bucket_name=self.bucket,
                              local_filepath=self.local_directory + config.sep + f)
                bs.upload()
        
    def sync_download(self):
        self.inventory()
        for f in self.bucket_files:
            if f not in self.local_files:
                bs = BlobSync(client=self.client, 
                              bucket_name=self.bucket,
                              local_filepath=self.local_directory + config.sep + f)
                bs.download()
    