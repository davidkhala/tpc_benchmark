"""Generic BigQuery Operations

Colin Dietrich SADA 2020
"""

import re
from google.cloud import bigquery

import config, tools


def create_dataset(verbose=False):
    """Create a dataset on the project
    
    See:
    https://cloud.google.com/bigquery/docs/reference/standard-sql/data-definition-language#create_table_statement  
    https://googleapis.dev/python/bigquery/latest/generated/google.cloud.bigquery.client.Client.html#google.cloud.bigquery.client.Client.create_dataset  
    https://googleapis.dev/python/bigquery/latest/generated/google.cloud.bigquery.job.CopyJob.html#google.cloud.bigquery.job.CopyJob
    for details
    
    Parameters
    ----------
    verbose : bool, print debug statements
    
    Returns
    -------
    A new copy job instance
    """
    
    dataset_name = config.gcp_project + "." + config.gcp_dataset
    dataset = bigquery.Dataset(dataset_name)
    dataset.location = config.gcp_location
    
    client = bigquery.Client.from_service_account_json(config.gcp_cred_file)
    copy_job = client.create_dataset(dataset)
    if verbose:
        print("Created dataset {}.{}".format(client.project, dataset.dataset_id))    
    return copy_job

def create_schema(verbose=False):
    """Apply the schema .sql file as reformatted from 
    config.tpcds_schema_ansi_sql_filepath
    to 
    config.tpcds_schema_bq_filepath
    using schema() method in this module.
    """
    client = bigquery.Client.from_service_account_json(config.gcp_cred_file)
    with open(config.tpcds_schema_bq_filepath, 'r') as f:
        query_txt = f.read()
        
    query_job = client.query(query_txt)  # API request
    rows = query_job.result()  # Waits for query to finish
    
    if verbose:
        for r in rows:
            print(r.name)