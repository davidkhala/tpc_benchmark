"""Prepare TPC-DS tests for Snowflake

...

| TPC-DS ANSI SQL | Snowflake    |
| --------------- | ------------ |
| decimal         | DECIMAL      |
| integer         | INTEGER      |
| char(N)         | CHAR(N)      |
| varchar(N)      | VARCHAR(N)   |
| time            | TIME         |
| date            | DATE         |

See
https://docs.snowflake.com/en/sql-reference/intro-summary-data-types.html
for Snowflake datatype specifications in standard SQL

"""

import re
from google.cloud import bigquery

import config, tools


def start_warehouse(warehouse_id):
    pass

def suspend_warehouse(warehouse_id):
    pass

def create_integration(table_name, gcs_location):
    # tells snowflake about CSV file structure
    named_file_name = _create_named_file_format(table_name)

    # link to GCS URI (and creates a service account which needs storage permissions in GCS IAM)
    storage_integration = _create_storage_integration(table_name, gcs_location)

    # grant snowflake user permissions to access "storage integration" in order to great a STAGE
    errors = _grant_storage_integration_access(storage_integration)

    # create STAGE: which knows what GCS URI to pull from, what file in bucket, how to read CSV file
    stage = _create_stage(table_name, storage_integration, named_file_format)

    # test storage
    storage_items = _list_stage(stage)

    return stage

def _create_named_file_format(table_name):
    pass

def _create_storage_integration(table_name, gcs_location):
    pass

def _grant_storage_integration_access(storage_integration):
    pass

def _create_storage(table_name, storage_integration, named_file_format):
    pass

def _list_stage(stage_name):
    pass

def import_data(integration_id, table_name):
    pass

def create_schema(schema_file, dataset, verbose=False):
    pass