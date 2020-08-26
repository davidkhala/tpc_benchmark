# Output Data Sources & Calculations  

## I. Snowflake Query History  

Two databases on Snowflake can be accessed to get information about already run queries. Only `snowflake.account_usage` is used in this project.

### 1. query_history_view - snowflake.account_usage
	
Get the time bound query history for all queries run on this account using the
`snowflake.account_usage` database

Note: Activity in the last 1 year is available, latency is 45 minutes.

https://docs.snowflake.com/en/sql-reference/account-usage.html  
https://docs.snowflake.com/en/sql-reference/account-usage/query_history.html

EXAMPLE QUERY  
with t0, t1 = str timestamps in ISO 8601:

```
query_text = ("select * " +
              "from query_history " +
              f"where start_time>=to_timestamp_ltz('{t0}')"
              )
```

#### Returns  

| Column Name                              | Data Type      | Description |
| ---------------------------------------- | -------------- | ----------- |
| READER_ACCOUNT_NAME                      | TEXT           | Name of the reader account in which the the SQL statement was executed. |
| QUERY_ID                                 | TEXT           | Internal/system-generated identifier for the SQL statement. |
| QUERY_TEXT                               | TEXT           | Text of the SQL statement. |
| DATABASE_ID                              | NUMBER         | Internal/system-generated identifier for the database that was in use. |
| DATABASE_NAME                            | TEXT           | Database that was in use at the time of the query. |
| SCHEMA_ID                                | NUMBER         | Internal/system-generated identifier for the schema that was in use. |
| SCHEMA_NAME                              | TEXT           | Schema that was in use at the time of the query. |
| QUERY_TYPE                               | TEXT           | DML, query, etc. If the query is currently running, or the query failed, then the query type may be UNKNOWN. |
| SESSION_ID                               | NUMBER         | Session that executed the statement. |
| USER_NAME                                | TEXT           | User who issued the query. |
| ROLE_NAME                                | TEXT           | Role that was active in the session at the time of the query. |
| WAREHOUSE_ID                             | NUMBER         | Internal/system-generated identifier for the warehouse that was used. |
| WAREHOUSE_NAME                           | TEXT           | Warehouse that the query executed on, if any. |
| WAREHOUSE_SIZE                           | TEXT           | Size of the warehouse when this statement executed. |
| WAREHOUSE_TYPE                           | TEXT           | Type of the warehouse when this statement executed. |
| CLUSTER_NUMBER                           | NUMBER         | The cluster (in a multi-cluster warehouse) that this statement executed on. |
| QUERY_TAG                                | TEXT           | Query tag set for this statement through the QUERY_TAG session parameter. |
| EXECUTION_STATUS                         | TEXT           | Execution status for the query: success, fail, incident. |
| ERROR_CODE                               | NUMBER         | Error code, if the query returned an error |
| ERROR_MESSAGE                            | TEXT           | Error message, if the query returned an error |
| START_TIME                               | TIMESTAMP_LTZ  | Statement start time (in the UTC time zone) |
| END_TIME                                 | TIMESTAMP_LTZ  | Statement end time (in the UTC time zone), or NULL if the statement is still running. |
| TOTAL_ELAPSED_TIME                       | NUMBER         | Elapsed time (in milliseconds). |
| BYTES_SCANNED                            | NUMBER         | Number of bytes scanned by this statement. |
| PERCENTAGE_SCANNED_FROM_CACHE            | FLOAT          | The percentage of data scanned from the local disk cache. |
| BYTES_WRITTEN                            | NUMBER         | Number of bytes written (e.g. when loading into a table). |
| BYTES_WRITTEN_TO_RESULT                  | NUMBER         | Number of bytes written to a result object. |
| BYTES_READ_FROM_RESULT                   | NUMBER         | Number of bytes read from a result object. |
| ROWS_PRODUCED                            | NUMBER         | Number of rows produced by this statement. |
| ROWS_INSERTED 1                          | NUMBER         | Number of rows inserted by the query. |
| ROWS_UPDATED 1                           | NUMBER         | Number of rows updated by the query. |
| ROWS_DELETED 1                           | NUMBER         | Number of rows deleted by the query. |
| ROWS_UNLOADED 1                          | NUMBER         | Number of rows unloaded during data export. |
| BYTES_DELETED 1                          | NUMBER         | Number of bytes deleted by the query. |
| PARTITIONS_SCANNED                       | NUMBER         | Number of micro-partitions scanned. |
| PARTITIONS_TOTAL                         | NUMBER         | Total micro-partitions of all tables included in this query. |
| BYTES_SPILLED_TO_LOCAL_STORAGE           | NUMBER         | Volume of data spilled to local disk. |
| BYTES_SPILLED_TO_REMOTE_STORAGE          | NUMBER         | Volume of data spilled to remote disk. |
| BYTES_SENT_OVER_THE_NETWORK              | NUMBER         | Volume of data sent over the network. |
| COMPILATION_TIME                         | NUMBER         | Compilation time (in milliseconds) |
| EXECUTION_TIME                           | NUMBER         | Execution time (in milliseconds) |
| QUEUED_PROVISIONING_TIME                 | NUMBER         | Time (in milliseconds) spent in the warehouse queue, waiting for the warehouse servers to provision, due to warehouse creation, resume, or resize. |
| QUEUED_REPAIR_TIME                       | NUMBER         | Time (in milliseconds) spent in the warehouse queue, waiting for servers in the warehouse to be repaired. |
| QUEUED_OVERLOAD_TIME                     | NUMBER         | Time (in milliseconds) spent in the warehouse queue, due to the warehouse being overloaded by the current query workload. |
| TRANSACTION_BLOCKED_TIME                 | NUMBER         | Time (in milliseconds) spent blocked by a concurrent DML. |
| OUTBOUND_DATA_TRANSFER_CLOUD             | TEXT           | Target cloud provider for statements that unload data to another region and/or cloud. |
| OUTBOUND_DATA_TRANSFER_REGION            | TEXT           | Target region for statements that unload data to another region and/or cloud. |
| OUTBOUND_DATA_TRANSFER_BYTES             | NUMBER         | Number of bytes transferred in statements that unload data to another region and/or cloud. |
| INBOUND_DATA_TRANSFER_CLOUD              | TEXT           | Source cloud provider for statements that load data from another region and/or cloud. |
| INBOUND_DATA_TRANSFER_REGION             | TEXT           | Source region for statements that load data from another region and/or cloud. |
| INBOUND_DATA_TRANSFER_BYTES              | NUMBER         | Number of bytes transferred in statements that load data from another region and/or cloud. |
| LIST_EXTERNAL_FILES_TIME                 | NUMBER         | Time (in milliseconds) spent listing external files. |
| CREDITS_USED_CLOUD_SERVICES              | NUMBER         | Number of credits used for cloud services in the hour. |
| RELEASE_VERSION                          | NUMBER         | Release version in the format of major_release.minor_release.patch_release. |
| EXTERNAL_FUNCTION_TOTAL_INVOCATIONS      | NUMBER         | The aggregate number of times that this query called remote services. For important details, see the Usage Notes. |
| EXTERNAL_FUNCTION_TOTAL_SENT_ROWS        | NUMBER         | The total number of rows that this query sent in all calls to all remote services. |
| EXTERNAL_FUNCTION_TOTAL_RECEIVED_ROWS    | NUMBER         | The total number of rows that this query received from all calls to all remote services. |
| EXTERNAL_FUNCTION_TOTAL_SENT_BYTES       | NUMBER         | The total number of bytes that this query sent in all calls to all remote services. |
| EXTERNAL_FUNCTION_TOTAL_RECEIVED_BYTES   | NUMBER         | The total number of bytes that this query received from all calls to all remote services. |
| QUERY_LOAD_PERCENT                       | NUMBER         | The percentage of load this query put on the warehouse. |

#### Used in Reporting  

| Column Name                              | Data Type      | Description |
| ---------------------------------------- | -------------- | ----------- |
| QUERY_ID                                 | TEXT           | Internal/system-generated identifier for the SQL statement. |
| QUERY_TEXT                               | TEXT           | Text of the SQL statement. |
| DATABASE_NAME                            | TEXT           | Database that was in use at the time of the query. |
| WAREHOUSE_NAME                           | TEXT           | Warehouse that the query executed on, if any. |
| WAREHOUSE_SIZE                           | TEXT           | Size of the warehouse when this statement executed. |
| WAREHOUSE_TYPE                           | TEXT           | Type of the warehouse when this statement executed. |
| QUERY_TAG                                | TEXT           | Query tag set for this statement through the QUERY_TAG session parameter. |
| START_TIME                               | TIMESTAMP_LTZ  | Statement start time (in the UTC time zone) |
| END_TIME                                 | TIMESTAMP_LTZ  | Statement end time (in the UTC time zone), or NULL if the statement is still running. |
| TOTAL_ELAPSED_TIME                       | NUMBER         | Elapsed time (in milliseconds). |
| BYTES_SCANNED                            | NUMBER         | Number of bytes scanned by this statement. |
| PERCENTAGE_SCANNED_FROM_CACHE            | FLOAT          | The percentage of data scanned from the local disk cache. |
| CREDITS_USED_CLOUD_SERVICES              | NUMBER         | Number of credits used for cloud services in the hour. |


## II. BigQuery INFORMATION SCHEMA

ONLY AVAILABLE for the last 180 days

https://cloud.google.com/bigquery/docs/information-schema-jobs
https://cloud.google.com/bigquery/pricing#flat_rate_pricing

INFORMATION_SCHEMA.JOBS_BY_PROJECT returns all jobs submitted in the current project.

EXAMPLE QUERY  
with t0, t1 = str timestamps in ISO 8601:  

```
query_text = ("select * from `region-us`.INFORMATION_SCHEMA.JOBS_BY_PROJECT " +
              "where job_type = 'QUERY' " +
              f"and end_time between '{t0}' AND '{t1}'")
```

### Returns  

| Column name            | Data type   | Value |
| ---------------------- | ----------- | ----- |
| creation_time          | TIMESTAMP   | (Partitioning column) Creation time of this job. Partitioning is based on the UTC time of this timestamp. |
| project_id             | STRING      | (Clustering column) ID of the project. |
| project_number         | INTEGER     | Number of the project. |
| folder_numbers         | RECORD      | Google Accounts and ID Administration (GAIA) IDs of folders in a project's ancestry, in order starting with the leaf folder closest to the project. This column is only populated in JOBS_BY_FOLDER. |
| user_email             | STRING      | (Clustering column) Email address or service account of the user who ran the job. |
| job_id                 | STRING      | ID of the job. For example, `bquxjob_1234`. |
| job_type               | STRING      | The type of the job. Can be QUERY, LOAD, EXTRACT, COPY, or null. Job type null indicates an internal job, such as script job statement evaluation or materialized view refresh. |
| statement_type         | STRING      | The type of query statement, if valid. For example, SELECT, INSERT, UPDATE, or DELETE. |
| start_time             | TIMESTAMP   | Start time of this job. |
| end_time               | TIMESTAMP   | End time of this job. |
| query                  | STRING      | SQL query text. Note: The JOBS_BY_ORGANIZATION view does not have the query column. |
| state                  | STRING      | Running state of the job. Valid states include PENDING, RUNNING, and DONE. |
| reservation_id         | STRING      | Name of the primary reservation assigned to this job, if applicable. |
| total_bytes_processed  | INTEGER     | Total bytes processed by the job. |
| total_slot_ms          | INTEGER     | Slot-milliseconds for the job over its entire duration. |
| error_result           | RECORD      | Details of error (if any) as an ErrorProto. |
| cache_hit              | BOOLEAN     | Whether the query results were cached. |
| destination_table      | RECORD      | Destination table for results (if any). |
| referenced_tables      | RECORD      | Array of tables referenced by the job. |
| labels                 | RECORD      | Array of labels applied to the job as key, value strings. |
| timeline               | RECORD      | Query timeline of the job. Contains snapshots of query execution. |
| job_stages             | RECORD      | Query stages of the job. |

### Used in Reporting  

| Column name            | Data type   | Value |
| ---------------------- | ----------- | ----- |
| statement_type         | STRING      | The type of query statement, if valid. For example, SELECT, INSERT, UPDATE, or DELETE. |
| start_time             | TIMESTAMP   | Start time of this job. |
| end_time               | TIMESTAMP   | End time of this job. |
| total_bytes_processed  | INTEGER     | Total bytes processed by the job. |
| total_slot_ms          | INTEGER     | Slot-milliseconds for the job over its entire duration. |
| cache_hit              | BOOLEAN     | Whether the query results were cached. |
| labels                 | RECORD      | Array of labels applied to the job as key, value strings. |


## III. Calculations  

See `analysis.py` for implementation of the following calculations.

### 1. Total elapsed time in seconds  

#### Inputs

##### *Snowflake*  
`START_TIME` : TIMESTAMP_LTZ, Statement start time (in the UTC time zone)
`END_TIME`   : TIMESTAMP_LTZ, Statement end time (in the UTC time zone), or NULL if the statement is still running.

##### *BigQuery*  
`start_time` : TIMESTAMP, Start time of this job.
`end_time`   : TIMESTAMP, End time of this job.

#### Calculations  

Variables are renamed as `START_TIME` to `start_time` and `END_TIME` to `end_time` and converted to a datatime64[ns] object.  
Output column is `dt` : integer, total seconds calculated as `end_time - start_time`  

### 2. TeraBytes Processed  

#### Inputs  
  
##### *Snowflake*  
`BYTES_SCANNED` : NUMBER, Number of bytes scanned by this statement  
  
##### *BigQuery*  
`total_bytes_processed` : INTEGER, Total bytes processed by the job.  

#### Calculations  

Variables are copied to `bytes` column.  
Output column is `TB` : float, total TeraBytes calculated as `bytes / 1e12`

### 3. Cost in Dollars (US)  

#### Inputs  

##### *Snowflake*
`CREDITS_USED_CLOUD_SERVICES` : NUMBER, number of credits used for cloud services in the hour.

##### *BigQuery*  
`dt` : float, calculated total seconds elapsed (see calculation above)

#### Calculations  

##### *Snowflake*  
With an Enterprise account it is $3.00 per Snowflake credit, therefore:  
`cost = CREDITS_USED_CLOUD_SERVICES * 3.00`  

##### *BigQuery*  
Using the U.S. Flex-slot short-term commitment rate = $4.00 per (100 slots)/(1 hour)  
Based on: https://cloud.google.com/bigquery/pricing#flat_rate_pricing  

Inputs  
`slots` : number of slots reserved  
`dt` : seconds of billed time in seconds

Calculated cost of billed time and slot reservation  
`cost = 4.00 * math.ceil(slots / 100) * (math.ceil(dt) / (60 * 60))`

Using the U.S. region on-demand pricing = $5.00 / TB  
Based on: https://cloud.google.com/bigquery/pricing#on_demand_pricing  

`TB` : TeraBytes of data processed (see calculation above)

cost of billed bytes:
`cost = TB * 5.00`
