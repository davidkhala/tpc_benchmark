"""Prepare TPC-H data for BigQuery

Colin Dietrich, SADA, 2020

## Implementation  
Based on the definitions in ./dbgen/dss.ddl, the following datatype definitions mapping was used.  Note `time` and `date` are converted to UPPER for code formatting consistency, no performance difference is intended.

| TPC-DS ANSI SQL | BigQuery SQL |
| --------------- | ------------ |
| decimal         | FLOAT64      |  
| integer         | INT64        |  
| char(N)         | STRING       |  
| varchar(N)      | STRING       |
| time            | TIME         |  
| date            | DATE         |  

See
https://cloud.google.com/bigquery/docs/reference/standard-sql/data-types
for BigQuery datatype specifications in standard SQL

"""

dtype_mapper = {r'  decimal\(\d+,\d+\)  ': r'  FLOAT64  ',
                r'  varchar\(\d+\)  ':     r'  STRING  ',
                r'  char\(\d+\)  ':        r'  STRING  ',
                r'  integer  ':            r'  INT64  ',
                # the following are just to have consistent UPPERCASE formatting
                r'  time  ':               r'  TIME  ',
                r'  date  ':               r'  DATE  '
               }