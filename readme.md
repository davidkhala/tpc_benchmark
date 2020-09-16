# TPC-DS and TPC-H Benchmark of BigQuery and Snowflake  

## Project Description  
This project uses TPC-DS and TPC-H standard benchmarks to compare the performance of BigQuery and SnowFlake.  Specifically, this project uses Conda environments, Python and Jupyter Notebooks to wrap and extend the compiled C code distributed by the TPC to do the following:  

1. Download .zip installers from a GCS bucket previously sourced from the TPC website
1. Compile the C source for the host OS and build a project folder structure
1. Wrap TPC binaries to generate data  
1. Upload data from disk to a GCS bucket
1. Create datasets/databases and initialize schema on both BigQuery and Snowflake    
1. Load data from the GCS bucket to the appropriate dataset/database  
1. Wrap TPC binaries to generate SQL on-the-fly from templates to match the database, scale factor and TPC test being run
1. Capture query text, query results and database job ids
1. Connect to support databases to get elapsed time and bytes processed per query 
1. Collate and plots per-query and summary statistics
1. Save all results to labeled and timestamped directories for further analysis

## Introduction to TPC Benchmarks  
This work uses two TPC benchmark tests, DS and H, to create a reproducible load on a data storage system. The first TPC tools were created to benchmark financial transactions in the early 1980s. Both benchmark tools and their offical specification are available free of charge in uncompiled C source format on their website:  

http://www.tpc.org/  

From this site, they describe the benchmarks:  
```
The TPC Benchmark DS (TPC-DS) is a decision support benchmark that models several generally applicable aspects of a decision support system, including queries and data maintenance. The benchmark provides a representative evaluation of performance as a general purpose decision support system. A benchmark result measures query response time in single user mode, query throughput in multi user mode and data maintenance performance for a given hardware, operating system, and data processing system configuration under a controlled, complex, multi-user decision support workload. The purpose of TPC benchmarks is to provide relevant, objective performance data to industry users. TPC-DS Version 2 enables emerging technologies, such as Big Data systems, to execute the benchmark.

The TPC Benchmark H (TPC-H) is a decision support benchmark. It consists of a suite of business oriented ad-hoc queries and concurrent data modifications. The queries and the data populating the database have been chosen to have broad industry-wide relevance. This benchmark illustrates decision support systems that examine large volumes of data, execute queries with a high degree of complexity, and give answers to critical business questions. 
```

### Specification  

In addition to the source code required to run the benchmarks, each .zip contains a PDF of the benchmark's specifications called `specification.pdf`. Refer to this document for detailed descriptions of all parts of the TPC supplied tools and the methods required to execute a TPC benchmark.  

### Goals Adherant to TPC Specifications  
The goal of this work is to follow as closely as possible the spirit and specification set out by the TPC. They describe this goal:  
```
The purpose of TPC benchmarks is to provide relevant, objective performance data to industry users. To achieve that purpose, TPC benchmark specifications require that benchmark tests be implemented with systems, products, technologies and pricing that:
• Are generally available to users;
• Are relevant to the market segment that the individual TPC benchmark models or represents (e.g., TPC-H models and represents complex, high data volume, decision support environments);
• Would plausibly be implemented by a significant number of users in the market segment the benchmark models or represents.

The use of new systems, products, technologies (hardware or software) and pricing is encouraged so long as they meet the requirements above. Specifically prohibited are benchmark systems, products, technologies or pricing (hereafter referred to as "implementations") whose primary purpose is performance optimization of TPC benchmark results without any corresponding applicability to real-world applications and environments. In other words, all "benchmark special" implementations that improve benchmark results but not real-world performance or pricing, are prohibited.
```  

## Project Structure  
The project has and generates the following structure:  

```
/data      - local directory or symbolic link, location generated data is saved
/download  - location TPC .zip files are downloaded to
/ds        - TPC-DS uncompressed source folder
/h         - TPC-H uncompressed source folder
/results   - results directories are written during benchmarking
/sc        - schema files used in creating or cloning databases
/tpl       - query template files for target systems and test
```

The Python and Jupyter source is commented with descriptions, other source files include:  
```
environment_tpc.yml  - Conda environment file to use for project
ds_stream_seq.csv    - TPC-DS query sequence for each stream test
h_stream_seq.csv     - TPC-H query sequence for each stream test
```

### Use of Query Templates  
A key feature of the TPC benchmarks is the use of query templates. These template files, with extension `.tpl` are supplied in the TPC source and used by the TPC query generator to create SQL to be executed on the target system under test. In this project, these templates were copied to /tpl and then manually modified to be run by the TPC query generator programs. The output of from the query generator will then be SQL syntax which will run on BigQuery and Snowflake. The four template folders are:
* `/tpl/bq_ds`
* `/tpl/bq_h`
* `/tpl/sf_ds`
* `/tpl/sf_h`  

A query edit log was maintained of all modifications done to the query templates; as well the project repository contains the templates in unedited format for quality control in folders within `/tpl` each with a `_gen` suffix in the folder name.  

## Required Resources  
The following accounts are required:  
* A Google Cloud Platform service account with BigQuery Admin and Storage Admin Roles
* A Google Cloud Platform account with ability to create and launch AI Notebooks  
* A Snowflake account with SYSADMIN level permissions

Additionally, these are needed locally in the project:  
GCP Service account key JSON file
Snowflake user name and password 

## Installing and Running Benchmarks   
At all times, refer to the benchmark's `specification.pdf` for details on the benchmark. The Jupyter Notebooks contain instructions inline with executable code.  
### Configuration File  
This project uses a single configuration file, `config.py` to set project specific variables. In the following instructions, the relevent Python variable will include the module in the notation, i.e. `config.value`

### Download TPC-H and TPC-DS  
The TPC data has to be downloaded to be used and a license agreement must be filled out manually.  
http://www.tpc.org/tpc_documents_current_versions/current_specifications5.asp  
After submitting, emails are sent with download links. Upload the zip files to a GCS bucket and add the 

For this project, both source programs were downloaded to GCS 2020-02-24.  

### Setup Google Cloud Platform AI Notebook  
1. Create an AI Notebook with 96 cores, 100GB of disk space. The VM can be scaled down to 4 cores after all data generation is complete.
2. Add a 20TB persistent disk
3. Start instance and open Jupyter Lab
4. Open new terminal from the plus menu (+)
5. Install gcc and gsutil if not already installed (usually is on AI Notebooks)
6. Activate the base Conda environment and install the Google BigQuery reservation API Python client:  
   `conda activate base`
   `pip install google-cloud-bigquery-reservation`
6. git clone the repo https://github.com/sadasystems/tpc_benchmark  
   Repository should be public, so no credentials are required.  
7. cd to the repo and update the base conda environment with
    ```
    conda env update --file environment_tpc.yml --name base
    ```
8. add the repo to the conda path so notebooks will find the package in the python path:  `conda develop /home/jupyter/code/tpc_benchmark`
9. add the persistent disk to a mount point
    https://devopscube.com/mount-extra-disks-on-google-cloud/
    mount the persistent disk to the same value as `config.fp_output_mnt`, this will likely be "/mnt/disks/data"
10. Since Jupyter loads in the `jupyter` user directory, create a symbolic link to the /data mount point with:  
`ln -s /data /home/jupyter/code/tpc_benchmark/data`  
(a new directory `data` should appear in the project folderin in Jupyter Lab)

### Setup GCP Service Account & Snowflake User
1. Create service account key for local development:  
    https://cloud.google.com/iam/docs/creating-managing-service-account-keys#iam-service-account-keys-create-python
2. Using Jupyter upload a JSON key into the user directory (i.e. `/jupyter`)
3. Using Jupyter, create a file called `poor_security.py` in the root repo directory. Add two variables to this file: `sf_username` = your Snowflake user name and `sf_password` = your Snowflake account password. Make sure this account has `SYSADMIN` level permissions.   

### Running Notebooks in Sequence  
To run from start to end an entire TPC power test (query stream sequence 0), the Jupyter Notebooks need to be executed in order.  This allows for an entire pipeline to be run, or a specific step to be repeated or explored further.  The naming conventions common to all notebooks need to be coordinated. A dataset generated with specific attributes will create specific blobs in GCS and commonly named dataset in BigQuery and Snowflake.  Specifically:  

`test` : str, test being done, either 'h' or 'ds'  
`scale` : int, test scale factor in GigaBytes, i.e. 100 for 100GB  
`cid` : str, config id, i.e. '02A' for the experiment config number  

### Notebook Step 01 - Compile TPC  
Run either `NB_01_DS_setup.ipynb` or `NB_01_H_setup.ipynb`. Both will download the 
.zip files locally, alter path variables and install the TPC binaries in a known location for use in the rest of the project.

### Notebook Step 02 - Data Generation  
Run either `NB_02_DS_datagen.ipynb` or `NB_02_H_datagen.ipynb`. Both will generate the CSV data to be uploaded and save a log of the data generation time elapsed for each threaded generator.

### Notebook Step 03 - Upload Data to GCS  
Run `NB_03_H-DS_GCS_upload.ipynb` and change the test and scale factor to match the data to be uploaded. Data from the appropriate `/data` folder will be renamed to a consistent format and uploaded to GCS.

### Notebook Step 04 - Schema Generation  
Run either `NB_04_DS_schema_gen.ipynb` or `NB_04_H_schema_gen.ipynb` to copy the schema files shipped with the TPC source. Then the notebook will load an edited schema file included in the git repo of either BigQuery or Snowflake syntax and initialize a dataset/database with it. 

### Notebook Step 05 - Data Import  
Run either `NB_05_H-DS_BQ_import.ipynb` or `NB_05_H-DS_SF_import.ipynb` and edit the test, scale and configuration id (cid) to match a desired test sequence. The process will report a cell magic time, save a log of the upload and print a summary of the upload process.  

### Notebook Step 06 - Benchmark  
Open `NB_06_benchmark.ipynb` and change the test configuration as needed. If set, query text, status, time will print as the test proceeds. Once both systems have completed the query stream, their results will be compared and the data saved to a new folder with metadata and timestamps.

### Notebook Step 07 - Summary  
If `NB_06_benchmark.ipynb` was run directly beforehand, just open `NB_07_summary.ipynb` and run it (it will load data via a Jupyter shared variable), if not uncomment `results_dir` and set the data folder to summarize. When run, this notebook will read the saved query ids for both systems, log into both and query their information schema or account history tables and download additional information on each query. This data is then merged with the original benchmark data.  The database system time spent (not OS that ran the notebook) and bytes processed are plotted, summarized and saved to the results directory.  

### Additional Notebooks  
Several additional notebooks are avaliable, labeled `A, B, C, D, E` for debugging and quality control tests.  

## Quality Control  
For both TPC tests, query templates were copied and edited to create templates that when run with the query generation binaries produce executable SQL for BigQuery and Snowflake.  Since all operations in this benchmarking effort used Python and each database's official Python client, the outputs of each query were accessed using the respective client's Pandas Dataframe conversion method.  Comparison was then done using Pandas methods.  

Results were collected as follows:
1. Query SQL was generated for the appropriate database and scale factor and executed via the client
2. Pandas Dataframes are output by each database client module
3. Dataframes are saved to CSV files in the query sequence results directory
4. After the query sequence is done, the CSV files are loaded into Pandas Dataframes
5. The `tools.consistent` method is used on the loaded Dataframes which does the following:
    a. Converts table column names to lowercase
    b. Converts any columns with numeric like data using pandas.to_numeric method
    c. Fills None/NaN values to numeric `-9999.99` to avoid evaluations failing due to None == None evaluating to False
6. Evaluate Dataframe differences using pandas.testing.assert_fame_equal using:

    a. `check_names=False` : Whether to check that the names attribute for both the index and column attributes of the DataFrame is identical.  Snowflake and BigQuery return upper and lowercase different column labels which are not important for query comparisons.
    b. `check_exact=False` : Whether to compare number exactly.  Explicitly set to False to ensure use of `check_less_precise` is readable.
    c. `check_less_precise=2` : Specify comparison precision. Only used when check_exact is False. 5 digits (False) or 3 digits (True) after decimal points are compared. If int, then specify the digits to compare.

    When comparing two numbers, if the first number has magnitude less than 1e-5, we compare the two numbers directly and check whether they are equivalent within the specified precision. Otherwise, we compare the ratio of the second number to the first number and check whether it is equivalent to 1 within the specified precision.

For this project, BigQuery defaults to returning 2 or 3 decimal places in Dataframes after conversion to numeric types.  Snowflake's client returns object columns with Decimal class contents which when converted to float dtype columns results in different numbers of decimal values.  In cases where the dissimilar additional decimal place is a 5, an evaluation based on decimal format or rounding produces values off by the last decimal place.  By setting `config.float_precision` to 2, all values are only compared to 2 decimal places regardless of additional decimal places available in the value.  

## Data Output  
Each benchmark run initiated by `NB_06_benchmark.ipynb` creates a folder in the project `/results` directory. If the optional exclusion of queries from calcuations is done, additional plots will be generated with the exclusions reflected.  Files and folders are generated using the `tools.make_name` function.

### Folder Name  
Each folder in `/results` is generated with the following `_` delimited fields:  
```
results : str, constant indicating benchmark results
db : str, data base name, either 'bq' or 'sf'; or 'bqsf' for both
kind : str, kind of record, either 'results' or 'times'
datasource : str, dataset if bq or database if snowflake, will be a combination of 
    test : str, test being done, either 'h' or 'ds'
    scale : str, scale factor with 'GB' suffix, i.e. '100GB'
    cid : str, config id, i.e. '02A' for the experiment config number
desc : str, description of experiment
timestamp : timestamp of when test was run
```

### Data files  
Inside the results folder, the following files are generated automatically:  
1. `query_result*.csv` - result data from a specific query
1. `query_text*.txt` - query text that was executed
1. `plot_qc*` - plot showing agreement in query results by query number
1. `metadata_initial.json` and `metadata_final.json` - test specific data for further processing and context
1. `benchmark_times*.csv` - timing and query ids recorded locally
1. `query_history*.csv` - information schema or account history data that includes the time period the benchmark was conducted with query ids for joining
1. `benchmark_results*.csv` - compiled times and bytes processed from `query_history` and API data
1. `plot_query_comparison*` - visual of `benchmark_results`
1. `benchmark_total*.csv` - data summed by system
1. `plot_totals*` - visual of `benchmark_total`

### Data Formatting  
Besides the plots, all files are UTF-8 encoded text files.  Their data formats are:  
#### query_result*.csv
These results are specific to the query number run and should match between systems under test.
#### query_text*.txt
The query text that generated from a `.tpl` file and by the test query generator and executed by the system under test.

#### plot_* 
Plot are all .png format, visual layout varies

#### metadata.json  
Class atribute dumps of `compare.QueryCompare` before any queries were evaluated (initial) and after (final)  

#### benchmark_times  
Initial data collect on the computer running Python.  
```
db         - str, data base name, either 'bq' or 'sf'
test       - str, test being done, either 'h' or 'ds'
scale      - str, scale factor with 'GB' suffix, i.e. '100GB'
source     - str, dataset if bq or database if snowflake
cid        - str, config id, i.e. '02A' for the experiment config number
desc       - str, description of experiment
query_n    - int, test query number 
seq_n      - int, test query stream sequence number
driver_t0  - str timestamp, host system time query api was executed
driver_t1  - str timestamp, host system time query api completed
qid        - str, database system under test unique query identification value
```

### query_history_bq.csv  
BigQuery job metadata from INFORMATION_SCHEMA.JOBS_BY_* view spanning the time of the benchmark.  See:  
https://cloud.google.com/bigquery/docs/information-schema-jobs  

### query_history_sf.csv  
Snowflake job metadata from `QUERY_HISTORY` view spanning the time of the benchmark.  See:  
https://docs.snowflake.com/en/sql-reference/account-usage/query_history.html  

### benchmark_results  
Same data format as `benchmark_times` for `db, test, scale, source, cid, desc, query_n, seq_n` with the following additions:  

```
dt    - str, time delta
dt_s  - float, time in seconds
TB    - float, amount of data processed in Terabytes
```
where all values are merged from the `query_history` data and represent the per-query system reported time and bytes processed (not the time observed by the computer running the Python and Jupyter Notebooks).  This file should be used as the 'final' data for a benchmark run.  

### benchmark_total  
Same data format as `benchmark_results`, with values summed by `db` and `test`. These values represent the 'total time' and 'total bytes' processed for the query stream sequence run (and for the 1st test sequence in the specification, the Power Test).
