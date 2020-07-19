# TPC Benchmark Comparison of BigQuery and Snowflake  

## Project Description  

SOW: Perform an analysis of the strengths and weakness of BigQuery when using TPC-DS and TPC-H standards, when compared to SnowFlake.

## Terminology  

TPC uses specific terminology for the work, for a complete list see 
x.xx in TCP-DS specifications.pdf  
y.yy in TPC-H specification.pdf  

For clarity of this document, the following are defined:  
SUT : System Under Test - i.e. the database system being tested

## Scope of Work  

The following items were specified in the SOW:  
* The setup experience  
* Ops Tasks
    * TODO: more clarity on what this means
* Data Loading  
* TPC-DS and TPC-H testing at 100G, 1T and 10T sizes  
    * Revision of the existing packaged  
    * TODO: What does this sentence mean?  
* Detailed TCO analysis, per the “Understanding Cloud Pricing” blog-post style  
    * TODO: Define TCO (is it Total Cost of Ownership?)
    * TODO: What is the reference blog for this style?
* Graphic representations of relative measures  

## Development Tasks  

1. Scope TPC benchmark methods - DONE

2. Localize TPC tests
    1. Reproducible extraction & make - DONE
    2. Bash script parallelized - DONE
    3. subprocess parallelized - DONE
    4. dsdgen & dbgen thread & child split output files - DONE  


3. Data Generation - Pipeline Development
    1. Sequential generation
    2. Parallel generation
    3. TPC-DS
        1. Generate 1GB data locally - DONE
        2. Generate 2GB data locally - DONE
    4. TPC-H
        1. Generate 1GB data locally - DONE
        2. Generate 2GB data locally - DONE


4. Naive BQ Data Loading  
    1. FUSE - DONE, but transfer fails, switch to GCS intermediate
    2. BQ API - DONE, direct python driver uploads properly


5. Data Upload to Cloud Storage
    1. Write methods using python driver - DONE, fails on large files though
    2. Write methods using resumable media - DONE, all files upload ok
    3. Write nested flat file schema for tests - DONE  


6. TPC-DS Schema & Query Generation  
    1. Modify ANSI SQL schema to BQ formatting - DONE  
    2. Modify Query SQL to BQ SQL - DONE  


7. Refactor packages
    1. Combine DS & H shared methods - DONE
    2. Rationalize names & methods - DONE
    3. Coordinate local, GCS, table names - DONE


8. Snowflake Account Setup  
    1. Snowflake account setup - user account done, organization pending
    2. Snowflake billing setup - CC ok, organization still pending


9. Validation Scale Loading GCS > BigQuery
    1. 1GB - TODO again
    2. 2GB - TODO again


10. Validation Scale Loading GCS > Snowflake  
    3. 1GB - TODO
    4. 2GB - TODO


11. Draft Query Experiment Structure
    1. File structure for query drafts
    2. Experimental sequence
    3. Query sequencing
        1. TPC-H
        2. TPC-DS


12. Generate TPC-H ANSI Queries
    1. Subprocess binary execution
    2. Modifify source to avoid sqlserver error
    3. Write query sequencer


13. Generate TPC-D ANSI Queries
    1. Subprocess binary execution
    2. Write query sequencer


14. Convert TPC-H ANSI Queries to Naive Syntax
    1. BQ
    1. SF
    
    
15. Convert TPC-H ANSI Queries to Naive Syntax
    1. BQ
    1. SF
    
    
16. Validation Database Loads
    1. 1GB BQ
    1. 1GB SF
    
17. Naive Query Execution
    a. 1GB Scale Factor
        1. DS suite time
        2. H suite time
        3. DS per query time
        4. H per query time
    b. 2GB Scale Factor
        1. DS suite time
        2. H suite time
        3. DS per query time
        4. H per query time
    e. Evaluate differences
    f. Identify additional units of measure
    

18. Generate 100GB Scale Factor Data
    1. DS
    2. H


19. Convert Naive Syntax DDL & Queries with Clustering
    1. BQ
    1. SF
    
    
20. Convert Naive Syntax DDL & Queries with Partitions
    1. BQ
    1. SF


21. Convert Naive Syntax Queries with Query Order
    1. BQ
    1. SF


22. Load 100GB Scale Factor
    1. H
        1. Naive Queries
        2. Clustering
        3. Partitions
        4. Query Order changes
    2. DS
        1. Naive Queries
        2. Clustering
        3. Partitions
        4. Query Order changes


23. Evaluate Test Profiles
    1. 100GB, Naive
    2. 100GB, Clustered
    3. 100GB, Partitioned
    4. 100GB, Clustered & Partitioned
    5. 100GB, Alternative Clustered & Partitioned
    6. 100GB, Table Order, Clustered & Partitioned
    7. 100GB, Table Order, Alternative Clustered & Partitioned


24. Evaluate Results
    1. Summarize Performance
    2. Identify need for further Test Profiles
    3. Identify most performant Test Profile in 100GB


25. Generate Datasets on VM
    1. 1TB
    2. 10TB


26. Load 1TB Data
    1. BQ
        1. Naive
        2. Most performant Test Profile
    2. SF
        1. Naive
        2. Most performant Test Profile


27. Evaluate 1TB Queries
    1. Summarize Performance
    2. Identify areas of improvement
    
    
28. Load 10TB Data
    1. BQ
        1. Naive
        2. Most performant Test Profile from 1TB
    2. SF
        1. Naive
        2. Most performant Test Profile from 1TB


29. Best Practices Review
    1. BQ
    2. SF
    

30. 2nd round comparisons
    1. query categories
    2. query orders
    3. cold start vs warm start / precached
    4. replacement queries
    
    
31. Draft Results
    1. Comparison Categories
    2. Caching
    3. Plots
    4. Notebook format & Markdown


32. Client Feedback  


33. Rewrite Draft Results
    a. Report Draft
    b. blog format
    
34. Apply License and Publish Code
    1. Remove project specific config values
    2. Remove dev files
    3. Reformat Notebooks


## Time and Allocation  
The Scope of Work states this project will take 100 hrs total to complete using the following capacities:  

| Role | Allocation | Equvalent Hours |  
| ---- | ---------- | --------------- |   
| Project Owner | 25% | 14 |  
| Subject Area Expert | 25% | 14 |  
| Project Manager | 25% | 14 |  
| Technical Lead | 100% | 57 |  

## Deliverable  

| Deliverable | Description |  
| ----------- | ----------- | 
| Benchmark Outline | Full outline of the benchmark document to be published |  
| Benchmark Draft | A complete first draft for joint review, feedback and ratification for publication |  
| Benchmark | URL to public-readable benchmark document |  


## PreSales Timeline Description    
Miles and the Scope of Work estimated this work will take 3-4 weeks.  

| Milestone | Timeline |  
| --------- | -------- |  
| kickoff | 0 |
| benchmark outline | + 1 week |  
| benchmark draft | +3 weeks |  
| benchmark | +3-4 weeks |  

## Data Structure  

### Code Repo  
project_root
    |---ds
    |---output
    |   |---tpc_binaries
    |
    |---h
        |---output
            |---tpc_binaries
    |---downloads
    (all source code)

### Mounted Persistent Disk  
The mounted persistent disk is where all data is written using a GCP VM.  A size of 20TB was provisioned to allow complete generation of the 10TB data and then transfer to Cloud Storage.  

### File Output and Folder Structure  

#### Data Generated  
Files are written to with dsdgen and dbgen.  Since these tools only support writing to a target directory, for each size in the study a separate folder is created: `1GB`, `100GB`, `1000GB` (1TB), `10000GB` (10TB)

TPC-DS uses underscore delimitation in the generated files
Example: `customer_demographics_7_12_.dat` is the 7th of 12 fractions of a parallel generation of the customer demographics table.  

TPC-H uses simple `tablename.tbl.n` where n is the nth fraction of a parallel generation task.
Example: `customer.tbl.8` is the 8th part of the customer table data.

/output
    |---ds
    |   |---1GB
    |   |---100GB
    |   |---1000GB
    |   |---10000GB
    |
    |---h
        |---1GB
        |---100GB
        |---1000GB
        |---10000GB


#### Queries Generated and Modified  

The default SQL is generated in ANSI SQL and must be modified to work in BigQuery and Snowflake.  The `01_ansi` directory is the output of the TPC query gen tools.  The git branch `dev_query` is then used to copy and alter each subsequent improvement of the original queries.  

/query
    |---ds
    |   |---01_ansi
    |   |---02_naive_bq
    |   |---02_naive_sf
    |   |---03_naive_bq_validation
    |   |---03_naive_sf_validation
    |   |---04_ex1_bq
    |   |---04_ex1_sf
    |   |---05_ex2_bq
    |   |---05_ex2_sf
    |   |---06_ex3_bq
    |   |---06_ex3_sf
    |
    |---h
        |---01_ansi
        |---02_naive_bq
        |---02_naive_sf
        |---03_naive_bq_validation
        |---03_naive_sf_validation
        |---04_ex1_bq
        |---04_ex1_sf
        |---05_ex2_bq
        |---05_ex2_sf
        |---06_ex3_bq
        |---06_ex3_sf

### Google Cloud Storage Bucket Structure  
Naming for bucket files is a bit different, given that folders don't exist on GCP.  

https://cloud.google.com/storage/docs/gsutil/addlhelp/HowSubdirectoriesWork  

Given that TPC-DS uses underscore delimitation and TPC-H uses a dot delimiter on the part suffix only, prepending folder names to the blob names seems like the simplest translation.  


Abstracting the /mnt/disks/20tb/ structure to:  
storage-root
    |---tpc-test
        |---scale-factor
            |---one.tbl
            |---two_data_3_96.dat

The paths would be:
/storage-root/tpc-test/scale-factor/one.tbl  
/storage-root/tpc-test/scale-factor/two_data_3_96.dat  

And converted to GSC flat files they would be:  
gcs-bucket_tpc-test_scale-factor_one.tbl  
gcs-bucket_tpc-test_scale-factor_two_data_3_96.dat  

Application Notes  
1. Underscore `_` is only used as replacement for folder delimiter (`\` or `/`) replacement
2. Any text to the right of the 3rd underscore is the original filename as generated by dsdgen or dbgen.  This will facilitate extracting the target table name from the original filename.  


## TPC  
This work uses two TPC tests, DS and H, to create a reproducible load on a data storage system.  TPC the organization started as benchmarking tools for debit card transactions in the early 1980s.  

TPC-DS - TPC Benchmark DS - decision support  
http://www.tpc.org/tpcds/  

TPC-DS is a Decision Support Benchmark  

TPC-DS is the de-facto industry standard benchmark for measuring the performance of decision support solutions including, but not limited to, Big Data systems. The current version is v2. It models several generally applicable aspects of a decision support system, including queries and data maintenance. Although the underlying business model of TPC-DS is a retail product supplier, the database schema, data population, queries, data maintenance model and implementation rules have been designed to be broadly representative of modern decision support systems.
This benchmark illustrates decision support systems that:
* Examine large volumes of data
* Give answers to real-world business questions
* Execute queries of various operational requirements and complexities (e.g., ad-hoc, reporting, iterative OLAP, data mining)
* Are characterized by high CPU and IO load
* Are periodically synchronized with source OLTP databases through database maintenance functions
* Run on “Big Data” solutions, such as RDBMS as well as Hadoop/Spark based systems

The TPC Benchmark DS (TPC-DS) is a decision support benchmark that models several generally applicable aspects of a decision support system, including queries and data maintenance. The benchmark provides a representative evaluation of performance as a general purpose decision support system. A benchmark result measures query response time in single user mode, query throughput in multi user mode and data maintenance performance for a given hardware, operating system, and data processing system configuration under a controlled, complex, multi-user decision support workload. The purpose of TPC benchmarks is to provide relevant, objective performance data to industry users. TPC-DS Version 2 enables emerging technologies, such as Big Data systems, to execute the benchmark.

TPC-H is a Decision Support Benchmark  

The TPC Benchmark H (TPC-H) is a decision support benchmark. It consists of a suite of business oriented ad-hoc queries and concurrent data modifications. The queries and the data populating the database have been chosen to have broad industry-wide relevance. This benchmark illustrates decision support systems that examine large volumes of data, execute queries with a high degree of complexity, and give answers to critical business questions. 

The performance metric reported by TPC-H is called the TPC-H Composite Query-per-Hour Performance Metric (QphH@Size), and reflects multiple aspects of the capability of the system to process queries. These aspects include the selected database size against which the queries are executed, the query processing power when queries are submitted by a single stream, and the query throughput when queries are submitted by multiple concurrent users. The TPC-H Price/Performance metric is expressed as dollars per QphH@Size

#### Specification.pdf  

pg 9: 
The purpose of TPC benchmarks is to provide relevant, objective performance data to industry users. To achieve that purpose, TPC benchmark specifications require that benchmark tests be implemented with systems, products, technologies and pricing that:
• Are generally available to users;
• Are relevant to the market segment that the individual TPC benchmark models or represents (e.g., TPC-H models and represents complex, high data volume, decision support environments);
• Would plausibly be implemented by a significant number of users in the market segment the benchmark models or represents.

The use of new systems, products, technologies (hardware or software) and pricing is encouraged so long as they meet the requirements above. Specifically prohibited are benchmark systems, products, technologies or pricing (hereafter referred to as "implementations") whose primary purpose is performance optimization of TPC benchmark results without any corresponding applicability to real-world applications and environments. In other words, all "benchmark special" implementations that improve benchmark results but not real-world performance or pricing, are prohibited.

### Downloading TPC-H and TPC-DS  
The TPC data has to be downloaded to be used and a license agreement must be filled out manually.  
http://www.tpc.org/tpc_documents_current_versions/current_specifications5.asp

After which, emails are sent with download links.  Note these links are not for external sharing.

Both source programs were downloaded locally 2020-02-24.  

TPC-H_Tools_v2.18.0.zip  
from  
```http://www.tpc.org/tpc_documents_current_versions/download_programs/tools-download5.asp?bm_type=TPC-H&download_key=80ACE813%2D3CFD%2D4EA2%2D9988%2D0D2910D5E72D```

TPC-DS_Tools_v2.11.0.zip  
from ```http://www.tpc.org/tpc_documents_current_versions/download_programs/tools-download5.asp?bm_type=TPC-DS&download_key=62E009C9%2D6434%2D4285%2DA04A%2D9C0817377874```

### Building TPC-H and TPC-DS  
In both sets of directions below, `tpc_root` is the base directory of the uncompressed .zip files downloaded in the previous step.  

There are two parts to building TPC tests.  First, there is creation of the test data and second is the creation of SUT specific 

### Building TPC-H  

Editing makefile.suite:  
https://www.haidongji.com/2011/03/30/data-generation-with-tpc-hs-dbgen-for-load-testing/  

### Building TPC-DS V1  

1. Change directory to the tools folder in tpc_root
`> cd tpc_root/tools`
2. Compile dsdgen and dsqgen using make.  Note that the default target environment is linux so nothing needs to be changed in the makefile or Makefile.suite
`> make`  
3. Generate the default data (1 GB) using:  
`> dsdgen`  
Which will save pipe delimited data with extension .data into the tpc_root/tools directory.
4. Edit `tpc_root/query_templates/sql_server.tpl` by adding the following line:  
`define _END = "";`  
This fixes a compile error as detailed here:  
https://dba.stackexchange.com/questions/36938/how-to-generate-tpc-ds-query-for-sql-server-from-templates/40436  
Note that the error refers to '_END' which is not present in the sql templates.
(This may need to be debugged again later -CRD)
5. Create the sql tests using dsqgen  
`./dsqgen -DIRECTORY ../query_templates -INPUT ../query_templates/templates.lst -VERBOSE Y -QUALIFY Y -SCALE 1 -DIALECT sqlserver -OUTPUT_DIR ../query_sqlserver`  
This will generate one output file in the OUTPUT_DIR named `query_0.sql`  
6. Alter tpcds.sql table definition file to align with BigQuery data types as follows:  


Query generation edit to include `_END == ""`  
https://dba.stackexchange.com/questions/36938/how-to-generate-tpc-ds-query-for-sql-server-from-templates/40436
https://dba.stackexchange.com/questions/36938/how-to-generate-tpc-ds-query-for-sql-server-from-templates

### Building TPC-DS V2  

1. Download and install conda 3.7 for 64bit 
2. Download source python to server
3. sudo apt-get install make gcc git curl
4. $python ds_pipeline_1.py
5. Copy tpc-ds zip to /downloads folder
6. $ python ds_pipeline_2.py
7. bash dsdgen_cpu_1_scale_1GB.sh


#### Generating TPC Data  
Note 1: Fivetran has a bash script for generating on a 16 core machine:  
https://github.com/fivetran/benchmark/blob/master/2-GenerateData.sh  


#### Loading TPC Data into BigQuery  


#### Validating Installation  
##### TPC-H  
TPC-H defines query validations to prove the installation is operating correctly.  Specific values are given as inputs to each query must produce pre-defined outputs.  Section 2.4 describes the queries used in the benchmark and the validation inputs and outputs.  For automated testing, `/dbgen/answers` contains the expected outputs.


### Setup Experience  

### Ops Tasks - Operations Tasks  

Loading into BQ from other sources  
https://cloud.google.com/compute/docs/disks/  
https://cloud.google.com/compute/docs/disks/gcs-buckets  
https://cloud.google.com/storage/docs/gcs-fuse  
https://cloud.google.com/storage/docs/gcs-fuse#using  
https://cloud.google.com/bigquery/docs/loading-data-local
https://cloud.google.com/bigquery/docs/loading-data-cloud-storage-csv#limitations  
https://cloud.google.com/bigquery-transfer/docs/cloud-storage-transfer  
https://github.com/GoogleCloudPlatform/gcsfuse/blob/a8d9f02/docs/mounting.md#unmounting

#### gcsfuse
1. Follow directions here to add apt source and install gcsfuse:  
`https://github.com/GoogleCloudPlatform/gcsfuse/blob/master/docs/installing.md`  
2. Ensure the Compute Engine Instance was created with ALL API access credentials
3. Choose what storege bucket to use, here we'll use 'test_bucket' for the example
4. ``` sudo mkdir /mnt/disks/test_bucket```
5. ``` sudo chmod 777 /mnt/disks/test_bucket```
6. ``` gcsfuse test_bucket /mnt/disks/test_bucket```


## TCO - Total Cost of Ownership?  

## Output Graphics  


## Reference Articles  

### TPC    
http://www.tpc.org/tpcds/

https://www.haidongji.com/2011/03/30/data-generation-with-tpc-hs-dbgen-for-load-testing/  

http://www.tpc.org/tpcds/presentations/tpcds_workload_analysis.pdf

#### Vertica  
https://www.vertica.com/wp-content/uploads/2017/01/Benchmarks-Prove-the-Value-of-an-Analytical-Database-for-Big-Data.pdf

The literally just reported what queries didn't run out of the TPC-DS 99 query suite and then timed what would on a couple of installs.  No mention of even why they didn't run.


#### Fivetran  
https://fivetran.com/blog/warehouse-benchmark  

Benchmark 
https://www.datacouncil.ai/blog/redshift-versus-snowflake-versus-bigquery-/-part-1-performance
https://github.com/fivetran/benchmark

The approximate method of creating things:
1. develop presto.sh
2. copy to google storage (see 1-LaunchDataproc.sh)
3. run 1-LaunchDataproc.sh - this will make a 50 worker cluster to generate data faster
    a. presto.sh appears to be a copy of Google's.  See
        https://github.com/GoogleCloudDataproc/initialization-actions

#### AtScale  
https://github.com/AtScaleInc/benchmark  


#### Data Council  
https://www.datacouncil.ai/blog/redshift-versus-snowflake-versus-bigquery-/-part-1-performance  

#### Dwgeek  
https://dwgeek.com/steps-to-generate-and-load-tpc-ds-data-into-netezza-server.html/  

#### Panoply  
https://blog.panoply.io/a-full-comparison-of-redshift-and-bigquery  


#### Xplenty  
https://www.xplenty.com/blog/snowflake-vs-bigquery/  

#### General TPC Data Generation  
https://github.com/cloudera/impala-tpcds-kit/tree/master/tpcds-gen
https://stackoverflow.com/questions/56631851/how-to-generate-tables-in-parallel-with-tpc-ds-benchmark  
https://github.com/maropu/spark-tpcds-datagen  
https://github.com/dhiraa/spark-tpcds  
http://www.greenplumguru.com/?p=1045  
https://github.com/pivotalguru/TPC-DS  


### Snowflake  

Snowflake comes in multiple instance types, they are:

#### Snowflake Standard:
Robust SQL functionality. Data is always encrypted when it is sent over the public Internet or when it is stored on disk.

#### Snowflake Premier:
Standard plus support for SLAs, and 24 x 365 service/support.

#### Snowflake Enterprise:
Premier plus 90-day time travel, multi-cluster warehouse, and materialized views.

#### Snowflake Business Critical:
Data is encrypted when it is transmitted over the network within the Snowflake VPC. Additionally, the metadata for all customer queries including the SQL text is fully encrypted at the application level before it is persisted to the metadata store.

Business Critical is Snowflake’s solution for customers with specific compliance requirements. It includes HIPAA support, is PCI compliant and features an enhanced security policy. Customers must enter into a Business Associate Agreement (“BAA”) with Snowflake before uploading any HIPAA data to the service. To obtain a Business Associate Agreement (“BAA”) please reach out to sales.

#### Snowflake Configuration Used  
Edition: Standard
Provider: Amazon Web Services
Region: US West (Oregon)


A good diagram of Snowflake architecture layers and caching
https://www.analytics.today/blog/caching-in-snowflake-data-warehouse  

Feature Engineering in Snowflake  
https://towardsdatascience.com/feature-engineering-in-snowflake-1730a1b84e5b  

Why You Need to Know Snowflake As A Data Scientist  
https://towardsdatascience.com/why-you-need-to-know-snowflake-as-a-data-scientist-d4e5a87c2f3d  


### BigQuery  

A good diagarm of BigQuery architecture layers
https://panoply.io/data-warehouse-guide/bigquery-architecture/  

Whitepaper on BQ internals  
https://cloud.google.com/files/BigQueryTechnicalWP.pdf

https://cloud.google.com/bigquery/docs/best-practices-costs
https://cloud.google.com/bigquery/docs/best-practices-storage

Loading CSV Data  
https://cloud.google.com/bigquery/docs/loading-data-cloud-storage-csv
(note if there's a batch comment?)
https://stackoverflow.com/questions/17416562/loading-from-google-cloud-storage-to-big-query-seems-slow
(useful comment by Jordan)

### Clustering Partitioning
Clustering, Time Partitioning and Range Partitioning are possible in BigQuery.  
Some details:
* Clustering must be done at table creation.  
* Clustering is only possible if time partitioning is also specified.(check if this also applies to range partitioning).  
* Range partitioning is still in beta.  



### Google Cloud Storage  

GCS client libraries for Python  
https://googleapis.dev/python/storage/latest/client.html  

Google Media Downloads and Resumable Uploads library for Python  
https://googleapis.github.io/google-resumable-media-python/latest/index.html  




### Unrelated Notes:
1. https://github.com/GoogleCloudDataproc/initialization-actions/tree/master/jupyter
    is an installer using conda for Python3 and Jupyter in Dataproc.  Look into this more!

2. BigQuery Jupyter integrations  
https://cloud.google.com/bigquery/docs/visualize-jupyter  

3. Snowflake Jupyter Notebook  
https://www.snowflake.com/blog/quickstart-guide-for-sagemaker-snowflake-part-one/  

### SQL Snippets  
1. Size of table in BQ  
    https://stackoverflow.com/questions/31266740/how-to-get-bigquery-storage-size-for-a-single-table
2. 


### TPC-DS Specification Notes  
# TPC-DS Specification  
Excerpts from TPC-DS Specification V1.0.0L, from TPC-DS v2.11.0rc2  
See TPC-DS_v2.11.0rc2/specification/specification.pdf  

## 0. Preamble  

### Associated Materials  
specification.pdf, section 0.5, pg. 13:  

| Content | File Name/Location | Usage | Additional Information |
| ------- | ------------------ | ----- | ---------------------- |
| Data generator | dsdgen | Used to generate the data sets for the benchmark  | Clause 3.4 |  
| Query generator | dsqgen | Used to generate the query sets for the benchmark | Clause 4.1.2 |  
| Query Templates | query_templates/ | Used by dsqgen to generate executable query text | Clause 4.1.3 |  
| Query Template Variants | query_variants/ | Used by dsqgen to generate alternative executable query text |  Appendix C |  
| Table definitions in ANSI SQL | tpcds.sql tpcds_source.sql | Sample implementation of the logical schema for the data warehouse. |  Appendix A |  
| Data Maintenance Functions in ANSI SQL | data_maintenance/ | Sample implementation of the SQL needed for the Data Maintenance phase of the benchmark | Clause 5.3 |  
| Answer Sets | answer_sets/ | Used to verify the initial population of the data warehouse. | Clause 7.3 |  
| Reference Data Set | run dsdgen with –validate flag | Set of files for each scale factor to compare the correct data generation of base data. | None |  

## 1. Business and Benchmark Model  

TPC-DS models the decision support functions of a retail product supplier. The supporting schema contains vital business information, such as customer, order, and product data. The benchmark models the two most important components of any mature decision support system:  

* User queries, which convert operational facts into business intelligence.
* Data maintenance, which synchronizes the process of management analysis with the operational external data source on which it relies.  

The benchmark abstracts the diversity of operations found in an information analysis application, while retaining essential performance characteristics. As it is necessary to execute a great number of queries and data transformations to completely manage any business analysis environment, no benchmark can succeed in exactly mimicking a particular environment and remain broadly applicable.

Although the emphasis is on information analysis, the benchmark recognizes the need to periodically refresh its data. The data represents a reasonable image of a business operation as they progress over time. 

Some TPC benchmarks model the operational aspect of the business environment where transactions are executed on a real time basis. Other benchmarks address the simpler, more static model of decision support. The TPC-DS benchmark, models the challenges of business intelligence systems where operational data is used both to support the making of sound business decisions in near real time and to direct long-range planning and exploration.

## GCP Setup  
1. Create service account key for local development:  
    https://cloud.google.com/iam/docs/creating-managing-service-account-keys#iam-service-account-keys-create-python
2. 

## VM Setup  
An AI Notebook can be used to run all code in this project.  Two instances are suggested to reduce costs:
1. High CPU Count to create data
    a. 96 cores, 86.4 GB Ram
    b. boot disk of 40TB persistent disk
2. Modest specs to run queries and modify SQL Queries


## Conda Environment Setup  
There are two Conda environment files,  
`environment_tpc.yml` - used to update the base conda environment run on GCP AI Notebooks  
See step 7. in Setup for command line argument to update base conda environment.

### Installing Conda  
https://docs.conda.io/projects/conda/en/latest/user-guide/tasks/manage-environments.html  


### Setup Steps  

1. Create an new AI Notebook on GCP
2. Add an additional persistent disk larger than 20TB
3. Start instance and open Jupyter Lab
4. Open new terminal from the plus menu (+)
5. Add ssh key to github repo if required
    see: https://help.github.com/en/github/authenticating-to-github/generating-a-new-ssh-key-and-adding-it-to-the-ssh-agent  
6. Once the ssh key is generated, you can view the hash in a Notebook with:
    ```
    with open("/home/jupyter/.ssh/id_rsa.pub", "r") as f:
        for line in f:
            print(line)
    ``` 
    and paste into github.com credential page
7. git clone the repo
8. cd to the repo and update the base conda environment with
    ```
    conda env update --file environment_tpc.yml --name base
    ```
9. add the repo to the conda path so notebooks will find the package in the python path:  `conda develop /home/jupyter/code/bq_snowflake_benchmark`
10. add the persistent disk to a mount point
    https://devopscube.com/mount-extra-disks-on-google-cloud/
    mount the persistent disk to the same value as `config.fp_output_mnt`, this will likely be "/mnt/disks/data"
11. Since Jupyter loads in the `jupyter` user directory, create a symbolic link to the /data mount point with:  
`$ln -s /data /home/jupyter/code/bq_snowflake_benchmark/data`  
(a new directory `data` should appear in the project folderin in Jupyter Lab)

### Query Results Quality Control  
For both TPC tests, query templates were copied and edited to create templates that when run with the query generation binaries produced executable SQL for BigQuery and Snowflake.  Since all operations in this benchmarking effort used Python and each database's official Python client, the outputs of each query were accessed using the respective client's Pandas Dataframe conversion method.  Comparison was then done using Pandas methods.  

Results were collected as follows:
1. Query SQL was generated for the appropriate database and scale factor and executed via the client
2. Pandas Dataframes are output by each database client module
3. Dataframes are saved to CSV files in the query sequence results directory
4. Anytime after the query sequence is done, the CSV files are loaded into Pandas Dataframes
5. The `tools.consistent` method is used on the loaded Dataframes which does the following:
    a. Converts table column names to lowercase
    b. Converts any columns with numeric like data using pandas.to_numeric method
    c. Fills None/NaN values to numeric `-9999.99` to avoid evaluations failing due to None == None evaluating to False
6. Evaluate Dataframe differences using pandas.testing.assert_fame_equal using:

a. `check_names=False` : Whether to check that the names attribute for both the index and column attributes of the DataFrame is identical.  Snowflake and BigQuery return upper and lowercase different column labels which are not important for query comparisons.
b. `check_exact=False` : Whether to compare number exactly.  Explicitly set to False to ensure use of `check_less_precise` is readable.
c. `check_less_precise=2` : Specify comparison precision. Only used when check_exact is False. 5 digits (False) or 3 digits (True) after decimal points are compared. If int, then specify the digits to compare.

When comparing two numbers, if the first number has magnitude less than 1e-5, we compare the two numbers directly and check whether they are equivalent within the specified precision. Otherwise, we compare the ratio of the second number to the first number and check whether it is equivalent to 1 within the specified precision.

For this project, BigQuery defaults to returning 2 or 3 decimal places in Dataframes after conversion to numeric types.  Snowflake's client returns object columns with Decimal class contents which when converted to float dtype columns results in different numbers of decimal values.  In cases where the dissimilar additional decimal place is a 5, an evaluation based on decimal format or rounding produces values off by the last decimal place.  By setting `check_less_precise` to 2, all values are only compared to 2 decimal places regardless of additional decimal places available in the value.  


### QUERY Alterations
TPC-DS 4.1.3.1 - Query syntax is phrased in SQL1999 with OLAP amendments
ANSI/ISO/IEC 9075-2:1999 Part 2, pg 269, 7.12.5 - 
```If UNION, EXCEPT, or INTERSECT is specified and neither ALL nor DISTINCT is specified, then DISTINCT is implicit.```

Snowflake
UNION     >> distinct records
UNION ALL >> all records, even duplicates

BigQuery
UNION DISTINCT >> distinct records
UNION ALL      >> all records, even duplicates

Therefore, the query conversion is as follows:

TPC-DS >> Snowflake
UNION  >> UNION

TPC-DS >> BigQuery
UNION  >> UNION DISTINCT


Note: distinct takes more compute as it has to find all duplicateds

https://cloud.google.com/bigquery/docs/reference/standard-sql/query-syntax#union
https://dwgeek.com/snowflake-set-operators-union-except-minus-and-intersect.html/
https://docs.snowflake.com/en/sql-reference/operators-query.html#union-all

### refresh queries:
TPC-H: 

generate refresh dataset with `dbgen -s 1 -U 1 -S 1 -i 1 -d 1 -f` 
generate sql statements with `python refresh.py`

TPC-DS:

generate refresh dataset with `./dsdgen -UPDATE = 1 -DIR = upd`
generate sql statements with `python dsrefresh.py`
