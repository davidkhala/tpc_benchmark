# Benchmark Comparison of BigQuery and Snowflake  

## Project Description  
Perform an analysis of the strengths and weakness of BigQuery when using TPC-DS and TPC-H standards, when compared to SnowFlake.

## Terminology  
TPC uses specific terminology for the work, for a complete list see 
x.xx in TCP-DS specifications.pdf  
y.yy in TPC-H specification.pdf  

For clarity of this document, the following are defined:  
SUT : System Under Test - i.e. the database system being tested

## Scope of Work  

The following topics will be observed and included in the final report:  
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


## Development Timeline  
1. Scope TPC benchmark methods - DONE
2. Localize TPC tests - DONE
3. TPC-DS Data Generation
	a. Generate 1GB data locally - DONE
	b. Generate 1GB data on vm - DONE in ~20 min @ 96 cores
	c. Generate 100GB data on vm - DONE in ~1 hr @ 96 cores
	d. Generate 1000GB, 10TB data on vm
	e. Transfer all data to GCS
4. TPC-DS Schema & Query Generation  
	a. Modify ANSI SQL schema to BQ formatting - DONE
	b. Modify Query SQL to BQ SQL - DONE
5. Naive Loading of BQ
	a. Load 1GB data into BQ
		i. FUSE
		ii. BQ API
	b. Load 100GB data into BQ
	c. Load 1TB, 10TB data into BQ
6. Opitmized Loading of BQ
	a. Load 100GB data into BQ
	b. Load 1TB, 10TB data into BQ

7. Naive Loading of Snowflake
	a. Load 100GB data into Snowflake
	b. Load 1TB data into Snowflake

8. 1st round comparisions
	a. BQ naive vs optimized
	b. BQ v SF
		i. 100GB
		ii. 1TB
	c. SF loaded v SF builtin TPC-DS

9. 2nd round comparisons
	a. query categories
	b. query orders
	c. cold start vs warm start / precached

10-19 Repeat 1-9 for TPC-H
- TPC-DS code is was written to handle both

## TPC  
This work uses two TPC tests to create a reproducible load on a data storage system.  TPC the organization started as benchmarking tools for debit card transactions in the early 1980s.  

For this work we will use two benchmarks:  
* TPC-DS  
* TPC-H  


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
https://github.com/GoogleCloudPlatform/gcsfuse/blob/master/docs/installing.md  
2. Ensure the Compute Engine Instance was created with ALL API access credentials
3. Choose what storege bucket to use, here we'll use 'test_bucket' for the example
4. //$ sudo mkdir /mnt/disks/test_bucket
5. //$ sudo chmod 777 /mnt/disks/test_bucket
	>> TODO: less permissive permission?
6. //$ gcsfuse test_bucket /mnt/disks/test_bucket


## TCO - Total Cost of Ownership?  

## Output Graphics  


## Reference Articles  

### TPC    
http://www.tpc.org/tpcds/

https://www.haidongji.com/2011/03/30/data-generation-with-tpc-hs-dbgen-for-load-testing/  


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


### Snowflake  

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


### Unrelated Notes:
1. https://github.com/GoogleCloudDataproc/initialization-actions/tree/master/jupyter
	is an installer using conda for Python3 and Jupyter in Dataproc.  Look into this more!

2. BigQuery Jupyter integrations  
https://cloud.google.com/bigquery/docs/visualize-jupyter  

3. Snowflake Jupyter Notebook  
https://www.snowflake.com/blog/quickstart-guide-for-sagemaker-snowflake-part-one/  


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