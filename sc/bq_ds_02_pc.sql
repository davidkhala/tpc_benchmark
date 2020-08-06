-- TPC-DS
-- Partition & Cluster Strategy I
-- 
-- Optimize the largest tables, specifically:
-- | table           | column              | TPC type   | BQ type |
-- | --------------- | ------------------- | ---------- | ------- |
-- | catalog_returns | cr_returned_date_sk | identifier | integer |
-- | catalog_sales   | cs_sold_date_sk     | identifier | integer |
-- | inventory       | inv_date_sk         | identifier | integer |
-- | store_returns   | sr_returned_date_sk | identifier | integer |
-- | store_sales     | ss_solid_date_sk    | identifier | integer |
-- | web_returns     | wr_returned_date_sk | identifier | integer |
-- | web_sales       | ws_sold_date_sk     | identifier | integer |
-- 
-- 1. Partitioning
--
-- Google's documentation on partitioning:
-- https://cloud.google.com/bigquery/docs/creating-column-partitions
-- 
-- TPC-DS Specification section discussing partitioning: 
-- 2.5.3.8.3 Horizontal partitioning of base tables or EADS is allowed. If the partitioning is a function of data in the table
-- or auxiliary data structure, the assignment shall be based on the values in the partitioning column(s). Only
-- primary keys, foreign keys, date columns and date surrogate keys may be used as partitioning columns. If
-- partitioning DDL uses directives that specify explicit partition values for the partitioning columns, they shall
-- satisfy the following conditions:
-- * They may not rely on any knowledge of the data stored in the partitioning column(s) except the minimum
-- and maximum values for those columns, and the definition of data types for those columns provided in
-- Clause 2.
-- * Within the limitations of integer division, they shall define each partition to accept an equal portion of the
-- range between the minimum and maximum values of the partitioning column(s).
-- * For date-based partitions, it is permissible to partition into equally sized domains based upon an integer
-- granularity of days, weeks, months, or years; all using the Gregorian calendar (e.g., 30 days, 4 weeks, 1
-- month, 1 year, etc.). For date-based partition granularities other than days, a partition boundary may extend
-- beyond the minimum or maximum boundaries as established in that table’s data characteristics as defined
-- in Clause 3.4
-- * The directives shall allow the insertion of values of the partitioning column(s) outside the range covered by
-- the minimum and maximum values, as required by Clause 1.5.
-- If any directives or DDL are used to horizontally partition data, the directives, DDL, and other details necessary
-- to replicate the partitioning behavior shall be disclosed.
-- Multi-level partitioning of base tables or auxiliary data structures is allowed only if each level of partitioning
-- satisfies the conditions stated above.
--
-- 2. Clustering
--
-- Google's documentation on clustering:
-- https://cloud.google.com/bigquery/docs/creating-clustered-tables
-- https://cloud.google.com/bigquery/docs/reference/standard-sql/data-definition-language#creating_a_clustered_table_from_the_result_of_a_query
-- 
-- TPC-DS Specification section discussing clustering:
-- 2.5.2.5 The physical clustering of records of different tables within the database is allowed as long as this clustering
-- does not alter the logical relationships of each table.
-- Comment: The intent of this clause is to permit flexibility in the physical layout of a database and based upon
-- the defined TPC-DS schema.
-- 10.3.2.2 The physical organization of tables and indices within the test and qualification databases must be disclosed. If
-- the column ordering of any table is different from that specified in Clause2.3 or 2.4,, it must be noted.
-- Comment: The concept of physical organization includes, but is not limited to: record clustering (i.e., rows
-- from different logical tables are co-located on the same physical data page), index clustering (i.e., rows and leaf
-- nodes of an index to these rows are co-located on the same physical data page), and partial fill-factors (i.e.,
-- physical data pages are left partially empty even though additional rows are available to fill them).
-- 
-- Usage
-- Before issuing a DDL command with this file, find and replace the following values to create a valid statement:
-- "_destination_dataset" : dataset being populated
-- "_source_dataset" : dataset that data is being copied from
-- 
-- Google Documentation:
-- https://cloud.google.com/bigquery/docs/reference/standard-sql/data-definition-language
-- 

create table `_destination_dataset.catalog_returns`
PARTITION BY RANGE_BUCKET(cr_returned_date_sk, GENERATE_ARRAY(2450821, 2452922, 1))
CLUSTER BY cr_returned_date_sk
as
select * from `_source_dataset.catalog_returns`;

create table `_destination_dataset.catalog_sales`
PARTITION BY RANGE_BUCKET(cs_sold_date_sk, GENERATE_ARRAY(2450815, 2452654, 1))
CLUSTER BY cs_sold_date_sk
as
select * from `_source_dataset.catalog_sales`;

create table `_destination_dataset.inventory`
PARTITION BY RANGE_BUCKET(inv_date_sk, GENERATE_ARRAY(2450815, 2452635, 1))
CLUSTER BY inv_date_sk
as
select * from `_source_dataset.inventory`;

create table `_destination_dataset.store_returns`
PARTITION BY RANGE_BUCKET(sr_returned_date_sk, GENERATE_ARRAY(2450820, 2452822, 1))
CLUSTER BY sr_returned_date_sk
as
select * from `_source_dataset.store_returns`;

create table `_destination_dataset.store_sales`
PARTITION BY RANGE_BUCKET(ss_sold_date_sk, GENERATE_ARRAY(2450816, 2452642, 1))
CLUSTER BY ss_sold_date_sk
as
select * from `_source_dataset.store_sales`;

create table `_destination_dataset.web_returns`
PARTITION BY RANGE_BUCKET(wr_returned_date_sk, GENERATE_ARRAY(2450820, 2453001, 1))
CLUSTER BY wr_returned_date_sk
as
select * from `_source_dataset.web_returns`;

create table `_destination_dataset.web_sales`
PARTITION BY RANGE_BUCKET(ws_sold_date_sk, GENERATE_ARRAY(2450816, 2452642, 1))
CLUSTER BY ws_sold_date_sk
as
select * from `_source_dataset.web_sales`;

create table `_destination_dataset.dbgen_version`
as
select * from `_source_dataset.dbgen_version`;

create table `_destination_dataset.customer_address`
as
select * from `_source_dataset.customer_address`;

create table `_destination_dataset.customer_demographics`
as
select * from `_source_dataset.customer_demographics`;

create table `_destination_dataset.date_dim`
as
select * from `_source_dataset.date_dim`;

create table `_destination_dataset.warehouse`
as
select * from `_source_dataset.warehouse`;

create table `_destination_dataset.ship_mode`
as
select * from `_source_dataset.ship_mode`;

create table `_destination_dataset.time_dim`
as
select * from `_source_dataset.time_dim`;

create table `_destination_dataset.reason`
as
select * from `_source_dataset.reason`;

create table `_destination_dataset.income_band`
as
select * from `_source_dataset.income_band`;

create table `_destination_dataset.item`
as
select * from `_source_dataset.item`;

create table `_destination_dataset.store`
as
select * from `_source_dataset.store`;

create table `_destination_dataset.call_center`
as
select * from `_source_dataset.call_center`;

create table `_destination_dataset.customer`
as
select * from `_source_dataset.customer`;

create table `_destination_dataset.web_site`
as
select * from `_source_dataset.web_site`;

create table `_destination_dataset.household_demographics`
as
select * from `_source_dataset.household_demographics`;

create table `_destination_dataset.web_page`
as
select * from `_source_dataset.web_page`;

create table `_destination_dataset.promotion`
as
select * from `_source_dataset.promotion`;

create table `_destination_dataset.catalog_page`
as
select * from `_source_dataset.catalog_page`;
