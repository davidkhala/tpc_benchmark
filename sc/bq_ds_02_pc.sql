-- TPC-DS
-- Partition Strategy
-- 
-- Optimize the largest tables, specifically:
-- CATALOG_RETURNS, partition and cluster by cr_returned_date_sk
-- CATALOG_SALES, partition and cluster by cs_sold_date_sk
-- INVENTORY, partition and cluster by inv_date_sk
-- STORE_RETURNS, partition and cluster by sr_returned_date_sk
-- STORE_SALES, partition and cluster by ss_solid_date_sk 
-- WEB_RETURNS, partition and cluster by wr_returned_date_sk
-- WEB_SALES, partition and cluster by ws_sold_date_sk
-- 
-- Google's Documentation on partitioning:
-- https://cloud.google.com/bigquery/docs/creating-column-partitions
-- 
-- Google's documentation on clustering:
-- https://cloud.google.com/bigquery/docs/clustered-tables
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

create table `_destination_table.catalog_returns`
PARTITION BY TIMESTAMP_TRUNC(cr_returned_date_sk, DAY)
CLUSTER BY TIMESTAMP_TRUNC(cr_returned_date_sk, DAY)
as
select * from `_source_table.catalog_returns`;

create table `_destination_table.catalog_sales`
PARTITION BY TIMESTAMP_TRUNC(cs_sold_date_sk, DAY)
CLUSTER BY TIMESTAMP_TRUNC(cs_sold_date_sk, DAY)
as
select * from `_source_table.catalog_sales`;

create table `_destination_table.inventory`
PARTITION BY TIMESTAMP_TRUNC(inv_date_sk, DAY)
CLUSTER BY TIMESTAMP_TRUNC(inv_date_sk, DAY)
as
select * from `_source_table.inventory`;

create table `_destination_table.store_returns`
PARTITION BY TIMESTAMP_TRUNC(sr_returned_date_sk, DAY)
CLUSTER BY TIMESTAMP_TRUNC(sr_returned_date_sk, DAY)
as
select * from `_source_table.store_returns`;

create table `_destination_table.store_sales`
PARTITION BY TIMESTAMP_TRUNC(ss_solid_date_sk, DAY)
CLUSTER BY TIMESTAMP_TRUNC(ss_solid_date_sk, DAY)
as
select * from `_source_table.store_sales`;

create table `_destination_table.web_returns`
PARTITION BY TIMESTAMP_TRUNC(wr_returned_date_sk, DAY)
CLUSTER BY TIMESTAMP_TRUNC(wr_returned_date_sk, DAY)
as
select * from `_source_table.web_returns`;

create table `_destination_table.web_sales`
PARTITION BY TIMESTAMP_TRUNC(ws_sold_date_sk, DAY)
CLUSTER BY TIMESTAMP_TRUNC(ws_sold_date_sk, DAY)
as
select * from `_source_table.web_sales`;

create table `_destination_table.dbgen_version`
as
select * from `_source_table.dbgen_version`;

create table `_destination_table.customer_address`
as
select * from `_source_table.customer_address`;

create table `_destination_table.customer_demographics`
as
select * from `_source_table.customer_demographics`;

create table `_destination_table.date_dim`
as
select * from `_source_table.date_dim`;

create table `_destination_table.warehouse`
as
select * from `_source_table.warehouse`;

create table `_destination_table.ship_mode`
as
select * from `_source_table.ship_mode`;

create table `_destination_table.time_dim`
as
select * from `_source_table.time_dim`;

create table `_destination_table.reason`
as
select * from `_source_table.reason`;

create table `_destination_table.income_band`
as
select * from `_source_table.income_band`;

create table `_destination_table.item`
as
select * from `_source_table.item`;

create table `_destination_table.store`
as
select * from `_source_table.store`;

create table `_destination_table.call_center`
as
select * from `_source_table.call_center`;

create table `_destination_table.customer`
as
select * from `_source_table.customer`;

create table `_destination_table.web_site`
as
select * from `_source_table.web_site`;

create table `_destination_table.household_demographics`
as
select * from `_source_table.household_demographics`;

create table `_destination_table.web_page`
as
select * from `_source_table.web_page`;

create table `_destination_table.promotion`
as
select * from `_source_table.promotion`;

create table `_destination_table.catalog_page`
as
select * from `_source_table.catalog_page`;
