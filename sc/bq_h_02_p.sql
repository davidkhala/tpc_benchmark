-- TPC-H
-- Partition Strategy I
-- 
-- Optimize the largest tables, specifically:
-- | table    | column      | TPC type | BQ type |
-- | -------- | ----------- | -------- | ------- |
-- | lineitem | l_shipdate  | Date     | Date    |
-- | orders   | o_orderdate | Date     | Date    |
-- 
-- Google's Documentation on partitioning:
-- https://cloud.google.com/bigquery/docs/creating-column-partitions
-- 
-- TPC-H Specification section discussing partitioning:
-- 1.5.4 Horizontal partitioning of base tables or auxiliary structures created by database directives (see Clause 1.5.7) is
-- allowed. Groups of rows from a table or auxiliary structure may be assigned to different files, disks, or areas. If this
-- assignment is a function of data in the table or auxiliary structure, the assignment must be based on the value of a
-- partitioning field. A partitioning field must be one and only one of the following:
-- * A column or set of columns listed in Clause 1.4.2.2, whether or not it is defined as a primary key
-- constraint;
-- * A column or set of columns listed in Clause 1.4.2.3, whether or not it is defined as a foreign key constraint;
-- * A column having a date datatype as defined in Clause 1.3.
-- Some partitioning schemes require the use of directives that specify explicit values for the partitioning field. If such
-- directives are used they must satisfy the following conditions:
-- * They may not rely on any knowledge of the data stored in the table except the minimum and maximum
-- values of columns used for the partitioning field. The minimum and maximum values of columns are
-- specified in Clause 4.2.3
-- * Within the limitations of integer division, they must define each partition to accept an equal portion of the
-- range between the minimum and maximum values of the partitioning column(s). For date-based partitions,
-- it is permissible to partition into equally sized domains based upon an integer granularity of days, weeks,
-- months, or years (e.g., 30 days, 4 weeks, 1 month, 1 year, etc.). For date-based partition granularities other
-- TPC Benchmark TM H Standard Specification Revision 2.18.0
-- Page 19than days, a partition boundary may extend beyond the minimum or maximum boundaries as established in
-- that table’s data characteristics as defined in Clause 4.2.3.
-- * The directives must allow the insertion of values of the partitioning column(s) outside the range covered by
-- the minimum and maximum values, as required by Clause 1.5.13.
-- Multiple-level partitioning of base tables or auxiliary structures is allowed only if each level of partitioning satisfies
-- the conditions stated above and each level references only one partitioning field as defined above. If implemented,
-- the details of such partitioning must be disclosed.
-- 
-- Usage
-- Before issuing a DDL command with this file, find and replace the following values to create a valid statement:
-- "_destination_dataset" : dataset being populated
-- "_source_dataset" : dataset that data is being copied from
-- 
-- Google Documentation:
-- https://cloud.google.com/bigquery/docs/reference/standard-sql/data-definition-language
-- 

CREATE TABLE `_destination_dataset.lineitem`
PARTITION BY l_shipdate
AS 
SELECT * FROM `_source_dataset.lineitem`;

CREATE TABLE `_destination_dataset.orders`
PARTITION BY o_orderdate
AS 
SELECT * FROM `_source_dataset.orders`;

CREATE TABLE `_destination_dataset.partsupp`
AS 
SELECT * FROM `_source_dataset.partsupp`;

CREATE TABLE `_destination_dataset.part`
AS 
SELECT * FROM `_source_dataset.part`;

CREATE TABLE `_destination_dataset.customer`
AS 
SELECT * FROM `_source_dataset.customer`;

CREATE TABLE `_destination_dataset.supplier`
AS 
SELECT * FROM `_source_dataset.supplier`;

CREATE TABLE `_destination_dataset.nation`
AS 
SELECT * FROM `_source_dataset.nation`;

CREATE TABLE `_destination_dataset.region`
AS 
SELECT * FROM `_source_dataset.region`;
