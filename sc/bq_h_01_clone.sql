CREATE TABLE `_destination_table.lineitem`
PARTITION BY l_shipdate
AS 
SELECT * FROM `_source_table.lineitem`;

CREATE TABLE `_destination_table.orders`
PARTITION BY o_orderdate
AS 
SELECT * FROM `_source_table.orders`;

CREATE TABLE `_destination_table.partsupp`
AS 
SELECT * FROM `_source_table.partsupp`;

CREATE TABLE `_destination_table.part`
AS 
SELECT * FROM `_source_table.part`;

CREATE TABLE `_destination_table.customer`
AS 
SELECT * FROM `_source_table.customer`;

CREATE TABLE `_destination_table.supplier`
AS 
SELECT * FROM `_source_table.supplier`;

CREATE TABLE `_destination_table.nation`
AS 
SELECT * FROM `_source_table.nation`;

CREATE TABLE `_destination_table.region`
AS 
SELECT * FROM `_source_table.region`;
