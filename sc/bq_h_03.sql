CREATE TABLE `_destination_table.lineitem`
PARTITION BY l_shipdate
AS 
SELECT * FROM `_source_table.lineitem`;

CREATE TABLE `_destination_table.orders`
PARTITION BY o_orderdate
AS 
SELECT * FROM `_source_table.orders`;

CREATE TABLE `_destination_table.partsupp`
PARTITION BY RANGE_BUCKET(ps_availqty, GENERATE_ARRAY(0, 10000, 100))
AS 
SELECT * FROM `_source_table.partsupp`;

CREATE TABLE `_destination_table.part`
PARTITION BY RANGE_BUCKET(p_size, GENERATE_ARRAY(1, 50, 1))
AS 
SELECT * FROM `_source_table.part`;

CREATE TABLE `_destination_table.customer`
PARTITION BY RANGE_BUCKET(c_nationkey, GENERATE_ARRAY(0, 25, 5))
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
