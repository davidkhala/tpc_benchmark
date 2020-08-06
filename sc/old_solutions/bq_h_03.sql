CREATE TABLE `_destination_dataset.lineitem`
PARTITION BY l_shipdate
AS 
SELECT * FROM `_source_dataset.lineitem`;

CREATE TABLE `_destination_dataset.orders`
PARTITION BY o_orderdate
AS 
SELECT * FROM `_source_dataset.orders`;

CREATE TABLE `_destination_dataset.partsupp`
PARTITION BY RANGE_BUCKET(ps_availqty, GENERATE_ARRAY(0, 10000, 100))
AS 
SELECT * FROM `_source_dataset.partsupp`;

CREATE TABLE `_destination_dataset.part`
PARTITION BY RANGE_BUCKET(p_size, GENERATE_ARRAY(1, 50, 1))
AS 
SELECT * FROM `_source_dataset.part`;

CREATE TABLE `_destination_dataset.customer`
PARTITION BY RANGE_BUCKET(c_nationkey, GENERATE_ARRAY(0, 25, 5))
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
