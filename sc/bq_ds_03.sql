-- DS test version 03
-- cluster on time
-- 
create table `_destination_table.dbgen_version`
as
select * from `_source_table.dbgen_version`;

create table `_destination_table.customer_address`
as
select * from `_source_table.customer_address`

create table `_destination_table.customer_demographics`
as
select * from `_source_table.customer_demographics`

create table `_destination_table.date_dim`
partition by d_date
as
select * from `_source_table.date_dim`

create table `_destination_table.warehouse`
as
select * from `_source_table.warehouse`

create table `_destination_table.ship_mode`
as
select * from `_source_table.ship_mode`

create table `_destination_table.time_dim`
as
select * from `_source_table.time_dim`

create table `_destination_table.reason`
as
select * from `_source_table.reason`

create table `_destination_table.income_band`
as
select * from `_source_table.income_band`

create table `_destination_table.item`
partition by i_rec_start_date
as
select * from `_source_table.item`

create table `_destination_table.store`
partition by s_rec_start_date
as
select * from `_source_table.store`

create table `_destination_table.call_center`
partition by cc_rec_start_date
as
select * from `_source_table.call_center`

create table `_destination_table.customer`
as
select * from `_source_table.customer`

create table `_destination_table.web_site`
partition by web_rec_start_date
as
select * from `_source_table.web_site`

create table `_destination_table.store_returns`
as
select * from `_source_table.store_returns`

create table `_destination_table.household_demographics`
as
select * from `_source_table.household_demographics`

create table `_destination_table.web_page`
partition by wp_rec_start_date
as
select * from `_source_table.web_page`

create table `_destination_table.promotion`
as
select * from `_source_table.promotion`

create table `_destination_table.catalog_page`
as
select * from `_source_table.catalog_page`

create table `_destination_table.inventory`
as
select * from `_source_table.inventory`

create table `_destination_table.catalog_returns`
as
select * from `_source_table.catalog_returns`

create table `_destination_table.web_returns`
as
select * from `_source_table.web_returns`

create table `_destination_table.web_sales`
as
select * from `_source_table.web_sales`

create table `_destination_table.catalog_sales`
as
select * from `_source_table.catalog_sales`

create table `_destination_table.store_sales`
as
select * from `_source_table.store_sales`
