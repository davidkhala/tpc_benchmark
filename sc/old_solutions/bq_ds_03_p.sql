-- DS test version 03 (currently identical to version 02)
--
-- 
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
partition by d_date
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
partition by i_rec_start_date
as
select * from `_source_dataset.item`;

create table `_destination_dataset.store`
partition by s_rec_start_date
as
select * from `_source_dataset.store`;

create table `_destination_dataset.call_center`
partition by cc_rec_start_date
as
select * from `_source_dataset.call_center`;

create table `_destination_dataset.customer`
as
select * from `_source_dataset.customer`;

create table `_destination_dataset.web_site`
partition by web_rec_start_date
as
select * from `_source_dataset.web_site`;

create table `_destination_dataset.store_returns`
as
select * from `_source_dataset.store_returns`;

create table `_destination_dataset.household_demographics`
as
select * from `_source_dataset.household_demographics`;

create table `_destination_dataset.web_page`
partition by wp_rec_start_date
as
select * from `_source_dataset.web_page`;

create table `_destination_dataset.promotion`
as
select * from `_source_dataset.promotion`;

create table `_destination_dataset.catalog_page`
as
select * from `_source_dataset.catalog_page`;

create table `_destination_dataset.inventory`
as
select * from `_source_dataset.inventory`;

create table `_destination_dataset.catalog_returns`
as
select * from `_source_dataset.catalog_returns`;

create table `_destination_dataset.web_returns`
as
select * from `_source_dataset.web_returns`;

create table `_destination_dataset.web_sales`
as
select * from `_source_dataset.web_sales`;

create table `_destination_dataset.catalog_sales`
as
select * from `_source_dataset.catalog_sales`;

create table `_destination_dataset.store_sales`
as
select * from `_source_dataset.store_sales`;
