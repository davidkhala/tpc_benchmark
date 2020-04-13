bq query --destination_table=h_2GB_1_basic.SUPPLIER_O1 --use_legacy_sql=false --range_partitioning=s_suppkey,1,20000,200 'SELECT * FROM h_2GB_1_basic.SUPPLIER'
bq mk --table --schema partsupp_schema.json --range_partitioning=ps_partkey,1,400000,4000 --clustering_fields ps_suppkey h_2GB_1_basic.PARTSUPP_O1
bq mk --table --schema customer_schema.json --range_partitioning=c_custkey,1,300000,3000 --clustering_fields c_mktsegment h_2GB_1_basic.CUSTOMER_O1
bq mk --table --schema part_schema.json --range_partitioning=p_partkey,1,400000,4000 --clustering_fields p_size,p_brand,p_type,p_container h_2GB_1_basic.PART_O1
bq mk --table --schema orders_schema.json --time_partitioning_field o_orderdate --clustering_fields o_orderstatus,o_orderpriority h_2GB_1_basic.ORDERS_O1
bq mk --table --schema lineitem_schema.json --time_partitioning_field l_shipdate --clustering_fields l_orderkey,l_partkey,l_suppkey h_2GB_1_basic.LINEITEM_O1
