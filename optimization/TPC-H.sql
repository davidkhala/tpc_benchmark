Q1:
select
l_returnflag,
l_linestatus,
sum(l_quantity) as sum_qty,
sum(l_extendedprice) as sum_base_price,
sum(l_extendedprice*(1-l_discount)) as sum_disc_price,
sum(l_extendedprice*(1-l_discount)*(1+l_tax)) as sum_charge,
avg(l_quantity) as avg_qty,
avg(l_extendedprice) as avg_price,
avg(l_discount) as avg_disc,
count(*) as count_order
from
`h_2GB_1_basic.LINEITEM` 
where
l_shipdate <= date '1998-11-28'
group by
l_returnflag,
l_linestatus
order by
l_returnflag,
l_linestatus;

Q2:
select
s_acctbal,
s_name,
n_name,
p_partkey,
p_mfgr,
s_address,
s_phone,
s_comment
from
`h_2GB_1_basic.PARTSUPP` ,
`h_2GB_1_basic.PART`,
`h_2GB_1_basic.SUPPLIER` ,
`h_2GB_1_basic.NATION` ,
`h_2GB_1_basic.REGION` 
where
p_partkey = ps_partkey
and s_suppkey = ps_suppkey
and p_size = 25
and p_type like 'LARGE POLISHED COPPER'
and s_nationkey = n_nationkey
and n_regionkey = r_regionkey
and r_name = 'AMERICA'
and ps_supplycost = (
select min(ps_supplycost) from
`h_2GB_1_basic.PARTSUPP` ,
`h_2GB_1_basic.SUPPLIER` ,
`h_2GB_1_basic.NATION` ,
`h_2GB_1_basic.REGION` 
where
p_partkey = ps_partkey
and s_suppkey = ps_suppkey
and s_nationkey = n_nationkey
and n_regionkey = r_regionkey
and r_name = 'AMERICA'
)
order by
s_acctbal desc,
n_name,
s_name,
p_partkey;


Q9:
select
nation,
o_year,
sum(amount) as sum_profit
from (
select
n_name as nation,
extract(year from o_orderdate) as o_year,
l_extendedprice * (1 - l_discount) - ps_supplycost * l_quantity as amount
from
part,
supplier,
lineitem,
partsupp,
orders,
nation
where
s_suppkey = l_suppkey
and ps_suppkey = l_suppkey
and ps_partkey = l_partkey
and p_partkey = l_partkey
and o_orderkey = l_orderkey
and s_nationkey = n_nationkey
and p_name like '%snow metallic turquoise mint lime%'
) as profit
group by
nation,
o_year
order by
nation,
o_year desc;
