-- $ID$
-- TPC-H/TPC-R Forecasting Revenue Change Query (Q6)
-- Functional Query Definition
-- Approved February 1998
:x
:o
select
	sum(l_extendedprice * l_discount) as revenue
from
	lineitem
where
	l_shipdate >= cast(':1' as date)
	and l_shipdate < date_add(cast(':1' as date), interval '1' year)
	and l_discount between :2 - 0.01 and :2 + 0.01
	and l_quantity < :3;
