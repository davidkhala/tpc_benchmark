-- $ID$
-- TPC-H/TPC-R Top Supplier Query (Q15)
-- Functional Query Definition
-- Approved February 1998

create view `49586899439487868_revenue_view` as
	select
		l_suppkey as supplier_no,
		sum(l_extendedprice * (1 - l_discount)) as total_revenue
	from
		`49586899439487868_lineitem`
	where
		l_shipdate >= cast(':1' as date)
		and l_shipdate < date_add(cast(':1' as date), interval '3' month)
	group by
		l_suppkey;

select
	s_suppkey,
	s_name,
	s_address,
	s_phone,
	total_revenue
from
	supplier,
	`49586899439487868_revenue_view`
where
	s_suppkey = supplier_no
	and total_revenue = (
		select
			max(total_revenue)
		from
			`49586899439487868_revenue_view`
	)
order by
	s_suppkey;

drop view `49586899439487868_revenue_view`;
