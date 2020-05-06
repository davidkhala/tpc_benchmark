  # 1
SELECT
  l_returnflag,
  l_linestatus,
  SUM(l_quantity) AS sum_qty,
  SUM(l_extendedprice) AS sum_base_price,
  SUM(l_extendedprice * (1 - l_discount)) AS sum_disc_price,
  SUM(l_extendedprice * (1 - l_discount) * (1 + l_tax)) AS sum_charge,
  AVG(l_quantity) AS avg_qty,
  AVG(l_extendedprice) AS avg_price,
  AVG(l_discount) AS avg_disc,
  COUNT(*) AS count_order
FROM
  h_100GB_01_PART_1.lineitem
WHERE
  l_shipdate <= DATE_SUB(CAST('1998-12-01' AS date), INTERVAL '103' day)
GROUP BY
  l_returnflag,
  l_linestatus
ORDER BY
  l_returnflag,
  l_linestatus;
  # 2
SELECT
  s_acctbal,
  s_name,
  n_name,
  p_partkey,
  p_mfgr,
  s_address,
  s_phone,
  s_comment
FROM
  h_100GB_01.partsupp,
  h_100GB_01.region,
  h_100GB_01.part,
  h_100GB_01.supplier,
  h_100GB_01.nation
WHERE
  p_partkey = ps_partkey
  AND s_suppkey = ps_suppkey
  AND p_size = 42
  AND p_type LIKE '%COPPER'
  AND s_nationkey = n_nationkey
  AND n_regionkey = r_regionkey
  AND r_name = 'MIDDLE EAST'
  AND ps_supplycost = (
  SELECT
    MIN(ps_supplycost)
  FROM
    h_100GB_01.partsupp,
    h_100GB_01.supplier,
    h_100GB_01.nation,
    h_100GB_01.region
  WHERE
    p_partkey = ps_partkey
    AND s_suppkey = ps_suppkey
    AND s_nationkey = n_nationkey
    AND n_regionkey = r_regionkey
    AND r_name = 'MIDDLE EAST' )
ORDER BY
  s_acctbal DESC,
  n_name,
  s_name,
  p_partkey
LIMIT
  100 ;

#3
SELECT
  l_orderkey,
  SUM(l_extendedprice * (1 - l_discount)) AS revenue,
  o_orderdate,
  o_shippriority
FROM
  h_100GB_01_PART_1.lineitem,
  h_100GB_01_PART_1.customer,
  h_100GB_01_PART_1.orders
WHERE
  c_mktsegment = 'MACHINERY'
  AND c_custkey = o_custkey
  AND l_orderkey = o_orderkey
  AND o_orderdate < date '1995-03-29'
  AND l_shipdate > date '1995-03-29'
GROUP BY
  l_orderkey,
  o_orderdate,
  o_shippriority
ORDER BY
  revenue DESC,
  o_orderdate
LIMIT
  10 ;

#4
SELECT
  o_orderpriority,
  COUNT(*) AS order_count
FROM
  h_100GB_01_PART_1.orders
WHERE
  o_orderdate >= date '1997-06-01'
  AND o_orderdate < DATE_ADD(CAST('1997-06-01' AS date), INTERVAL '3' month)
  AND EXISTS (
  SELECT
    *
  FROM
    h_100GB_01_PART_1.lineitem
  WHERE
    l_orderkey = o_orderkey
    AND l_commitdate < l_receiptdate )
GROUP BY
  o_orderpriority
ORDER BY
  o_orderpriority;

#5
SELECT
  n_name,
  SUM(l_extendedprice * (1 - l_discount)) AS revenue
FROM
  h_100GB_01_PART_1.lineitem,
  h_100GB_01.region,
  h_100GB_01_PART_1.orders,
  h_100GB_01.customer,
  h_100GB_01.supplier,
  h_100GB_01.nation
WHERE
  c_custkey = o_custkey
  AND l_orderkey = o_orderkey
  AND l_suppkey = s_suppkey
  AND c_nationkey = s_nationkey
  AND s_nationkey = n_nationkey
  AND n_regionkey = r_regionkey
  AND r_name = 'MIDDLE EAST'
  AND o_orderdate >= CAST('1996-01-01' AS date)
  AND o_orderdate < DATE_ADD(CAST('1996-01-01' AS date), INTERVAL '1' year)
GROUP BY
  n_name
ORDER BY
  revenue DESC;

#6
SELECT
  SUM(l_extendedprice * l_discount) AS revenue
FROM
  h_100GB_01_PART_1.lineitem
WHERE
  l_shipdate >= CAST('1996-01-01' AS date)
  AND l_shipdate < DATE_ADD(CAST('1996-01-01' AS date), INTERVAL '1' year)
  AND l_discount BETWEEN 0.08 - 0.01
  AND 0.08 + 0.01
  AND l_quantity < 25;

#7
SELECT
  supp_nation,
  cust_nation,
  l_year,
  SUM(volume) AS revenue
FROM (
  SELECT
    n1.n_name AS supp_nation,
    n2.n_name AS cust_nation,
    EXTRACT(year
    FROM
      l_shipdate) AS l_year,
    l_extendedprice * (1 - l_discount) AS volume
  FROM
    h_100GB_01_PART_1.lineitem,
    h_100GB_01.nation n1,
    h_100GB_01_PART_1.orders,
    h_100GB_01.customer,
    h_100GB_01.supplier,
    h_100GB_01.nation n2
  WHERE
    s_suppkey = l_suppkey
    AND o_orderkey = l_orderkey
    AND c_custkey = o_custkey
    AND s_nationkey = n1.n_nationkey
    AND c_nationkey = n2.n_nationkey
    AND ( (n1.n_name = 'SAUDI ARABIA'
        AND n2.n_name = 'CANADA')
      OR (n1.n_name = 'CANADA'
        AND n2.n_name = 'SAUDI ARABIA') )
    AND l_shipdate BETWEEN CAST('1995-01-01' AS date)
    AND CAST('1996-12-31' AS date) ) AS shipping
GROUP BY
  supp_nation,
  cust_nation,
  l_year
ORDER BY
  supp_nation,
  cust_nation,
  l_year;


#8
SELECT
  o_year,
  SUM(CASE
      WHEN nation = 'SAUDI ARABIA' THEN volume
    ELSE
    0
  END
    ) / SUM(volume) AS mkt_share
FROM (
  SELECT
    EXTRACT(year
    FROM
      o_orderdate) AS o_year,
    l_extendedprice * (1 - l_discount) AS volume,
    n2.n_name AS nation
  FROM
    h_100GB_01_PART_1.lineitem,
    h_100GB_01.nation n1,
    h_100GB_01_PART_1.orders,
    h_100GB_01.part,
    h_100GB_01.customer,
    h_100GB_01.supplier,
    h_100GB_01.nation n2,
    h_100GB_01.region
  WHERE
    p_partkey = l_partkey
    AND s_suppkey = l_suppkey
    AND l_orderkey = o_orderkey
    AND o_custkey = c_custkey
    AND c_nationkey = n1.n_nationkey
    AND n1.n_regionkey = r_regionkey
    AND r_name = 'MIDDLE EAST'
    AND s_nationkey = n2.n_nationkey
    AND o_orderdate BETWEEN CAST('1995-01-01' AS date)
    AND CAST('1996-12-31' AS date)
    AND p_type = 'STANDARD BRUSHED BRASS' ) AS all_nations
GROUP BY
  o_year
ORDER BY
  o_year;

#9
SELECT
  nation,
  o_year,
  SUM(amount) AS sum_profit
FROM (
  SELECT
    n_name AS nation,
    EXTRACT(year
    FROM
      o_orderdate) AS o_year,
    l_extendedprice * (1 - l_discount) - ps_supplycost * l_quantity AS amount
  FROM
    h_100GB_01_PART_1.lineitem,
    h_100GB_01.nation,
    h_100GB_01_PART_1.orders,
    h_100GB_01.partsupp,
    h_100GB_01.part,
    h_100GB_01.supplier
  WHERE
    s_suppkey = l_suppkey
    AND ps_suppkey = l_suppkey
    AND ps_partkey = l_partkey
    AND p_partkey = l_partkey
    AND o_orderkey = l_orderkey
    AND s_nationkey = n_nationkey
    AND p_name LIKE '%chartreuse%' ) AS profit
GROUP BY
  nation,
  o_year
ORDER BY
  nation,
  o_year DESC;

#10
SELECT
  c_custkey,
  c_name,
  SUM(l_extendedprice * (1 - l_discount)) AS revenue,
  c_acctbal,
  n_name,
  c_address,
  c_phone,
  c_comment
FROM
  h_100GB_01_PART_1.lineitem,
  h_100GB_01.nation,
  h_100GB_01_PART_1.orders,
  h_100GB_01.customer
WHERE
  c_custkey = o_custkey
  AND l_orderkey = o_orderkey
  AND o_orderdate >= CAST('1995-01-01' AS date)
  AND o_orderdate < DATE_ADD(CAST('1995-01-01' AS date), INTERVAL '3' month)
  AND l_returnflag = 'R'
  AND c_nationkey = n_nationkey
GROUP BY
  c_custkey,
  c_name,
  c_acctbal,
  c_phone,
  n_name,
  c_address,
  c_comment
ORDER BY
  revenue DESC
LIMIT
  20 ;

#11
SELECT
  ps_partkey,
  SUM(ps_supplycost * ps_availqty) AS value
FROM
  h_100GB_01.partsupp,
  h_100GB_01.supplier,
  h_100GB_01.nation
WHERE
  ps_suppkey = s_suppkey
  AND s_nationkey = n_nationkey
  AND n_name = 'CHINA'
GROUP BY
  ps_partkey
HAVING
  SUM(ps_supplycost * ps_availqty) > (
  SELECT
    SUM(ps_supplycost * ps_availqty) * 0.0000010000
  FROM
    h_100GB_01.partsupp,
    h_100GB_01.supplier,
    h_100GB_01.nation
  WHERE
    ps_suppkey = s_suppkey
    AND s_nationkey = n_nationkey
    AND n_name = 'CHINA' )
ORDER BY
  value DESC;

#12
SELECT
  l_shipmode,
  SUM(CASE
      WHEN o_orderpriority = '1-URGENT' OR o_orderpriority = '2-HIGH' THEN 1
    ELSE
    0
  END
    ) AS high_line_count,
  SUM(CASE
      WHEN o_orderpriority <> '1-URGENT' AND o_orderpriority <> '2-HIGH' THEN 1
    ELSE
    0
  END
    ) AS low_line_count
FROM
  h_100GB_01_PART_1.orders,
  h_100GB_01_PART_1.lineitem
WHERE
  o_orderkey = l_orderkey
  AND l_shipmode IN ('FOB',
    'TRUCK')
  AND l_commitdate < l_receiptdate
  AND l_shipdate < l_commitdate
  AND l_receiptdate >= CAST('1996-01-01' AS date)
  AND l_receiptdate < DATE_ADD(CAST('1996-01-01' AS date), INTERVAL '1' year)
GROUP BY
  l_shipmode
ORDER BY
  l_shipmode;

#13
SELECT
  c_count,
  COUNT(*) AS custdist
FROM (
  SELECT
    c_custkey,
    COUNT(o_orderkey) AS c_count
  FROM
    h_100GB_01.customer
  LEFT OUTER JOIN
    h_100GB_01_PART_1.orders
  ON
    c_custkey = o_custkey
    AND o_comment NOT LIKE '%pending%accounts%'
  GROUP BY
    c_custkey ) AS c_orders
GROUP BY
  c_count
ORDER BY
  custdist DESC,
  c_count DESC;

#14
SELECT
  100.00 * SUM(CASE
      WHEN p_type LIKE 'PROMO%' THEN l_extendedprice * (1 - l_discount)
    ELSE
    0
  END
    ) / SUM(l_extendedprice * (1 - l_discount)) AS promo_revenue
FROM
  h_100GB_01_PART_1.lineitem,
  h_100GB_01.part
WHERE
  l_partkey = p_partkey
  AND l_shipdate >= CAST('1996-06-01' AS date)
  AND l_shipdate < DATE_ADD(CAST('1996-06-01' AS date), INTERVAL '1' month);

#15
CREATE VIEW
  `h_100GB_01_PART_1.49586899439487868_revenue_view` AS
SELECT
  l_suppkey AS supplier_no,
  SUM(l_extendedprice * (1 - l_discount)) AS total_revenue
FROM
  `tpc-benchmarking-9432.h_100GB_01_PART_1.lineitem`
WHERE
  l_shipdate >= CAST('1997-04-01' AS date)
  AND l_shipdate < DATE_ADD(CAST('1997-04-01' AS date), INTERVAL '3' month)
GROUP BY
  l_suppkey;

SELECT
  s_suppkey,
  s_name,
  s_address,
  s_phone,
  total_revenue
FROM
  h_100GB_01.supplier,
  `h_100GB_01_PART_1.49586899439487868_revenue_view`
WHERE
  s_suppkey = supplier_no
  AND total_revenue = (
  SELECT
    MAX(total_revenue)
  FROM
    `h_100GB_01_PART_1.49586899439487868_revenue_view` )
ORDER BY
  s_suppkey;
DROP VIEW
  `h_100GB_01_PART_1.49586899439487868_revenue_view`;

#16
SELECT
  p_brand,
  p_type,
  p_size,
  COUNT(DISTINCT ps_suppkey) AS supplier_cnt
FROM
  h_100GB_01.partsupp,
  h_100GB_01.part
WHERE
  p_partkey = ps_partkey
  AND p_brand <> 'Brand#41'
  AND p_type NOT LIKE 'PROMO BRUSHED%'
  AND p_size IN (44,
    22,
    47,
    34,
    9,
    6,
    1,
    16)
  AND ps_suppkey NOT IN (
  SELECT
    s_suppkey
  FROM
    h_100GB_01.supplier
  WHERE
    s_comment LIKE '%Customer%Complaints%' )
GROUP BY
  p_brand,
  p_type,
  p_size
ORDER BY
  supplier_cnt DESC,
  p_brand,
  p_type,
  p_size;

#17
SELECT
  SUM(l_extendedprice) / 7.0 AS avg_yearly
FROM
  h_100GB_01_PART_1.lineitem,
  h_100GB_01.part
WHERE
  p_partkey = l_partkey
  AND p_brand = 'Brand#15'
  AND p_container = 'WRAP BAG'
  AND l_quantity < (
  SELECT
    0.2 * AVG(l_quantity)
  FROM
    h_100GB_01_PART_1.lineitem
  WHERE
    l_partkey = p_partkey );

#18
SELECT
  c_name,
  c_custkey,
  o_orderkey,
  o_orderdate,
  o_totalprice,
  SUM(l_quantity)
FROM
  h_100GB_01_PART_1.lineitem,
  h_100GB_01_PART_1.orders,
  h_100GB_01.customer
WHERE
  o_orderkey IN (
  SELECT
    l_orderkey
  FROM
    h_100GB_01_PART_1.lineitem
  GROUP BY
    l_orderkey
  HAVING
    SUM(l_quantity) > 315 )
  AND c_custkey = o_custkey
  AND o_orderkey = l_orderkey
GROUP BY
  c_name,
  c_custkey,
  o_orderkey,
  o_orderdate,
  o_totalprice
ORDER BY
  o_totalprice DESC,
  o_orderdate
LIMIT
  100 ;

#19
SELECT
  SUM(l_extendedprice* (1 - l_discount)) AS revenue
FROM
  h_100GB_01_PART_1.lineitem,
  h_100GB_01.part
WHERE
  ( p_partkey = l_partkey
    AND p_brand = 'Brand#53'
    AND p_container IN ('SM CASE',
      'SM BOX',
      'SM PACK',
      'SM PKG')
    AND l_quantity >= 9
    AND l_quantity <= 9 + 10
    AND p_size BETWEEN 1
    AND 5
    AND l_shipmode IN ('AIR',
      'AIR REG')
    AND l_shipinstruct = 'DELIVER IN PERSON' )
  OR ( p_partkey = l_partkey
    AND p_brand = 'Brand#54'
    AND p_container IN ('MED BAG',
      'MED BOX',
      'MED PKG',
      'MED PACK')
    AND l_quantity >= 12
    AND l_quantity <= 12 + 10
    AND p_size BETWEEN 1
    AND 10
    AND l_shipmode IN ('AIR',
      'AIR REG')
    AND l_shipinstruct = 'DELIVER IN PERSON' )
  OR ( p_partkey = l_partkey
    AND p_brand = 'Brand#11'
    AND p_container IN ('LG CASE',
      'LG BOX',
      'LG PACK',
      'LG PKG')
    AND l_quantity >= 25
    AND l_quantity <= 25 + 10
    AND p_size BETWEEN 1
    AND 15
    AND l_shipmode IN ('AIR',
      'AIR REG')
    AND l_shipinstruct = 'DELIVER IN PERSON' );

#20
SELECT
  s_name,
  s_address
FROM
  h_100GB_01.supplier,
  h_100GB_01.nation
WHERE
  s_suppkey IN (
  SELECT
    ps_suppkey
  FROM
    h_100GB_01.partsupp
  WHERE
    ps_partkey IN (
    SELECT
      p_partkey
    FROM
      h_100GB_01.part
    WHERE
      p_name LIKE 'khaki%' )
    AND ps_availqty > (
    SELECT
      0.5 * SUM(l_quantity)
    FROM
      h_100GB_01_PART_1.lineitem
    WHERE
      l_partkey = ps_partkey
      AND l_suppkey = ps_suppkey
      AND l_shipdate >= CAST('1997-01-01' AS date)
      AND l_shipdate < DATE_ADD(CAST('1997-01-01' AS date), INTERVAL '1' year) ) )
  AND s_nationkey = n_nationkey
  AND n_name = 'MOZAMBIQUE'
ORDER BY
  s_name;

#21
SELECT
  s_name,
  COUNT(*) AS numwait
FROM
  h_100GB_01_PART_1.lineitem l1,
  h_100GB_01.nation,
  h_100GB_01_PART_1.orders,
  h_100GB_01.supplier
WHERE
  s_suppkey = l1.l_suppkey
  AND o_orderkey = l1.l_orderkey
  AND o_orderstatus = 'F'
  AND l1.l_receiptdate > l1.l_commitdate
  AND EXISTS (
  SELECT
    *
  FROM
    h_100GB_01_PART_1.lineitem l2
  WHERE
    l2.l_orderkey = l1.l_orderkey
    AND l2.l_suppkey <> l1.l_suppkey )
  AND NOT EXISTS (
  SELECT
    *
  FROM
    h_100GB_01_PART_1.lineitem l3
  WHERE
    l3.l_orderkey = l1.l_orderkey
    AND l3.l_suppkey <> l1.l_suppkey
    AND l3.l_receiptdate > l3.l_commitdate )
  AND s_nationkey = n_nationkey
  AND n_name = 'UNITED KINGDOM'
GROUP BY
  s_name
ORDER BY
  numwait DESC,
  s_name
LIMIT
  100 ;

#22
SELECT
  cntrycode,
  COUNT(*) AS numcust,
  SUM(c_acctbal) AS totacctbal
FROM (
  SELECT
    SUBSTR(c_phone, 0, 2) AS cntrycode,
    c_acctbal
  FROM
    h_100GB_01.customer
  WHERE
    SUBSTR(c_phone, 0, 2) IN ('26',
      '13',
      '12',
      '31',
      '18',
      '25',
      '23')
    AND c_acctbal > (
    SELECT
      AVG(c_acctbal)
    FROM
      h_100GB_01.customer
    WHERE
      c_acctbal > 0.00
      AND SUBSTR(c_phone, 0, 2) IN ('26',
        '13',
        '12',
        '31',
        '18',
        '25',
        '23') )
    AND NOT EXISTS (
    SELECT
      *
    FROM
      h_100GB_01_PART_1.orders
    WHERE
      o_custkey = c_custkey ) ) AS custsale
GROUP BY
  cntrycode
ORDER BY
  cntrycode;
