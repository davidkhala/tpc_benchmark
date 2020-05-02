-- sccsid:     @(#)dss.ddl	2.1.8.1
create table nation  ( n_nationkey  int64 not null,
                            n_name       string not null,
                            n_regionkey  int64 not null,
                            n_comment    string);

create table region  ( r_regionkey  int64 not null,
                            r_name       string not null,
                            r_comment    string);

create table part  ( p_partkey     int64 not null,
                          p_name        string not null,
                          p_mfgr        string not null,
                          p_brand       string not null,
                          p_type        string not null,
                          p_size        int64 not null,
                          p_container   string not null,
                          p_retailprice float64 not null,
                          p_comment     string not null );

create table supplier ( s_suppkey     int64 not null,
                             s_name        string not null,
                             s_address     string not null,
                             s_nationkey   int64 not null,
                             s_phone       string not null,
                             s_acctbal     float64 not null,
                             s_comment     string not null);

create table partsupp ( ps_partkey     int64 not null,
                             ps_suppkey     int64 not null,
                             ps_availqty    int64 not null,
                             ps_supplycost  float64  not null,
                             ps_comment     string not null );

create table customer ( c_custkey     int64 not null,
                             c_name        string not null,
                             c_address     string not null,
                             c_nationkey   int64 not null,
                             c_phone       string not null,
                             c_acctbal     float64   not null,
                             c_mktsegment  string not null,
                             c_comment     string not null);

create table orders  ( o_orderkey       int64 not null,
                           o_custkey        int64 not null,
                           o_orderstatus    string not null,
                           o_totalprice     float64 not null,
                           o_orderdate      date not null,
                           o_orderpriority  string not null,  
                           o_clerk          string not null, 
                           o_shippriority   int64 not null,
                           o_comment        string not null);

create table lineitem ( l_orderkey    int64 not null,
                             l_partkey     int64 not null,
                             l_suppkey     int64 not null,
                             l_linenumber  int64 not null,
                             l_quantity    float64 not null,
                             l_extendedprice  float64 not null,
                             l_discount    float64 not null,
                             l_tax         float64 not null,
                             l_returnflag  string not null,
                             l_linestatus  string not null,
                             l_shipdate    date not null,
                             l_commitdate  date not null,
                             l_receiptdate date not null,
                             l_shipinstruct string not null,
                             l_shipmode     string not null,
                             l_comment      string not null);

