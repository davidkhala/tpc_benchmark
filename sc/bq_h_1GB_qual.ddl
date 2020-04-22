-- Sccsid:     @(#)dss.ddl	2.1.8.1
CREATE TABLE NATION  ( N_NATIONKEY  INT64 NOT NULL,
                            N_NAME       STRING NOT NULL,
                            N_REGIONKEY  INT64 NOT NULL,
                            N_COMMENT    STRING);

CREATE TABLE REGION  ( R_REGIONKEY  INT64 NOT NULL,
                            R_NAME       STRING NOT NULL,
                            R_COMMENT    STRING);

CREATE TABLE PART  ( P_PARTKEY     INT64 NOT NULL,
                          P_NAME        STRING NOT NULL,
                          P_MFGR        STRING NOT NULL,
                          P_BRAND       STRING NOT NULL,
                          P_TYPE        STRING NOT NULL,
                          P_SIZE        INT64 NOT NULL,
                          P_CONTAINER   STRING NOT NULL,
                          P_RETAILPRICE FLOAT64 NOT NULL,
                          P_COMMENT     STRING NOT NULL );

CREATE TABLE SUPPLIER ( S_SUPPKEY     INT64 NOT NULL,
                             S_NAME        STRING NOT NULL,
                             S_ADDRESS     STRING NOT NULL,
                             S_NATIONKEY   INT64 NOT NULL,
                             S_PHONE       STRING NOT NULL,
                             S_ACCTBAL     FLOAT64 NOT NULL,
                             S_COMMENT     STRING NOT NULL);

CREATE TABLE PARTSUPP ( PS_PARTKEY     INT64 NOT NULL,
                             PS_SUPPKEY     INT64 NOT NULL,
                             PS_AVAILQTY    INT64 NOT NULL,
                             PS_SUPPLYCOST  FLOAT64  NOT NULL,
                             PS_COMMENT     STRING NOT NULL );

CREATE TABLE CUSTOMER ( C_CUSTKEY     INT64 NOT NULL,
                             C_NAME        STRING NOT NULL,
                             C_ADDRESS     STRING NOT NULL,
                             C_NATIONKEY   INT64 NOT NULL,
                             C_PHONE       STRING NOT NULL,
                             C_ACCTBAL     FLOAT64   NOT NULL,
                             C_MKTSEGMENT  STRING NOT NULL,
                             C_COMMENT     STRING NOT NULL);

CREATE TABLE ORDERS  ( O_ORDERKEY       INT64 NOT NULL,
                           O_CUSTKEY        INT64 NOT NULL,
                           O_ORDERSTATUS    STRING NOT NULL,
                           O_TOTALPRICE     FLOAT64 NOT NULL,
                           O_ORDERDATE      DATE NOT NULL,
                           O_ORDERPRIORITY  STRING NOT NULL,  
                           O_CLERK          STRING NOT NULL, 
                           O_SHIPPRIORITY   INT64 NOT NULL,
                           O_COMMENT        STRING NOT NULL);

CREATE TABLE LINEITEM ( L_ORDERKEY    INT64 NOT NULL,
                             L_PARTKEY     INT64 NOT NULL,
                             L_SUPPKEY     INT64 NOT NULL,
                             L_LINENUMBER  INT64 NOT NULL,
                             L_QUANTITY    FLOAT64 NOT NULL,
                             L_EXTENDEDPRICE  FLOAT64 NOT NULL,
                             L_DISCOUNT    FLOAT64 NOT NULL,
                             L_TAX         FLOAT64 NOT NULL,
                             L_RETURNFLAG  STRING NOT NULL,
                             L_LINESTATUS  STRING NOT NULL,
                             L_SHIPDATE    DATE NOT NULL,
                             L_COMMITDATE  DATE NOT NULL,
                             L_RECEIPTDATE DATE NOT NULL,
                             L_SHIPINSTRUCT STRING NOT NULL,
                             L_SHIPMODE     STRING NOT NULL,
                             L_COMMENT      STRING NOT NULL);

