"""Prepare TPC-DS tests for Snowflake

...

| TPC-DS ANSI SQL | Snowflake    |
| --------------- | ------------ |
| decimal         | DECIMAL      |
| integer         | INTEGER      |
| char(N)         | CHAR(N)      |
| varchar(N)      | VARCHAR(N)   |
| time            | TIME         |
| date            | DATE         |

See
https://docs.snowflake.com/en/sql-reference/intro-summary-data-types.html
for Snowflake datatype specifications in standard SQL

"""
import snowflake.connector
import atexit

import pandas as pd

import config, poor_security, gcp_storage, tools
import h_setup, ds_setup


def calc_cost(running_time):
    """ estimates cost for query runtime based on warehouse size """
    return running_time * config.sf_warehouse_cost


def brute_force_clean_query(query_text):
    query_text = query_text.replace('set rowcount', 'LIMIT').strip()
    query_text = query_text.replace('top 100;', 'LIMIT 100;').strip()
    query_text = query_text.replace('\n top 100', '\n LIMIT 100').strip()

    if query_text.endswith('go'):
        query_text = query_text[:len(query_text) - 2]

    return query_text


def parse_query_job(query_result, verbose=False):
    """

    Parameters
    ----------
    query_result : results from snowflake
    verbose : bool, print results

    Returns
    -------
    t0 : datetime object, time query started
    t1 : datetime object, time query ended
    bytes_processed : int, bytes processed with query
    bytes_billed : int, bytes billed for query
    df : Pandas DataFrame containing results of query
    """
    t0, t1, bytes_processed, row_count, cost, data = query_result
    df = pd.DataFrame(data)

    if verbose:
        print("Query Statistics")
        print("================")
        print("Total Billed Time: {}".format(t1-t0))
        print("Bytes Processed: {}".format(bytes_processed))
        print("Rows Processed: {}".format(row_count))
        print("Cost: {}".format(cost))
        print()
        if len(df) < 25:
            print("Result:")
            print("=======")
            print(df)
        else:
            print("Head of Result:")
            print("===============")
            print(df.head())

    return t0, t1, bytes_processed, row_count, cost, df


class Warehouse:
    def __init__(self):
        self.size = "XSMALL"
        self.size_options = ["XSMALL", "SMALL", "MEDIUM", "LARGE", "XLARGE", "XXLARGE", "XXXLARGE", "X4LARGE"]
        self.max_cluster_count = 1
        self.min_cluster_count = 1
        self.scaling_policy = "STANDARD"
        self.scaling_policy_options = ["STANDARD", "ECONOMY"]
        self.auto_suspend = True
        self.initially_suspended = True
        self.resource_monitor = None
        self.comment = "NA"


class Connector:
    def __init__(self, verbose=False, verbose_query=False):
        """"Snowflake Connector wrapper class"""
        self.verbose = verbose              # debug output for whole class
        self.verbose_query = verbose_query  # print query text
        self.verbose_query_n = False        # print line numbers in query text
        self.cached = False                 # user cache control for queries
        self.conn = None                    # Connector class connection
        self.cursor = None                  # Connector cursor
        self.dry_run = False                # execute query in dry run mode

        atexit.register(self.close)         # if class closes, close connection

    def close(self):
        """ Closes connection to Snowflake server"""
        self.conn.close()

    def connect(self, account, username, password, verbose=False):
        """Initializes a network connection to Snowflake using
        values saved in config.py and poor_security.py
        """

        if verbose:
            print("Snowflake configuration")
            print("=======================")
            print(f'Username: {username}')
            print(f'Account:  {account}')
            print()

        self.conn = snowflake.connector.connect(user=username,
                                                password=password,
                                                account=account
                                                )
        self.cursor = self.conn.cursor()

    def query(self, query_text, verbose=False):
        """Opens cursor, runs query and returns all results at once

        Parameters
        ----------
        query_text : str, query to execute
        verbose : bool, print debug statements

        Returns
        -------
        query_result : Snowflake connector cursor object result
        """

        assert self.conn is not None, "Connection not initialized"

        if self.dry_run:
            return
        else:
            if self.verbose_query:
                if self.verbose_query_n:
                    qt = "\n".join([str(n) + "  " + line for n, line in enumerate(query_text.split("\n"))])
                else:
                    qt = query_text
                print("SNOWFLAKE QUERY TEXT")
                print("====================")
                print(qt)
                print()
            query_result = self.cursor.execute(query_text)
            return query_result

    def cache_on(self):
        self.query("ALTER SESSION SET USE_CACHED_RESULT=true")

    def cache_off(self):
        self.query("ALTER SESSION SET USE_CACHED_RESULT=false")

    def role_select(self, role, verbose=False):
        self.query(f'USE ROLE {role}', verbose=verbose)

    def set_query_tag(self, tag_text):
        self.query(f"ALTER SESSION SET QUERY_TAG = '{tag_text}'")

    def set_timezone(self, timezone_code):
        self.query(f"ALTER SESSION SET TIMEZONE = '{timezone_code}'")

    def show_warehouses(self):
        return self.query("SHOW WAREHOUSES")

    def warehouse_create(self, name, verbose=False):
        return self.query(f"CREATE WAREHOUSE IF NOT EXISTS {name}", verbose=verbose)

    def warehouse_resume(self, name=None, verbose=False):
        """Starts a warehouse"""
        self.query(f'ALTER WAREHOUSE {name} RESUME;', verbose=verbose)

    def warehouse_use(self, name, verbose=False):
        self.query(f'USE WAREHOUSE {name}', verbose=verbose)

    def warehouse_suspend(self, name, verbose=False):
        """ suspends warehouse and closes connection """
        self.query(f'ALTER WAREHOUSE {name} SUSPEND;', verbose=verbose)

    def database_create(self, db, verbose=False):
        self.query(f'CREATE DATABASE IF NOT EXISTS {db}', verbose=verbose)

    def database_use(self, db, verbose=False):
        self.query(f'USE DATABASE {db}', verbose=verbose)

    def database_drop(self, name, verbose=False):
        """ Drop a database from the current warehouse"""
        self.query(f"DROP DATABASE IF EXISTS {name} CASCADE", verbose=verbose)

    def schema_use(self, name, verbose=False):
        """Use a specific schema in a database"""
        self.query(f"USE SCHEMA {name}", verbose=verbose)

    def create_named_file_format(self, named_ff, verbose=False):
        """Create named file format"""
        query_text = f"""create or replace file format {named_ff}
                     type = csv
                     field_delimiter = '|'
                     skip_header = 0
                     null_if = ('NULL', 'null')
                     empty_field_as_null = true
                     encoding = 'iso-8859-1' 
                     compression = none;"""

        self.query(query_text, verbose=verbose)

    def create_gcs_integration(self, st_int_name, bucket, verbose=False):
        """Create GCS integration"""
        uri = "gcs://" + bucket
        query_text = (f"CREATE STORAGE INTEGRATION {st_int_name} " +
                      "TYPE=EXTERNAL_STAGE " +
                      "STORAGE_PROVIDER=GCS " +
                      "ENABLED=TRUE " +
                      f"STORAGE_ALLOWED_LOCATIONS=('{uri}/');")
        self.query(query_text, verbose=verbose)

    def grant_storage_integration_access(self, role, name, verbose=False):
        """ grant access to STORAGE INTEGRATION and STAGE creation"""

        query_text = f'GRANT CREATE STAGE on schema public to ROLE {role};'
        self.query(query_text)

        query_text = f'GRANT USAGE on INTEGRATION {name} to ROLE {role};'
        self.query(query_text, verbose=verbose)

    def create_gcs_stage(self, st_int_name, bucket, named_ff, verbose=False):
        """ creates STAGE for each file needed during import """
        uri = "gcs://" + bucket
        query_text = (f"CREATE STAGE {st_int_name}_stage URL='{uri}' " +
                      f"STORAGE_INTEGRATION={st_int_name} " +
                      f"FILE_FORMAT={named_ff};")
        self.query(query_text, verbose=verbose)

    def list_stage(self, st_int_name, verbose=False):
        """ list items in "stage" """
        query_text = f'list  @{st_int_name}_stage;'
        self.query(query_text, verbose=verbose)

    def import_data(self, table, gcs_file_path, storage_integration, verbose=False):
        """ run import """
        query_text = (f"copy into {table} from '{gcs_file_path}' " +
                      f"storage_integration={storage_integration} " +
                      "file_format=(format_name=csv_file_format);")
        result = self.query(query_text, verbose=verbose)
        return result


class AU:
    def __init__(self, warehouse):
        """Connect to the snowflake.account_usage context

        Note: requires import access:
        grant imported privileges on database snowflake to role ...;

        Parameters
        ----------
        warehouse : str, what Snowflake warehouse to run the queries on
        """

        self.role_str = None  # defaults to SYSADMIN
        self.warehouse = warehouse
        self.database = "snowflake"
        self.schema = "account_usage"

        self.verbose = False
        self.verbose_query = False

        self.sfc = Connector(verbose_query=self.verbose_query,
                             verbose=self.verbose)

    def connect(self):
        """Initializes a network connection to Snowflake using
        values saved in config.py and poor_security.py
        """

        self.sfc.connect(username=poor_security.sf_username,
                         password=poor_security.sf_password,
                         account=config.sf_account,
                         verbose=self.verbose)
        self.sfc.set_timezone("UTC")
        self.sfc.set_query_tag("read_query_history")
        self.sfc.cache_off()
        self.sfc.warehouse_use(self.warehouse, verbose=self.verbose)
        self.sfc.database_use(self.database, verbose=self.verbose)
        self.sfc.schema_use("account_usage")

    def close(self):
        self.sfc.close()

    def cache_set(self, state="off"):
        """Set Snowflake user cache, API defaults to True, here we default to False

        Parameters
        ----------
        state : str, 'on' = cache on, anything else = cache off
        """
        if state == "on":
            self.sfc.cache_on()
        else:
            self.sfc.cache_off()

    @staticmethod
    def parse_query_result(query_result):
        """
        Parameters
        ----------
        query_result : Snowflake connector cursor object result

        Returns
        -------
        df_result : Pandas DataFrame containing results of query
        qid : str, query id - unique id of query on Snowflake platform
        """
        df_result = query_result.fetch_pandas_all()
        qid = query_result.sfqid
        return df_result, qid

    def query_history_view(self, t0, t1):
        """Get the time bound query history for all queries run on this account using the
        `snowflake.account_usage` database

        Note: Activity in the last 1 year is available, latency is 45 minutes.
        https://docs.snowflake.com/en/sql-reference/account-usage.html
        https://docs.snowflake.com/en/sql-reference/account-usage/query_history.html

        Parameters
        ----------
        Both parameters can be either datetime, pd.Timestamp, or str objects
        that can be parsed by pd.to_datetime
        t0 : start time
        t1 : end time
        """
        t0 = pd.to_datetime(t0)
        t1 = pd.to_datetime(t1)
        t0 = t0.strftime("%Y-%m-%d %H:%M:%S")
        t1 = t1.strftime("%Y-%m-%d %H:%M:%S")

        query_text = ("select * " +
                      "from query_history " +
                      f"where start_time>=to_timestamp_ltz('{t0}')"  #+
                      #f"end_time_range_end=>to_timestamp_ltz('{t1}')));"
                      )

        query_result = self.sfc.query(query_text)
        df_result, qid = self.parse_query_result(query_result)
        return df_result, qid


class SFTPC:
    def __init__(self, test, scale, cid, warehouse, desc="",
                 timestamp=None, verbose=False, verbose_query=False):
        """Snowflake Connector query class

        Parameters
        ----------
        test : str, TPC test being executed, either "ds" or "h"
        scale : int, database scale factor (i.e. 1, 100, 1000 etc)
        cid : str, config identifier, i.e. "01" or "03A"
        desc : str, description of current tdata collection effort
        warehouse : str, what Snowflake warehouse to run the queries on
        timestamp : Pandas Timestamp object, optional
        verbose : bool, print debug statements
        verbose_query : bool, print query text
        """

        self.test = test
        self.scale = scale
        self.cid = cid

        self.warehouse = warehouse

        self.role_str = None
        self.scale_str = str(scale)+"GB"

        self.database = f"{self.test}_{self.scale_str}_{self.cid}"
        self.desc = desc

        self.storage_integration_name = self.database + "_gcs_integration"

        self.df_gcs_full = None  # all files in bucket, as fyi
        self.df_gcs = None       # just files for this dataset

        self.verbose = verbose
        self.verbose_query = verbose_query
        self.verbose_query_n = False  # print line numbers in query text
        self.verbose_iter = False

        self.sfc = Connector(verbose_query=self.verbose_query,
                             verbose=self.verbose)

        self.q_label_base = self.database + "-xx-" + self.desc
        self.q_label_base = self.q_label_base.lower()

        self.cache = False

        self.timestamp = timestamp
        self.results_dir, _ = tools.make_name(db="sf", test=self.test, cid=self.cid,
                                              kind="results", datasource=self.database,
                                              desc=self.desc, ext="", timestamp=self.timestamp)
        self.results_csv_fp = None

    def _connect(self):
        self.sfc.connect(username=poor_security.sf_username,
                         password=poor_security.sf_password,
                         account=config.sf_account,
                         verbose=self.verbose)
        self.sfc.verbose_query_n = self.verbose_query_n

    def connect(self):
        """Initializes a network connection to Snowflake using
        values saved in config.py and poor_security.py
        """

        self._connect()
        self.sfc.set_timezone("UTC")
        self.set_query_label(self.q_label_base)
        self.cache_set("off")
        self.warehouse_use()
        self.database_use()

    def close(self):
        self.sfc.close()

    def cache_set(self, state="off"):
        """Set Snowflake user cache, API defaults to True, here we default to False

        Parameters
        ----------
        state : str, 'on' = cache on, anything else = cache off
        """
        if state == "on":
            self.sfc.cache_on()
        else:
            self.sfc.cache_off()

    def role(self, role):
        self.role_str = role
        self.sfc.role_select(role, verbose=self.verbose)

    def set_query_label(self, query_label):
        self.sfc.set_query_tag(query_label)

    def show_warehouses(self):
        return self.sfc.show_warehouses()

    def warehouse_create(self):
        self.sfc.warehouse_create(self.warehouse, verbose=self.verbose)

    def warehouse_resume(self):
        self.sfc.warehouse_resume(self.warehouse, verbose=self.verbose)

    def warehouse_use(self):
        self.sfc.warehouse_use(self.warehouse, verbose=self.verbose)

    def warehouse_suspend(self):
        self.sfc.warehouse_suspend(self.warehouse, verbose=self.verbose)

    def database_create(self):
        self.sfc.database_create(self.database, verbose=self.verbose)

    def database_setup_role(self, role):
        query_text = (f"grant select on all tables in schema {self.database}.public " +
                      f"to role {role};")
        self.sfc.query(query_text, verbose=self.verbose)

    def database_use(self):
        self.sfc.database_use(self.database, verbose=self.verbose)

    def database_drop(self):
        self.sfc.database_drop(self.database, verbose=self.verbose)

    def create_schema(self, schema_file):
        """Apply the schema .sql file as reformatted from
        config.tpcds_schema_ansi_sql_filepath
        to
        config.tpcds_schema_bq_filepath

        Parameters
        ----------
        schema_file : str, path to file containing DDL or sql schema query definitions
        """
        with open(schema_file, 'r') as f:
            schema_text = f.read()
        query_list = [qt.strip() + ";" for qt in schema_text.split(";") if len(qt.strip()) > 0]
        for query_text in query_list:
            self.sfc.query(query_text, verbose=self.verbose)

    def create_named_file_format(self):
        """Create named file format"""
        query_text = f"""create or replace file format {config.sf_named_file_format}
                     type = csv
                     field_delimiter = '|'
                     skip_header = 0
                     null_if = ('NULL', 'null')
                     empty_field_as_null = true
                     encoding = 'iso-8859-1' 
                     compression = none;"""

        self.sfc.query(query_text, verbose=self.verbose)

    def gcs_integration_create(self):
        """Create GCS storage integration"""
        uri = "gcs://" + config.gcs_data_bucket
        query_text = (f"CREATE STORAGE INTEGRATION {self.storage_integration_name} " +
                      "TYPE=EXTERNAL_STAGE " +
                      "STORAGE_PROVIDER=GCS " +
                      "ENABLED=TRUE " +
                      f"STORAGE_ALLOWED_LOCATIONS=('{uri}/');")
        self.sfc.query(query_text, verbose=self.verbose)

    def gcs_integration_list(self):
        """ lists all files in GCS bucket """
        return self.sfc.query(f"show integrations like 'gcs%';")

    def gcs_integration_drop(self):
        query_text = f"DROP INTEGRATION IF EXISTS {self.storage_integration_name}"
        self.sfc.query(query_text, verbose=self.verbose)

    def grant_storage_integration_access(self):
        """ grant access to STORAGE INTEGRATION and STAGE creation
        Note: for SYSADMIN level account to have access, run
        GRANT CREATE INTEGRATION ON ACCOUNT TO SYSADMIN
        as ACCOUNTADMIN
        """

        query_text = (f"GRANT CREATE STAGE on schema public " +
                      f"to ROLE {self.role_str};")
        self.sfc.query(query_text, verbose=self.verbose)

        query_text = (f"GRANT USAGE on INTEGRATION {self.storage_integration_name} " +
                      f"to ROLE {self.role_str};")
        self.sfc.query(query_text, verbose=self.verbose)

    def create_stage(self):
        """ creates STAGE for each file needed during import
        """
        uri = "gcs://" + config.gcs_data_bucket
        query = (f"CREATE STAGE {self.storage_integration_name}_stage URL='{uri}' " +
                 f"STORAGE_INTEGRATION={self.storage_integration_name} " +
                 f"FILE_FORMAT={config.sf_named_file_format};")

        self.sfc.query(query, verbose=self.verbose)

    def list_stage(self, st_int_name):
        """ list items in "stage" """
        query_text = f'list  @{st_int_name}_stage;'
        self.sfc.query(query_text)

    def gcs_inventory(self):
        """Inventory files in GCS that match this class' test and scale"""
        self.df_gcs_full = gcp_storage.inventory_bucket_df(config.gcs_data_bucket)
        self.df_gcs_full.sort_values(by=["test", "scale", "table", "n"], inplace=True)
        self.df_gcs = self.df_gcs_full.loc[(self.df_gcs_full.test == self.test) &
                                           (self.df_gcs_full.scale == self.scale_str)].copy()
        self.df_gcs.uri = self.df_gcs.uri.str.replace("gs:", "gcs:")

    def import_data_apply(self, table, gcs_file_path):
        """ run import """
        query_text = (f"copy into {table} from '{gcs_file_path}' " +
                      f"storage_integration={self.storage_integration_name} " +
                      "file_format=(format_name=csv_file_format);")

        self.sfc.query(query_text)

    def import_data(self):
        for table in self.df_gcs.table.unique():
            if table in config.ignore_tables:
                continue
            _df = self.df_gcs.loc[self.df_gcs.table == table]
            if self.verbose:
                print("Loading table:", table)
            # this could be turned into a pooled apply
            _df.apply(lambda r: self.import_data_apply(table=r.table,
                                                       gcs_file_path=r.uri),
                      axis=1)
            if self.verbose:
                print("Done!")
                print()

    @staticmethod
    def parse_query_result(query_result):
        """
        Parameters
        ----------
        query_result : Snowflake connector cursor object result

        Returns
        -------
        df_result : Pandas DataFrame containing results of query
        qid : str, query id - unique id of query on Snowflake platform
        """
        df_result = query_result.fetch_pandas_all()
        qid = query_result.sfqid
        return df_result, qid

    def query_n(self, n, qual=None, std_out=False):

        """Query Snowflake with a specific nth query

        Parameters
        ----------
        n : int, query number to execute
        qual : None, or True to use qualifying values (to test 1GB qualification db)
        std_out : bool, print std_out and std_err output

        Returns
        -------
        t0 : datetime object, time query started
        t1 : datetime object, time query ended
        query_result : Snowflake connector cursor object result
        query_text : str, query text generated for query
        """
        tpl_dir = f"{config.fp_query_templates}{config.sep}{'sf'}_{self.test}"

        if self.test == "ds":
            query_text = ds_setup.qgen_template(n=n,
                                                templates_dir=tpl_dir,
                                                dialect="sqlserver_tpc",
                                                scale=self.scale,
                                                qual=qual,
                                                verbose=self.verbose,
                                                verbose_std_out=std_out)
        elif self.test == "h":
            query_text = h_setup.qgen_template(n=n,
                                               templates_dir=tpl_dir,
                                               scale=self.scale,
                                               qual=qual,
                                               verbose=self.verbose,
                                               verbose_std_out=std_out)
        else:
            return None
        t0 = pd.Timestamp.now("UTC")

        # Snowflake doesn't process multiple queries in one query statement,
        # additionally another query command will wipe out the previous query_result
        # data.
        # Also, TPC-DS is completely single-queries, TPC-H has a view created and
        # deleted in #15, the creation and delete steps don't have data to capture
        query_list = [q + ";" for q in query_text.split(";") if len(q.strip()) > 0]

        # if query includes a view (make view, query, delete view)
        if len(query_list) == 3:
            query_1_result = self.sfc.query(query_list[0])
            if self.verbose:
                print("Non-query reply:", query_1_result.fetchall())

            query_2_result = self.sfc.query(query_list[1])
            query_result = query_2_result
            df_result = query_result.fetch_pandas_all()
            qid = query_result.sfqid

            query_3_result = self.sfc.query(query_list[2])
            if self.verbose:
                print("Non-query reply:", query_3_result.fetchall())

        # single query statement
        else:
            query_result = self.sfc.query(query_text)
            df_result = query_result.fetch_pandas_all()
            qid = query_result.sfqid

        t1 = pd.Timestamp.now("UTC")

        return t0, t1, df_result, query_text, qid

    def query_history(self, t0, t1):
        """Get the time bound query history for the current Snowflake context, as set
        by connector cursor - project, warehouse and database - using the
        `information_schema` database

        Note: Activity in the last 7 days - 6 months is available, latency should be none.
        https://docs.snowflake.com/en/sql-reference/info-schema.html
        https://docs.snowflake.com/en/sql-reference/functions/query_history.html

        Parameters
        ----------
        Both parameters can be either datetime, pd.Timestamp, or str objects
        that can be parsed by pd.to_datetime
        t0 : start time
        t1 : end time
        """
        t0 = pd.to_datetime(t0)
        t1 = pd.to_datetime(t1)
        t0 = t0.strftime("%Y-%m-%d %H:%M:%S")
        t1 = t1.strftime("%Y-%m-%d %H:%M:%S")

        query_text = ("select * " +
                      "from table(information_schema.query_history(" +
                      f"end_time_range_start=>to_timestamp_ltz('{t0}')," +
                      f"end_time_range_end=>to_timestamp_ltz('{t1}')));")

        query_result = self.sfc.query(query_text)
        df_result, qid = self.parse_query_result(query_result)
        return df_result, qid

    def copy(self, destination):
        """Copy current database to new database"""

        #query_text = f"create or replace database DS_100GB_01 clone DS_100GB"
        query_text = f"create or replace database {destination} clone {self.database}"
        query_result = self.sfc.query(query_text)
        return query_result

    def query_seq(self, seq, seq_id=None, qual=None, save=False, verbose_iter=False):
        """Query Snowflake with TPC-DS or TPC-H query template number n

        Parameters
        ----------
        seq : iterable sequence int, query numbers to execute between
            1 and 99 for ds and 1 and 22 for h
        seq_id : str, optional id for stream sequence - i.e. 0 or 4 etc,
            this id is the stream id from ds or h
        qual : None, or True to use qualifying values (to test 1GB qualification db)
        save : bool, save data about this query sequence to disk
        verbose_iter : bool, print per iteration status statements

        Returns
        -------
        if length of sequence is 1, Snowflake cursor reply object
        else None
        """

        if seq_id is None:
            seq_id = "sNA"
        n_time_data = []
        columns = ["db", "test", "scale", "source", "cid", "desc",
                   "query_n", "seq_id", "driver_t0", "driver_t1", "qid"]

        t0_seq = pd.Timestamp.now("UTC")
        i_total = len(seq)
        for i, n in enumerate(seq):
            qn_label = self.database + "-q" + str(n) + "-" + seq_id + "-" + self.desc
            qn_label = qn_label.lower()

            if verbose_iter:
                print("="*40)
                print("Snowflake Start Query:", n)
                print("-"*20)
                print("Stream Completion: {} / {}".format(i+1, i_total))
                print("Query Label:", qn_label)
                print("-"*20)
                print()

            self.set_query_label(qn_label)

            (t0, t1,
             df_result, query_text, qid) = self.query_n(n=n,
                                                        qual=qual,
                                                        std_out=False
                                                        )

            _d = ["sf", self.test, self.scale, self.database, self.cid, self.desc,
                  n, seq_id, t0, t1, qid]
            n_time_data.append(_d)

            # write results as collected by each query
            if save:
                if len(df_result) > 0:
                    self.write_results_csv(df=df_result, query_n=n)

            if verbose_iter:
                dt = t1 - t0
                print("Query ID: {}".format(qid))
                print("Total Time Elapsed: {}".format(dt))
                print("-"*40)
                print()

            if self.verbose:
                if len(df_result) < 25:
                    print("Result:")
                    print("-------")
                    print(df_result)
                    print()
                else:
                    print("Head of Result:")
                    print("---------------")
                    print(df_result.head())
                    print()

        t1_seq = pd.Timestamp.now("UTC")

        #if self.verbose:
        dt_seq = t1_seq - t0_seq
        print()
        print("="*40)
        print("Snowflake Query Stream Done!")
        print("Total Time Elapsed: {}".format(dt_seq))
        print()

        # write local timing results to file
        self.write_times_csv(results_list=n_time_data, columns=columns)

        # this was for multi-query results, maybe remove?
        if len(seq) == 1:
            return df_result
        else:
            return

    def write_results_csv(self, df, query_n):
        """Write the results of a TPC query to a CSV file in a specific
        folder

        Parameters
        ----------
        df : Pandas DataFrame
        query_n : int, query number in TPC test
        """

        fd = self.results_dir + config.sep
        tools.mkdir_safe(fd)
        fp = fd + "query_result_sf_{0:02d}.csv".format(query_n)
        df = tools.to_consistent(df, n=config.float_precision)
        df.to_csv(fp, index=False, float_format="%.3f")

    def write_times_csv(self, results_list, columns):
        """Write a list of results from queries to a CSV file

        Parameters
        ----------
        results_list : list, data as recorded on the local machine
        columns : list, column names for output CSV
        """
        _, fp = tools.make_name(db="sf", test=self.test, cid=self.cid,
                                kind="times",
                                datasource=self.database, desc=self.desc,
                                ext=".csv",
                                timestamp=self.timestamp)
        self.results_csv_fp = self.results_dir + config.sep + fp
        df = pd.DataFrame(results_list, columns=columns)
        tools.mkdir_safe(self.results_dir)
        df.to_csv(self.results_csv_fp, index=False)
