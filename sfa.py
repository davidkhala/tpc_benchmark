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

import config, poor_security, gcp_storage
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


def parse_query_job(query_job, verbose=False):
    """

    Parameters
    ----------
    query_job_tuple : results from snowflake
    verbose : bool, print results

    Returns
    -------
    t0 : datetime object, time query started
    t1 : datetime object, time query ended
    bytes_processed : int, bytes processed with query
    bytes_billed : int, bytes billed for query
    df : Pandas DataFrame containing results of query
    """
    t0, t1, bytes_processed, row_count, cost, data = query_job
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
    def __init__(self, verbose_query=False, verbose=False):
        """"Snowflake Connector wrapper class"""
        self.verbose_query = verbose_query  # print query text
        self.verbose = verbose              # debug output for whole class
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
            print(f'Using configuration: user:{username}')
            print(f'Password: {password}')
            print(f'Account: {account}')

        self.conn = snowflake.connector.connect(user=username,
                                                password=password,
                                                account=account
                                                )
        self.cursor = self.conn.cursor()
        if verbose:
            print("Connector cursor created.")

    def query(self, query_text, verbose=False):
        """Opens cursor, runs query and returns all results at once

        Parameters
        ----------
        query_text : str, query to execute
        verbose : bool, print debug statements
        """

        assert self.conn is not None, "Connection not initialized"

        if self.dry_run:
            return
        else:
            if self.verbose_query:
                print("QUERY TEXT")
                print("==========")
                print(query_text)
                print()
            result = self.cursor.execute(query_text)
            return result

    def cache_on(self):
        self.query("ALTER SESSION SET USE_CACHED_RESULT=true")

    def cache_off(self):
        self.query("ALTER SESSION SET USE_CACHED_RESULT=false")

    def role_select(self, role, verbose=False):
        self.query(f'USE ROLE {role}', verbose=verbose)

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


class SFTPC:
    def __init__(self, warehouse, test, scale, n, verbose_query=False, verbose=False):
        self.warehouse = warehouse
        self.test = test
        self.scale = scale
        self.n = n

        self.role_str = None
        self.scale_str = str(scale)+"GB"

        self.database = f"{self.test}_{self.scale_str}_{self.n}"

        self.storage_integration_name = self.database + "_gcs_integration"

        self.df_gcs_full = None  # all files in bucket, as fyi
        self.df_gcs = None       # just files for this dataset

        self.verbose = verbose
        self.verbose_query = verbose_query

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

    def close(self):
        self.sfc.close()

    def cache(self, on=False):
        if on:
            self.sfc.cache_on()
        else:
            self.sfc.cache_off()

    def role(self, role):
        self.role_str = role
        self.sfc.role_select(role, verbose=self.verbose)

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
            _df.apply(lambda r: self.import_data_apply(table=r.table,
                                                       gcs_file_path=r.uri),
                      axis=1)
            if self.verbose:
                print("Done!")
                print()

    def query_n(self, n,
                qual=None,
                verbose=False, verbose_out=False):

        """Query Snowflake with a specific Nth query"""
        tpl_dir = f"{config.fp_query_templates}{config.sep}{'sf'}_{self.test}"

        # generate query text
        if self.test == "ds":
            query_text = ds_setup.qgen_template(n=n,
                                                templates_dir=tpl_dir,
                                                dialect="sqlserver_tpc",
                                                scale=self.scale,
                                                qual=qual,
                                                verbose=verbose,
                                                verbose_out=verbose_out)
        elif self.test == "h":
            query_text = h_setup.qgen_template(n=n,
                                               templates_dir=tpl_dir,
                                               scale=self.scale,
                                               qual=qual,
                                               verbose=verbose,
                                               verbose_out=verbose_out)
        else:
            return None
        t0 = pd.Timestamp.now()
        query_result = self.sfc.query(query_text)
        t1 = pd.Timestamp.now()
        #return n, query_text, start, end, bytes_processed, rows_count, cost, df
        return query_result

    def query_history(self):
        query_text = ("select * from table(information_schema.query_history()) " +
                      "order by start_time;")
        query_result = self.sfc.query(query_text)
        df = query_result.fetch_pandas_all()
        qid = query_result.sfqid
        return df, qid
