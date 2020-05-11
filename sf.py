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
import pandas as pd
import logging
import atexit

import config, poor_security


def open_connection(verbose=False):
    """Starts Snowflake warehouse and opens a connection.
    Reads values saved in config.py and poor_security.py

    Returns
    -------
    conn : Snowflake connector object
    """

    if verbose:
        print(f'using configuration: user:{poor_security.sf_username}')
        print(f'pass: {poor_security.sf_password}')
        print(f'account: {config.sf_account}')

    conn = snowflake.connector.connect(user=poor_security.sf_username,
                                       password=poor_security.sf_password,
                                       account=config.sf_account
                                       )
    return conn


# Variables
TABLES_DS = [
    'customer_address', 'customer_demographics', 'ship_mode', 'time_dim', 'reason', 'income_band', 'item',
    'store', 'call_center', 'customer', 'web_site', 'store_returns', 'household_demographics', 'web_page', 'promotion', 'catalog_page',
    'inventory', 'catalog_returns', 'web_returns', 'web_sales', 'catalog_sales', 'store_sales',
]
TABLE_DS_SKIP = ['date_dim']
TABLES_H = ['nation', 'lineitem', 'customer', 'orders', 'part', 'partsupp', 'region', 'supplier']

#DATASET_SIZES = ['1GB', '2GB', '100GB', '1000GB', '10000GB']

GCS_LOCATION = 'gcs://tpc-benchmark-5947' #TODO: needs to go to config

#config.sf_role = 'ACCOUNTADMIN' # TODO: needs to go to config
#config.sf_role = 'SYSADMIN'

#storage_integration_name = 'gcs_storage_integration' #TODO: needs to go to config
#config.sf_named_file_format = 'csv_file_format'  # TODO: move to config


class SnowflakeHelper:
    """ manages snowflake db  """
    def __init__(self, test, scale, verbose=False):
        """"Snowflake connection Class

        Parameters
        ----------
        test : TPC test type, either 'h' or 'ds'
        scale : TPC scale factor in Gigabytes, 100, 1000 or 10000 expected
        """

        self.test = test            # TPC-DS or TPC-h as 'ds' or 'h'
        self.scale = scale          # TPC Scale factor in GB
        #self.gcs_file_range = config.cpu_count  # auto detected in config.py

        # user cache control for queries
        self.cached = False

        self.conn = None
        self.is_integrated = False

        if verbose:
            print('Preparing to open connection to Snowflake...')
        self.conn = open_connection()
        if verbose:
            print('Connection opened.')

        if self.test not in ["ds", "h"]:  # validate test type requested
            raise Exception(f"Unsupported test: {self.test}")

        # set tables to be imported to snowflake based on test
        if self.test == "h":
            # create named file formats for TPC
            self.tables = TABLES_H
        else:
            self.tables = TABLES_DS
        print(self.tables)

        atexit.register(self._close_connection)

    def _close_connection(self):
        """ closes connection"""
        # close connection
        self.conn.close()

    def _get_cost(self, running_time):
        """ estimates cost for query runtime based on warehouse size """
        # TODO: review this
        return running_time * config.sf_warehouse_cost

    def brute_force_clean_query(self, query_text):
        query_text = query_text.replace('set rowcount', 'LIMIT').strip()
        query_text = query_text.replace('top 100;', 'LIMIT 100;').strip()
        query_text = query_text.replace('\n top 100', '\n LIMIT 100').strip()

        if query_text.endswith('go'):
            query_text = query_text[:len(query_text) - 2]

        return query_text

    def run_queries(self, queries):
        """Run one or more queries


        opens cursor, runs query and returns a single (first) result or
        all if 'fetch-all' flag is specified """

        batch_t0 = None
        batch_running_time = 0.0
        batch_row_count = 0
        batch_cost = 0.0
        batch_data = []

        for query in queries:
            (t0, t1, bytes_processed, row_count, cost, rows) = self.run_query(query)
            # properly count times (only count execution time):

            # first query sets start time
            if not batch_t0:
                batch_t0 = t0

            # add this query time to batch total running time
            #print(t1, t0)
            dt = t1 - t0
            batch_running_time += dt.total_seconds()

            # add rest of data
            batch_row_count += row_count
            batch_cost += cost
            if len(rows) > 0:
                batch_data.append(rows)

        # calculate batch end time running time by adding runtime duration to start time
        batch_t1 = batch_t0 + pd.Timedelta(batch_running_time, "s")

        # return data just for second select query
        return batch_t0, batch_t1, -1, batch_row_count, batch_cost, []

    def run_query(self, query_text, dry_run=False, verbose=False):
        """Run a query on Snowflake
        Opens cursor, runs query and returns a single (first) result or
        all if 'fetch-all' flag is specified

        Parameters
        ----------
        query_text : str, query to execute
        dry_run : bool, execute on Snowflake as a dry run, i.e. no computation
        verbose : bool, print debug statements
        """

        cs = self.conn.cursor()
        row_count = 0
        t0 = None
        t1 = None
        rows = []
        cost = None

        if self.cached:
            cs.execute("ALTER SESSION SET USE_CACHED_RESULT=true")
        else:
            cs.execute("ALTER SESSION SET USE_CACHED_RESULT=false")

        try:
            # execute query and capture time
            t0 = pd.Timestamp.now()
            cs.execute(query_text)
            t1 = pd.Timestamp.now()

            # extract row count and data
            row_count = cs.rowcount
            rows = cs.fetchall()
            dt = t1 - t0

            cost = self._get_cost(dt.total_seconds())
        except Exception as ex:
            t1 = pd.Timestamp.now()
            print(f'Error running query """{query_text}""", error: {ex}')
        finally:
            cs.close()

        logging.debug(f'query results: {t0}, {t1}, {row_count}, {len(rows)}, {cost}')

        return t0, t1, -1, row_count, cost, rows

    def warehouse_start(self, verbose=False):  #create_db=False, verbose=False):
        """ starts warehouse """
        # check if connection is set
        if not self.conn:
            self.conn = open_connection()

        # resume warehouse
        query_text = f'USE ROLE {config.sf_role}'
        if verbose:
            print(f'running query: {query_text}')
        self.run_query(query_text)

        query_text = f'ALTER WAREHOUSE {config.sf_warehouse} RESUME;'
        if verbose:
            print(f'running query: {query_text}')
        self.run_query(query_text)

        query_text = f'USE WAREHOUSE {config.sf_warehouse}'
        print(f'running query: {query_text}')
        self.run_query(query_text)

        #if create_db:
        #    query_text = f'CREATE DATABASE IF NOT EXISTS {self.test}_{self.scale}'
        #    if verbose:
        #        print(f'running query: {query_text}')
        #    self.run_query(query_text)

        #query_text = f'USE DATABASE {self.test}_{self.scale}'
        #if verbose:
        #    print(f'running query: {query_text}')
        #self.run_query(query_text)

    def select_database(self, verbose=False):
        query_text = f'USE DATABASE {self.test}_{self.scale}'
        if verbose:
            print(f'running query: {query_text}')
        self.run_query(query_text)

    def create_database(self, verbose=False):
        query_text = f'CREATE DATABASE IF NOT EXISTS {self.test}_{self.scale}'
        if verbose:
            print(f'running query: {query_text}')
        self.run_query(query_text)

    def warehouse_suspend(self, verbose=False):
        """ suspends warehouse and closes connection """
        # suspend warehouse
        result = self.run_query(f'ALTER WAREHOUSE {config.sf_warehouse} SUSPEND;')
        if verbose:
            print(f'warehouse suspend: {result}')
        # close connection
        self.conn.close()
        # reset connection object
        self.conn = None

    def create_integration(self, is_dry_run=False, verbose=False):
        """ integrates snowflake account with GCS location """
        # Integrating Snowflake and GCS is a multi-step process requiring
        # actions on both Snowflake and GCP IAM side

        # STEP 1: tell snowflake about CSV file structures we're
        # # expecting to import from GCS
        self._create_named_file_format(is_dry_run)

        # STEP 2: we need to "stage" all GCS .dat files for
        # # Snowflake to import via "COPY INTO" statement
        # generate "stage" name
        st_int_name = f'gcs_{self.test}_{self.scale}_integration'

        if verbose:
            print(f'Integrating "{st_int_name}" ... ')

        # link to GCS URI (and creates a service account which needs
        # storage permissions in GCS IAM)
        self._create_storage_integration(st_int_name, is_dry_run)

        # grant snowflake user permissions to access "storage integration"
        # in order to great a STAGE
        self._grant_storage_integration_access(st_int_name, is_dry_run)

        # create STAGE: which knows what GCS URI to pull from,
        # what file in bucket, how to read CSV file
        self._create_stage(st_int_name, is_dry_run)

        # test stage
        self._list_stage(st_int_name, is_dry_run)
        if verbose:
            print(f'--finished staging "{st_int_name}"\n\n')

        self.is_integrated = True

        return st_int_name

    def create_schema(self, is_dry_run=False):
        """ list items in "stage"

         Send a schema to SF Warehouse
         """
        print(f'\n\n--pushing schema: "{config.h_schema_ddl_filepath}"')

        # open file and read all rows:
        if self.test == "ds":
            schema_file = '/home/vagrant/bq_snowflake_benchmark/ds/v2.11.0rc2/tools/tpcds.sql'
        else:
            schema_file = self.config.h_schema_ddl_filepath
        with open(schema_file) as f:
            lines = f.readlines()

        # extract queries:
        queries = []
        current_query = ''
        for line in lines:
            # check if this is end of query
            if ';' in line:
                # if end of query found, add it and reset
                current_query += line
                queries.append(current_query)
                current_query = ''
            else:  # end of query not found, keep accumulating
                current_query += line

        # run queries
        for query in queries:
            if is_dry_run:
                print(f'{query}\n\n\n')
            else:
                print(f'running query: {query}')
                result = self.run_query(query)
                print(f'result {result}')

        print(f'\n\n--finished pushing schema')
        return

    def _extract_table_name_from_gcs_filepath(self, gcs_filepath):
        """ since list @stage returns all files for all tests, we need to match table name, size when importing data """

        # ignore debug files
        filename = gcs_filepath[len(GCS_LOCATION) + 1:]  # +1 is for slash
        if filename.startswith('_data_'):
            return False, None

        # different tests have different naming conventions
        if self.test == "ds":
            # validate filename prefix (make sure it belongs to this test and size)
            gcs_prefix = f'{GCS_LOCATION}/{self.test}_{self.scale}_'

            # before proceeding, check if ignore everything not matching our TEST TYPE and TEST SIZE
            if not gcs_filepath.startswith(gcs_prefix):
                return False, None

            # get table name from gcs_filepath (note: avoiding conflicts like "customer" and "customer_address")
            gcs_filepath_table_tokens = gcs_filepath[len(gcs_prefix):].split('_')
            gcs_filepath_table = gcs_filepath_table_tokens[0]

            try:  # if second token is not a number, then it's a two_word table
                file_index = int(gcs_filepath_table_tokens[1])
            except ValueError as ex:
                gcs_filepath_table += '_' + gcs_filepath_table_tokens[1]
        else:
            # validate filename prefix (make sure it belongs to this test and size)
            gcs_prefix = f'{GCS_LOCATION}/{self.test}_{self.scale}_'

            # before proceeding, check if ignore everything not matching our TEST TYPE and TEST SIZE
            if not gcs_filepath.startswith(gcs_prefix):
                return False, None

            # get table name from gcs_filepath (note: avoiding conflicts like "customer" and "customer_address")
            gcs_filepath_table_tokens = gcs_filepath[len(gcs_prefix):].split('.')
            gcs_filepath_table = gcs_filepath_table_tokens[0]

        return True, gcs_filepath_table

    def list_integration(self, integration_name, verbose=False):
        """ lists all files in GCS bucket """
        if verbose:
            print(f'Listing stage: "@{integration_name}_stage"')

        # run query on snowflake db
        results = self.run_query(f'list @{integration_name}_stage;')

        # unpack results
        t0, t1, bytes_processed, row_count, cost, rows = results
        if len(rows) == 0:
            print('Error listing integration')
            raise

        # db for keeping cleaned up and sorted .dat filenames
        table_files_db = {}

        # for TPC-H test we do not need to scan the list due to the way snowflake treats filenames (as regex),
        # so file ending with `tbl.1` will match all files following the pattern (ie: `tbl.11`, `tbl.12`, etc.)
        if self.test == "h":
            for table in self.tables:
                if table not in table_files_db.keys():
                    table_files_db[table] = []
                if table in ('nation', 'region'):
                    table_files_db[table].append(f'{GCS_LOCATION}/{self.test}_{self.scale}_{table}.tbl')
                    continue
                for i in range(1, 10):
                    table_files_db[table].append(f'{GCS_LOCATION}/{self.test}_{self.scale}_{table}.tbl.{i}')
            return table_files_db

        # cleanup results and sort files into buckets based on table name
        for row in rows:
            # extract gcs filepath from response
            gcs_filepath = row[0]

            # extract table name from file being processed
            matched, gcs_filepath_table = self._extract_table_name_from_gcs_filepath(gcs_filepath)

            # skip files found in bucket which are not related to this test
            if not matched:
                continue

            # see which table this files belongs to and append to appropriate list
            is_found_table = False
            for table in self.tables:
                if table == gcs_filepath_table:
                    # first entry
                    if table not in table_files_db.keys():
                        table_files_db[table] = []
                    # add gcs file to table list
                    table_files_db[table].append(gcs_filepath)
                    is_found_table = True
                    break
            # if file matches no tables, raise and exception!
            if not is_found_table:
                print(f'unknown table!!!! {gcs_filepath}')

        print(f'\n\n--done listing stage')
        return table_files_db

    def _create_named_file_format(self, dry_run=False, verbose=False):

        """ creates NAMED FILE FORMATs in snowflake db """

        if verbose:
            print("Creating named file format:", config.sf_named_file_format)

        # config.sf_named_file_format = f'{self.test}_{self.scale}_{table}_csv_format'
        # TODO: not sure if we need to create a named file format for each table
        query_text = f"""create or replace file format {config.sf_named_file_format}
                     type = csv
                     field_delimiter = '|'
                     skip_header = 0
                     null_if = ('NULL', 'null')
                     empty_field_as_null = true
                     encoding = 'iso-8859-1' 
                     compression = none;"""

        if dry_run:
            print(query_text)
        else:
            print(f'running query: {query_text}')
            result = self.run_query(query_text)
            print(f'result {result}')

        print(f'\n\n--done creating named file format')
        return

    def _create_storage_integration(self, st_int_name, is_dry_run=False):
        """ creates STORAGE INTEGRATION """
        print(f'\n\n--creating storage integration: "{st_int_name}"')

        query_text = (f"CREATE STORAGE INTEGRATION {st_int_name} " +
                      "TYPE=EXTERNAL_STAGE " +
                      "STORAGE_PROVIDER=GCS " +
                      "ENABLED=TRUE " +
                      f"STORAGE_ALLOWED_LOCATIONS=('{GCS_LOCATION}/');")

        if is_dry_run:
            print(query_text)
        else:
            print(f'running query: {query_text}')
            result = self.run_query(query_text)
            print(f'result {result}')

        print(f'\n\n--finished creating storage integration')
        return

    def _grant_storage_integration_access(self, st_int_name, is_dry_run=False):
        """ grant access to STORAGE INTEGRATION and STAGE creation"""
        query = f'GRANT CREATE STAGE on schema public to ROLE {config.sf_role};'

        if is_dry_run:
            print(query)
        else:
            print(f'running query: {query}')
            result = self.run_query(query)
            print(f'result {result}')

        query = f'GRANT USAGE on INTEGRATION {st_int_name} to ROLE {config.sf_role};'

        if is_dry_run:
            print(query)
        else:
            print(f'running query: {query}')
            result = self.run_query(query)
            print(f'result {result}')
        return

    def _create_stage(self, st_int_name, is_dry_run=False):
        """ creates STAGE for each file needed during import """

        query = f'''CREATE STAGE {st_int_name}_stage URL='{GCS_LOCATION}' STORAGE_INTEGRATION={st_int_name} FILE_FORMAT={config.sf_named_file_format};'''

        if is_dry_run:
            print(query)
        else:
            print(f'running query: {query}')
            result = self.run_query(query)
            print(f'result {result}')
        return

    def _list_stage(self, st_int_name, is_dry_run=False):
        """ list items in "stage" """

        query = f'list  @{st_int_name}_stage;'

        if is_dry_run:
            print(query)
        else:
            print(f'running query: {query}')
            result = self.run_query(query)
            print(f'result {result}')
        return

    def import_data(self, table, gcs_file_path, storage_integration, is_dry_run=False):
        """ run import """
        query = (f"copy into {table} from '{gcs_file_path}' " +
                 f"storage_integration={storage_integration} " +
                 "file_format=(format_name=csv_file_format);")

        if is_dry_run:
            print(query)
        else:
            print(f'running query: {query}')
            result = self.run_query(query)
            print(f'result {result}')
        return
