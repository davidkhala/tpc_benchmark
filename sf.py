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


def _open_connection(config):
    """ starts warehouse, opens connection """
    print(f'using config: user:{config.sf_username}, pass: {config.sf_password}, account: {config.sf_account}')
    # connect to snowflake
    conn = snowflake.connector.connect(
        user=config.sf_username,
        password=config.sf_password,
        account=config.sf_account
    )
    return conn


# Variables
TEST_DS = 'ds'
TEST_H = 'h'
TABLES_DS = [
    'customer_address', 'customer_demographics', 'ship_mode', 'time_dim', 'reason', 'income_band', 'item',
    'store', 'call_center', 'customer', 'web_site', 'store_returns', 'household_demographics', 'web_page', 'promotion', 'catalog_page',
    'inventory', 'catalog_returns', 'web_returns', 'web_sales', 'catalog_sales', 'store_sales',
]
TABLE_DS_SKIP = ['date_dim']
TABLES_H = ['nation', 'lineitem', 'customer', 'orders', 'part', 'partsupp', 'region', 'supplier']
DATASET_SIZES = ['1GB', '2GB', '100GB', '1000GB', '10000GB']
GCS_LOCATION = 'gcs://tpc-benchmark-5947' #TODO: needs to go to config
SF_ROLE = 'ACCOUNTADMIN' # TODO: needs to go to config
#SF_ROLE = 'SYSADMIN'
storage_integration_name = 'gcs_storage_integration' #TODO: needs to go to config
named_file_format_name = 'csv_file_format'  # TODO: move to config
is_integrated = False

class SnowflakeHelper:
    """ manages snowflake db  """
    def __init__(self, test_type, test_size, config):
        """" initializes helper class """
        # open connection
        print('preparing to open connection to Snowflake')
        self.conn = _open_connection(config)
        print('connection opened')
        # save config
        self.config = config
        # validate test type requested
        if test_type not in [TEST_DS, TEST_H]:
            err = f'Unsupported test: {self.test_type}'
            print(err)
            raise err
        # save type of test to run: C or DS
        self.test_type = test_type
        # size of dataset to use
        self.test_size = test_size

        # TODO: this should be based on number of CPUs
        self.gcs_file_range = 96

        # set tables to be imported to snowflake based on test
        if self.test_type == TEST_H:
            # create named file formats for TPC
            self.tables = TABLES_H
        else:
            self.tables = TABLES_DS

    def _close_connection(self):
        """ closes connection"""
        # close connection
        self.conn.close()

    def _run_query(self, query, fetch_all=False):
        """ opens cursor, runs query and returns a single (first) result or all if 'fetch-all' flag is specified """
        cs = self.conn.cursor()
        result = None
        try:
            cs.execute(query)
            if not fetch_all:
                rows = cs.fetchone()
                return rows[0]
            else:
                rows = cs.fetchall()
                return rows
        except Exception as ex:
            print(f'Error running query {ex}')
        finally:
            cs.close()
        return result

    def warehouse_start(self):
        """ starts warehouse """
        # check if connection is set
        if not self.conn:
            self.conn = _open_connection(self.config)

        # resume warehouse
        query = f'USE ROLE {SF_ROLE}'
        print(f'running query: {query}')
        result = self._run_query(query)
        print(f'result: {result}')

        query = f'ALTER WAREHOUSE {self.config.sf_warehouse} RESUME;'
        print(f'running query: {query}')
        result = self._run_query(query)
        print(f'warehouse start: {result}')

        query = f'USE WAREHOUSE {self.config.sf_warehouse}'
        print(f'running query: {query}')
        result = self._run_query(query)
        print(f'result: {result}')

        query = f'CREATE DATABASE IF NOT EXISTS {self.test_type}_{self.test_size}'
        print(f'running query: {query}')
        result = self._run_query(query)
        print(f'result: {result}')

        query = f'USE DATABASE {self.test_type}_{self.test_size}'
        print(f'running query: {query}')
        result = self._run_query(query)
        print(f'result: {result}')

    def warehouse_suspend(self):
        """ suspends warehouse and closes connection """
        # suspend warehouse
        result = self._run_query(f'ALTER WAREHOUSE {self.config.sf_warehouse} SUSPEND;')
        print(f'warehouse suspend: {result}')
        # close connection
        self.conn.close()
        # reset connection object
        self.conn = None

    def is_integrated(self):
        """ checks to see if we need to run GCS integration again """

        # TODO: for now set it manually
        return is_integrated

    def create_integration(self, is_dry_run=False):
        """ integrates snowflake account with GCS location """
        # Integrating Snowflake and GCS is a multi-step process requiring actions on both Snowflake and GCP IAM side

        # STEP 1: tell snowflake about CSV file structures we're expecting to import from GCS
        self._create_named_file_format(is_dry_run)

        # STEP 2: we need to "stage" all GCS .dat files for Snowflake to import via "COPY INTO" statement
        # generate "stage" name
        st_int_name = f'gcs_{self.test_type}_{self.test_size}_integration'
        print(f'\n\n--integrating "{st_int_name}" ... ')
        # link to GCS URI (and creates a service account which needs storage permissions in GCS IAM)
        self._create_storage_integration(st_int_name, is_dry_run)

        # grant snowflake user permissions to access "storage integration" in order to great a STAGE
        self._grant_storage_integration_access(st_int_name, is_dry_run)

        # create STAGE: which knows what GCS URI to pull from, what file in bucket, how to read CSV file
        self._create_stage(st_int_name, is_dry_run)

        # test stage
        self._list_stage(st_int_name, is_dry_run)

        print(f'--finished staging "{st_int_name}"\n\n')
        return st_int_name

    def create_schema(self, is_dry_run=False):
        """ list items in "stage" """
        print(f'\n\n--pushing schema: "{self.config.h_schema_ddl_filepath}"')

        # open file and read all rows:
        # with open(self.config.h_schema_ddl_filepath, 'r') as f:
        with open('/home/vagrant/bq_snowflake_benchmark/ds/v2.11.0rc2/tools/tpcds.sql') as f:
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
                result = self._run_query(query)
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
        if self.test_type == TEST_DS:
            # validate filename prefix (make sure it belongs to this test and size)
            gcs_prefix = f'{GCS_LOCATION}/{self.test_type}_{self.test_size}_'

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
            gcs_prefix = f'{GCS_LOCATION}/{self.test_type}_{self.test_size}_'

            # before proceeding, check if ignore everything not matching our TEST TYPE and TEST SIZE
            if not gcs_filepath.startswith(gcs_prefix):
                return False, None

            # get table name from gcs_filepath (note: avoiding conflicts like "customer" and "customer_address")
            gcs_filepath_table_tokens = gcs_filepath[len(gcs_prefix):].split('.')
            gcs_filepath_table = gcs_filepath_table_tokens[0]

        return True, gcs_filepath_table

    def list_integration(self, integration_name):
        """ lists all files in GCS bucket """
        print(f'\n\n--listing stage: "@{integration_name}_stage"')

        # run query on snowflake db
        rows = self._run_query(f'list @{integration_name}_stage;', fetch_all=True)

        if len(rows) == 0:
            print('Error listing integration')
            raise

        # db for keeping cleaned up and sorted .dat filenames
        table_files_db = {}

        # for TPC-H test we do not need to scan the list due to the way snowflake treats filenames (as regex),
        # so file ending with `tbl.1` will match all files following the pattern (ie: `tbl.11`, `tbl.12`, etc.)
        if self.test_type == TEST_H:
            for table in self.tables:
                if table not in table_files_db.keys():
                    table_files_db[table] = []
                if table in ('nation', 'region'):
                    table_files_db[table].append(f'{GCS_LOCATION}/{self.test_type}_{self.test_size}_{table}.tbl')
                    continue
                for i in range(1, 10):
                    table_files_db[table].append(f'{GCS_LOCATION}/{self.test_type}_{self.test_size}_{table}.tbl.{i}')
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

    def _create_named_file_format(self, is_dry_run=False):
        print(f'\n\n--creating named file format: "@{named_file_format_name}"')

        """ creates NAMED FILE FORMATs in snowflake db """
        # named_file_format_name = f'{self.test_type}_{self.test_size}_{table}_csv_format'
        # TODO: not sure if we need to create a named file format for each table
        query = f'''create or replace file format {named_file_format_name}
            type = csv
            field_delimiter = '|'
            skip_header = 1
            null_if = ('NULL', 'null')
            empty_field_as_null = true
            encoding = 'iso-8859-1' 
            compression = none;'''

        if is_dry_run:
            print(query)
        else:
            print(f'running query: {query}')
            result = self._run_query(query)
            print(f'result {result}')

        print(f'\n\n--done creating named file format')
        return

    def _create_storage_integration(self, st_int_name, is_dry_run=False):
        """ creates STORAGE INTEGRATION """
        print(f'\n\n--creating storage integration: "{st_int_name}"')

        query = f'''CREATE STORAGE INTEGRATION {st_int_name} TYPE=EXTERNAL_STAGE STORAGE_PROVIDER=GCS ENABLED=TRUE STORAGE_ALLOWED_LOCATIONS=('{GCS_LOCATION}/');'''

        if is_dry_run:
            print(query)
        else:
            print(f'running query: {query}')
            result = self._run_query(query)
            print(f'result {result}')

        print(f'\n\n--finished creating storage integration')
        return

    def _grant_storage_integration_access(self, st_int_name, is_dry_run=False):
        """ grant access to STORAGE INTEGRATION and STAGE creation"""
        query = f'GRANT CREATE STAGE on schema public to ROLE {SF_ROLE};'

        if is_dry_run:
            print(query)
        else:
            print(f'running query: {query}')
            result = self._run_query(query)
            print(f'result {result}')

        query = f'GRANT USAGE on INTEGRATION {st_int_name} to ROLE {SF_ROLE};'

        if is_dry_run:
            print(query)
        else:
            print(f'running query: {query}')
            result = self._run_query(query)
            print(f'result {result}')
        return

    def _create_stage(self, st_int_name, is_dry_run=False):
        """ creates STAGE for each file needed during import """

        query = f'''CREATE STAGE {st_int_name}_stage URL='{GCS_LOCATION}' STORAGE_INTEGRATION={st_int_name} FILE_FORMAT={named_file_format_name};'''

        if is_dry_run:
            print(query)
        else:
            print(f'running query: {query}')
            result = self._run_query(query)
            print(f'result {result}')
        return

    def _list_stage(self, st_int_name, is_dry_run=False):
        """ list items in "stage" """

        query = f'list  @{st_int_name}_stage;'

        if is_dry_run:
            print(query)
        else:
            print(f'running query: {query}')
            result = self._run_query(query)
            print(f'result {result}')
        return

    def import_data(self, table, gcs_file_path, storage_integration, is_dry_run=False):
        """ run import """
        query = f'''copy into {table} from '{gcs_file_path}' storage_integration={storage_integration} file_format=(format_name=csv_file_format);'''

        if is_dry_run:
            print(query)
        else:
            print(f'running query: {query}')
            result = self._run_query(query)
            print(f'result {result}')
        return
