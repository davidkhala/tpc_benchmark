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
    # connect to snowflake using authentication data from config
    # TODO: warehouse and db probably shouldn't be passed here and should be "created if not exist"
    # conn = snowflake.connector.connect(
    #     user=config.sf_username,
    #     password=config.sf_password,
    #     account=config.sf_account,
    #     warehouse=config.sf_warehouse,
    #     database=config.sf_database,
    # )

    conn = snowflake.connector.connect(
        user='sadadauren',
        password='Test1234!',
        account='ed75261.us-central1.gcp'
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
TABLES_H = ['customer', 'lineitem', 'nation', 'orders', 'part', 'partsupp', 'region', 'supplier']
DATASET_SIZES = ['1GB', '2GB', '100GB']
GCS_LOCATION = 'gcs://tpc-benchmark-5947' #TODO: needs to go to config
SF_ROLE = 'ACCOUNTADMIN' # TODO: needs to go to config
storage_integration_name = 'gcs_storage_integration' #TODO: needs to go to config
named_file_format_name = 'csv_file_format' # TODO: move to config
is_integration = False

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
        # save type of test to run: C or DS
        if test_type not in [TEST_DS, TEST_H]:
            print(f'Unsupported test: {self.test_type}')
            raise
        self.test_type = test_type
        # size of dataset to use
        self.test_size = test_size

        # default file gcs file range:
        self.gcs_file_range = 12 # 1 GB

        if test_size == '100GB':
            self.gcs_file_range = 96  # 100 GB

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

    def _run_query(self, query):
        """ opens cursor, runs query and returns result """
        cs = self.conn.cursor()
        result = None
        try:
            cs.execute(query)
            one_row = cs.fetchone()
            result = one_row[0]
        except Exception as ex:
            print(f'Error running query {ex}')
        finally:
            cs.close()
        return result

    def warehouse_start(self):
        """ starts warehouse """
        # check if connection is set
        if not self.conn:
            self.conn = _open_connection()

        # suspend warehouse
        query = f'ALTER WAREHOUSE {self.config.sf_warehouse} RESUME;'
        print(f'running query: {query}')
        result = self._run_query(query)
        print(f'warehouse start: {result}')

        query = f'USE DATABASE SF_TUTS'
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
        return is_integration

    def create_integration(self):
        """ integrates snowflake account with GCS location """
        # Integrating Snowflake and GCS is a multi-step process requiring actions on both Snowflake and GCP IAM side

        # STEP 1: tell snowflake about CSV file structures we're expecting to import from GCS
        self._create_named_file_format()


        # first let's get list of files from GCS bucket (we will only create stages for files that exist in bucket)
        # bucket_items = self.list_integration()
        # print('available buckets:')
        # print(bucket_items)

        # go through each table
        for table in self.tables:

            # generate gcs file name for this table and this specific file index
            for file_idx in range(1, self.gcs_file_range):
                st_int_name = f'{self.test_type}_{self.test_size}_{table}_{file_idx}_{self.gcs_file_range}'
                print(f'staging file {st_int_name}')
                # link to GCS URI (and creates a service account which needs storage permissions in GCS IAM)
                self._create_storage_integration(st_int_name)

                # grant snowflake user permissions to access "storage integration" in order to great a STAGE
                self._grant_storage_integration_access(st_int_name)

                # create STAGE: which knows what GCS URI to pull from, what file in bucket, how to read CSV file
                self._create_stage(st_int_name)

        # test storage
        self._list_stage()

        return

    def list_integration(self):
        """ lists all files in GCS bucket """
        return self._run_query(f'list @{storage_integration_name};')

    def _create_named_file_format(self):
        """ creates NAMED FILE FORMATs in snowflake db """
        # named_file_format_name = f'{self.test_type}_{self.test_size}_{table}_csv_format'
        # TODO: not sure if we need to create a named file format for each table
        query = f'''create or replace file format {named_file_format_name}
            type = csv
            field_delimiter = '|'
            skip_header = 1
            null_if = ('NULL', 'null')
            empty_field_as_null = true
            compression = none;'''

        print(f'running query: {query}')
        result = self._run_query(query)
        print(f'result {result}')
        return

    def _create_storage_integration(self, st_int_name):
        """ creates STORAGE INTEGRATION """

        query = f'''CREATE STORAGE INTEGRATION {st_int_name}
            TYPE = EXTERNAL_STAGE
            STORAGE_PROVIDER = GCS
            ENABLED = TRUE
            STORAGE_ALLOWED_LOCATIONS = ('{GCS_LOCATION}/{st_int_name}.dat');'''

        print(f'running query: {query}')
        result = self._run_query(query)
        print(f'result {result}')
        return

    def _grant_storage_integration_access(self, st_int_name):
        """ grant access to STORAGE INTEGRATION """
        query = f'grant create stage on schema public to role {SF_ROLE}'
        print(f'running query: {query}')
        result = self._run_query(query)
        print(f'result {result}')

        query = f'grant usage on integration {st_int_name} to role {SF_ROLE}'
        print(f'running query: {query}')
        result = self._run_query(query)
        print(f'result {result}')
        return

    def _create_stage(self, st_int_name):
        """ creates STAGE for each file needed during import """

        # TODO: create stage per each table
        query = f'''create stage {st_int_name}_stage
          url = {GCS_LOCATION}
          storage_integration = {st_int_name}
          file_format = {named_file_format_name};'''

        print(f'running query: {query}')
        result = self._run_query(query)
        print(f'result {result}')
        return

    def _list_stage(self):
        pass

    def import_data(self, table, st_int_name):
        """ run import """
        query = f'copy into {table} from @{st_int_name}_stage;'
        print(f'running query: {query}')
        result = self._run_query(query)
        print(f'result {result}')
        return