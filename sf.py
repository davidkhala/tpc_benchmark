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
    conn = snowflake.connector.connect(
        user=config.sf_username,
        password=config.sf_password,
        account=config.sf_account,
        warehouse=config.sf_warehouse,
        database=config.sf_database,
    )
    return conn


# Variables
TEST_DS = 'DS'
TEST_H = 'H'
TABLES_DS = []
TABLES_H = ['customer', 'lineitem', 'nation', 'orders', 'part', 'partsupp', 'region', 'supplier']
DATASET_SIZES = ['1GB', '2GB', '100GB']
GCS_LOCATIONS = ['gcs://tpc-benchmark-5947/'] #TODO: needs to go to config
SF_ROLE = 'ACCOUNTADMIN' # TODO: needs to go to config
storage_integration_name = 'gcs_storage_integration' #TODO: needs to go to config
named_file_format_name = 'csv_file_format' # TODO: move to config


class SnowflakeHelper:
    """ manages snowflake db  """
    def __init__(self, test_type, test_size, config):
        """" initializes helper class """
        # open connection
        self.conn = _open_connection(config)
        # save config
        self.config = config
        # save type of test to run: C or DS
        if test_type not in [TEST_DS, TEST_H]:
            print(f'Unsupported test: {self.test_type}')
            raise
        self.test_type = test_type
        # size of dataset to use
        self.test_size = test_size

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
        result = self._run_query(f'ALTER WAREHOUSE {self.config.sf_warehouse} START;')
        print(f'warehouse start: {result}')

    def warehouse_suspend(self):
        """ suspends warehouse and closes connection """
        # suspend warehouse
        result = self._run_query(f'ALTER WAREHOUSE {self.config.sf_warehouse} SUSPEND;')
        print(f'warehouse suspend: {result}')
        # close connection
        self.conn.close()
        # reset connection object
        self.conn = None

    def create_integration(self, gcs_location):
        """ integrates snowflake account with GCS location """
        # Integrating Snowflake and GCS is a multi-step process requiring actions on both Snowflake and GCP IAM side

        # STEP 1: tell snowflake about CSV file structures we're expecting to import from GCS
        self._create_named_file_format()

        # link to GCS URI (and creates a service account which needs storage permissions in GCS IAM)
        self._create_storage_integration()

        # grant snowflake user permissions to access "storage integration" in order to great a STAGE
        self._grant_storage_integration_access()

        # create STAGE: which knows what GCS URI to pull from, what file in bucket, how to read CSV file
        self._create_stage()

        # test storage
        self._list_stage()

        return

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

    def _create_storage_integration(self):
        """ creates STORAGE INTEGRATION """
        query = f'''CREATE STORAGE INTEGRATION {storage_integration_name}
            TYPE = EXTERNAL_STAGE
            STORAGE_PROVIDER = GCS
            ENABLED = TRUE
            STORAGE_ALLOWED_LOCATIONS = ({GCS_LOCATIONS});'''

        print(f'running query: {query}')
        result = self._run_query(query)
        print(f'result {result}')
        return

    def _grant_storage_integration_access(self):
        """ grant access to STORAGE INTEGRATION """
        query = f'grant create stage on schema public to role {SF_ROLE}'
        print(f'running query: {query}')
        result = self._run_query(query)
        print(f'result {result}')

        query = f'grant usage on integration {storage_integration_name} to role {SF_ROLE}'
        print(f'running query: {query}')
        result = self._run_query(query)
        print(f'result {result}')
        return

    def _create_stage(self):
        """ creates STAGE """
        stage_name = 'gcs_stage'
        # TODO: create stage per each table
        query = f'''create stage {stage_name}
          url = {GCS_LOCATIONS[0]} 
          storage_integration = {storage_integration_name}
          file_format = {named_file_format_name};'''

        print(f'running query: {query}')
        result = self._run_query(query)
        print(f'result {result}')
        return

    def _list_stage(self):
        pass

    def import_data(self):
        """ run import """
        query = f'copy into part from @h_1gb_stage;'
        print(f'running query: {query}')
        result = self._run_query(query)
        print(f'result {result}')
        return