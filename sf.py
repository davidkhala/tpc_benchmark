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

import config  # , poor_security


def open_connection(verbose=False):
    """Starts Snowflake warehouse and opens a connection.
    Reads values saved in config.py and poor_security.py

    Returns
    -------
    conn : Snowflake connector object
    """

    #     if verbose:
    #         print(f'using configuration: user:{poor_security.sf_username}')
    #         print(f'pass: {poor_security.sf_password}')
    #         print(f'account: {config.sf_account}')

    #     conn = snowflake.connector.connect(user=poor_security.sf_username,
    #                                        password=poor_security.sf_password,
    #                                        account=config.sf_account
    #                                        )

    conn = snowflake.connector.connect(
        user=config.sf_username,
        password=config.sf_password,
        account=config.sf_account)

    return conn


# Variables
TABLES_DS = [
    'customer_address', 'customer_demographics', 'ship_mode', 'time_dim', 'reason', 'income_band', 'item',
    'store', 'call_center', 'customer', 'web_site', 'store_returns', 'household_demographics', 'web_page', 'promotion',
    'catalog_page',
    'inventory', 'catalog_returns', 'web_returns', 'web_sales', 'catalog_sales', 'store_sales',
]
TABLE_DS_SKIP = ['date_dim']
TABLES_H = ['nation', 'lineitem', 'customer', 'orders', 'part', 'partsupp', 'region', 'supplier']

# DATASET_SIZES = ['1GB', '2GB', '100GB', '1000GB', '10000GB']

GCS_LOCATION = 'gcs://tpc-benchmark-5947'  # TODO: needs to go to config

# config.sf_role = 'ACCOUNTADMIN' # TODO: needs to go to config
# config.sf_role = 'SYSADMIN'

# storage_integration_name = 'gcs_storage_integration' #TODO: needs to go to config
# config.sf_named_file_format = 'csv_file_format'  # TODO: move to config

WAREHOUSE_TEST_PREFIX = 'test_concurrent_'


class SnowflakeHelper:
    """ manages snowflake db  """

    def __init__(self, test, scale, verbose=False):
        """"Snowflake connection Class

        Parameters
        ----------
        test : TPC test type, either 'h' or 'ds'
        scale : TPC scale factor in Gigabytes, 100, 1000 or 10000 expected
        """

        self.test = test  # TPC-DS or TPC-h as 'ds' or 'h'
        self.scale = scale  # TPC Scale factor in GB
        self.gcs_file_range = 96 # config.cpu_count  # auto detected in config.py

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
            # print(t1, t0)
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

    def warehouse_start(self, verbose=False):  # create_db=False, verbose=False):
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

        query_text = f'CREATE DATABASE IF NOT EXISTS {WAREHOUSE_TEST_PREFIX}{self.test}_{self.scale}'
        if verbose:
            print(f'running query: {query_text}')
            self.run_query(query_text)

        query_text = f'USE DATABASE {WAREHOUSE_TEST_PREFIX}{self.test}_{self.scale}'
        if verbose:
            print(f'running query: {query_text}')
        self.run_query(query_text)

    def select_database(self, verbose=False):
        query_text = f'USE DATABASE {WAREHOUSE_TEST_PREFIX}{self.test}_{self.scale}'
        if verbose:
            print(f'running query: {query_text}')
        self.run_query(query_text)

    def create_database(self, verbose=False):
        query_text = f'CREATE DATABASE IF NOT EXISTS {WAREHOUSE_TEST_PREFIX}{self.test}_{self.scale}'
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

    def threaded_upload():
        # get list of tables/files to upload from GCS to snowflake
        listConn = snowflake.connector.connect(user=SF_USERNAME, password=SF_PASSWORD, account=SF_ACCOUNT)
        db = list_integration(listConn)

        threads = []

        # load each table in a separate thread
        thread_idx = 0

        logging.info(f"total tables to process: {len(db.keys())}")

        for table, files in db.items():
            logging.info(f'processing table: {table}')
            chunks = [files[x:x + 5] for x in range(0, len(files), 5)]

            for chunk in chunks:
                t = threading.Thread(target=upload, args=(thread_idx, table, chunk), name=f'worker_{thread_idx}')
                t.start()
                threads.append(t)
                thread_idx += 1
                time.sleep(1)  # sleep 1 seconds before firing off another thread

        logging.info(f'total threads started: {thread_idx}')

        # wait for all threads to finish:
        for t in threads:
            if t.is_alive():
                logging.info(f'joining threads {t.getName()}')
                t.join()
            else:
                logging.info(f'thread is not alive {t.getName()}')

        listConn.close()
        logging.info("Main: all done")




    # log_column_names = ["test", "scale", "dataset",
    #                     "table", "status",
    #                     "t0", "t1",
    #                     "size_bytes", "job_id"]
    #
    #
    # def add_view(query_text, project, dataset):
    #     """Handle the unhelpful behavior of the Python DDL API and views:
    #     The API allows setting a default dataset but does not honor that
    #     attribute when creating views"""
    #
    #     pdt = project + "." + dataset + "."
    #     return query_text.replace(config.p_d_id, pdt)


    def parse_query_job(query_job_tuple, verbose=False):
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
        start_ts, end_ts, bytes_processed, row_count, cost, data_rows = query_job_tuple
        df = pd.DataFrame(data_rows)

        if verbose:
            print("Query Statistics")
            print("================")
            print("Total Billed Time: {}".format(end_ts-start_ts))
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

        return start_ts, end_ts, bytes_processed, row_count, cost, df


    def query_n(sf_helper, n, test, templates_dir, scale,
                #project, dataset,
                qual=None,
                dry_run=False, # use_cache=False,
                verbose=False, verbose_out=False):

        """Query Snowflake with a specific Nth query

        ...
        """

        assert test in config.tests, "'{}' not a TPC test".format(test)

        # generate query text
        if test == "ds":
            query_text = ds_setup.qgen_template(n=n,
                                                templates_dir=templates_dir,
                                                dialect="sqlserver_tpc",
                                                scale=scale,
                                                qual=qual,
                                                verbose=verbose,
                                                verbose_out=verbose_out)
        elif test == "h":
            query_text = h_setup.qgen_template(n=n,
                                               templates_dir=templates_dir,
                                               scale=scale,
                                               qual=qual,
                                               verbose=verbose,
                                               verbose_out=verbose_out)
        else:
            return None

        # clean query
        query_text = sf_helper.brute_force_clean_query(query_text)
        #logging.debug(f'query idx: {n}, q: "{query_text}"')

        # check if we're running a single query or a batch
        if query_text.count(";") > 1:
            batch = query_text.split(';')
            batch = [b.strip() for b in batch if len(b.strip()) != 0]
            #logging.debug(f'batch: {batch}')
            query_result = sf_helper.run_queries(batch)
        else:
            query_result = sf_helper.run_query(query_text)

        #logging.debug(f'results: {query_result}')

        (start, end, bytes_processed,
         rows_count, cost, df) = parse_query_job(query_job_tuple=query_result,
                                                 verbose=verbose)
        return n, query_text, start, end, bytes_processed, rows_count, cost, df


    def query_seq(desc, test, seq, templates_dir, scale,
                  project, dataset,
                  qual=None, save=False,
                  dry_run=False, use_cache=False,
                  verbose=False, verbose_iter=False, verbose_query=False):
        """Query Snowflake with TPC DS/H test sequence

        ...
        """

        assert test in config.tests, "'{}' not a TPC test".format(test)

        # connect to Snowflake
        sf_helper = sf.SnowflakeHelper(test, f'{scale}GB', config)
        sf_helper.cache = use_cache  # API default is True, here False
        # enable warehouse and target database based on "test" and "size"
        sf_helper.warehouse_start()

        # run all queries in sequence
        query_data = []
        df_out = pd.DataFrame(None)
        for n in seq:
            if verbose_iter:
                print("===============")
                print("START QUERY:", n)

            (n, query_text, t0, t1,
             bytes_processed, rows_count,
             cost, df) = query_n(sf_helper=sf_helper,
                                 n=n,
                                 test=test,
                                 templates_dir=templates_dir,
                                 scale=scale,
                                 qual=qual,
                                 #project=project,  # NA
                                 #dataset=dataset,  # NA
                                 dry_run=dry_run,
                                 #use_cache=use_cache, set above as sf_helper.cached
                                 verbose=verbose,
                                 verbose_out=False
                                 )
            _d = ["sf", test, scale, dataset, desc, n,
                  t0, t1, bytes_processed, "NA", "NA", cost]
            query_data.append(_d)

            df_out = pd.concat([df_out, df])

            if verbose_query:
                print()
                print("QUERY EXECUTED")
                print("==============")
                print(query_text)

            if verbose_iter:
                print("-" * 40)
                print("Total Billed Time: {}".format(t1-t0))
                print("Bytes Processed: {}".format(bytes_processed))
                print("Rows Processed: {}".format(rows_count))
                print("-" * 40)
                print("END QUERY:", n)
                print("=========")
                print()

        # set of columns to write to csv file
        columns = ["db", "test", "scale", "bq_dataset", "desc", "query_n",
                   "t0", "t1", "bytes_processed", "bytes_billed", "query_plan", "cost"]

        # write results to csv file
        utils.write_to_csv("sf", test, dataset, desc, columns, query_data, kind="query")
        if save:
            df_fp = utils.result_namer("sf", test, dataset, desc, kind="query")
            df_out.to_csv(df_fp, index=False)

        # suspend warehouse
        sf_helper.warehouse_suspend()
        return True


    def stream_p(sf_helper, p, test, templates_dir, scale,
                 #project, dataset,
                 qual=None,
                 #dry_run=False, use_cache=False,
                 verbose=False, verbose_out=False):
        """...
        """
        # TODO: Duplicate code
        assert test in config.tests, "'{}' not a TPC test".format(test)

        # generate query text
        if test == "ds":
            query_text = ds_setup.qgen_stream(p=p,
                                              templates_dir=templates_dir,
                                              dialect="sqlserver",
                                              scale=scale,
                                              qual=qual,
                                              verbose=verbose,
                                              verbose_out=verbose_out)

        elif test == "h":
            query_text = h_setup.qgen_stream(p=p,
                                             templates_dir=templates_dir,
                                             scale=scale,
                                             qual=qual,
                                             verbose=verbose,
                                             verbose_out=verbose_out)
        else:
            return None

        # brute force fix:
        query_text = sf_helper.brute_force_clean_query(query_text)
        #logging.debug(f'query idx: {n}, q: "{query_text}"')

        # with "stream" we always run in "batch" mode.
        # first split query stream on ";"
        batch = query_text.split(';')
        batch = [b.strip() for b in batch if len(b.strip()) != 0]

        # since dsqgen can use both semicolon and "go" for query termination: split on "go" also if this is a TPC-H test
        if test == "h":
            final_query_list = []
            for query in batch:
                if 'go' in query:
                    split_on_go = query.split('go')
                    for item in split_on_go:
                        if item.strip() != '':
                            final_query_list.append(item)
                else:
                    final_query_list.append(query)
            batch = final_query_list

        # run batch of queries
        logging.debug(f'batch_count: {len(batch)}')
        query_result = sf_helper.run_queries(batch)

        (start, end, bytes_received, rows, cost, df) = parse_query_job(query_job_tuple=query_result, verbose=verbose)

        return p, query_text, start, end, bytes_received, rows, cost, df


    def stream_seq(desc, test, seq, templates_dir, scale,
                   project, dataset,
                   qual=None,
                   dry_run=False, use_cache=False,
                   verbose=False, verbose_iter=False):
        """...
        """

        # connect to Snowflake
        sf_helper = sf.SnowflakeHelper(test, f'{scale}GB', config)

        # enable warehouse and target database based on "test" and "size"
        sf_helper.warehouse_start()

        stream_data = []
        for p in seq:
            (p, query_text, t0, t1,
             bytes_processed, rows_count,
             cost, df) = stream_p(sf_helper=sf_helper,
                                  p=p,
                                  test=test,
                                  templates_dir=templates_dir,
                                  scale=scale,
                                  #project=project,
                                  #dataset=dataset,
                                  qual=qual,
                                  #dry_run=dry_run,
                                  #use_cache=use_cache,
                                  verbose=verbose,
                                  verbose_out=False)
            # unpack results
            _s = ["sf", test, scale, dataset, desc, p,
                  t0, t1, "NA", "NA", "NA", cost]
            stream_data.append(_s)

            if verbose_iter:
                print("STREAM:", p)
                print("============")
                print("Total Billed Time: {}".format(t1-t0))
                print("Bytes Processed: {}".format(bytes_processed))
                print("Rows Processed: {}".format(rows_count))
                print("-" * 40)
                print()

        # set csv file columns
        columns = ["db", "test", "scale", "dataset", "desc", "stream_p",
                   "t0", "t1", "bytes_processed", "bytes_billed", "query_plan", "cost"]

        # write data to file
        utils.write_to_csv("sf", test, dataset, desc, columns, stream_data, kind="stream")

        # suspend warehouse
        sf_helper.warehouse_suspend()

        return True



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


    def threaded_upload(idx, table, files):
        # get list of tables/files to upload from GCS to snowflake
        listConn = snowflake.connector.connect(user=SF_USERNAME, password=SF_PASSWORD, account=SF_ACCOUNT)
        db = list_integration(listConn)

        threads = []

        # load each table in a separate thread
        thread_idx = 0

        logging.info(f"total tables to process: {len(db.keys())}")

        for table, files in db.items():
            logging.info(f'processing table: {table}')
            chunks = [files[x:x + 5] for x in range(0, len(files), 5)]

            for chunk in chunks:
                t = threading.Thread(target=upload, args=(thread_idx, table, chunk), name=f'worker_{thread_idx}')
                t.start()
                threads.append(t)
                thread_idx += 1
                time.sleep(1)  # sleep 1 seconds before firing off another thread

        logging.info(f'total threads started: {thread_idx}')

        # wait for all threads to finish:
        for t in threads:
            if t.is_alive():
                logging.info(f'joining threads {t.getName()}')
                t.join()
            else:
                logging.info(f'thread is not alive {t.getName()}')

        listConn.close()
        logging.info("Main: all done")