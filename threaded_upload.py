import logging
import threading
import time
import datetime
import snowflake.connector

SF_USERNAME = 'dauren'
SF_PASSWORD = '239nj8834uffe'
SF_ACCOUNT = 'wja13212'
SF_DATABASE = 'TEST_CONCURRENT_DS_10000GB'
STORAGE_INTEGRATION = 'gcs_ds_10000GB_integration'
GCS_LOCATION = 'gcs://tpc-benchmark-5947'
GCS_FILEPATH = ''
TABLES = [
    'customer_address', 'customer_demographics', 'ship_mode', 'time_dim', 'date_dim', 'reason', 'income_band', 'item',
    'store', 'call_center', 'customer', 'web_site', 'store_returns', 'household_demographics', 'web_page', 'promotion', 'catalog_page',
    'inventory', 'catalog_returns', 'web_returns', 'web_sales', 'catalog_sales', 'store_sales', 'warehouse',
]

def _extract_table_name_from_gcs_filepath(gcs_filepath):
    """ since list @stage returns all files for all tests, we need to match table name, size when importing data """

    # ignore debug files
    filename = gcs_filepath[len(GCS_LOCATION) + 1:]  # +1 is for slash
    if filename.startswith('_data_'):
        return False, None

    # different tests have different naming conventions
    # validate filename prefix (make sure it belongs to this test and size)
    gcs_prefix = f'{GCS_LOCATION}/ds_10000GB_'

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

    return True, gcs_filepath_table


def list_integration(conn):
    """ lists all files in GCS bucket """
    # select role
    query_text = f'USE ROLE ACCOUNTADMIN'
    logging.info(query_text)
    run_query(conn, query_text)

    # select proper database
    query_text = f'USE DATABASE {SF_DATABASE}'
    logging.info(query_text)
    run_query(conn, query_text)

    # run query on snowflake db
    count, rows = run_query(conn, f'list @{STORAGE_INTEGRATION}_stage;')

    # db for keeping cleaned up and sorted .dat filenames
    table_files_db = {}

    # cleanup results and sort files into buckets based on table name
    for row in rows:
        # extract gcs filepath from response
        gcs_filepath = row[0]

        # extract table name from file being processed
        matched, gcs_filepath_table = _extract_table_name_from_gcs_filepath(gcs_filepath)

        # skip files found in bucket which are not related to this test
        if not matched:
            continue

        # see which table this files belongs to and append to appropriate list
        is_found_table = False
        for table in TABLES:
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
            logging.error(f'unknown table!!!! {gcs_filepath}')

    logging.info(f'\n\n--done listing stage')
    return table_files_db

def run_query(conn, query):
    row_count = 0
    rows = []
    cs = conn.cursor()
    try:
        cs.execute(query)
        row_count = cs.rowcount
        rows = cs.fetchall()
    except Exception as ex:
        logging.error(f'Error running query """{query}""", error: {ex}')
    finally:
        cs.close()

    return row_count, rows


def upload(idx, table, files):
    logging.info("START thread %s: table (%s), filecount (%s)", idx, table, len(files))
    # open snowflake connection
    conn = snowflake.connector.Connect(user=SF_USERNAME, password=SF_PASSWORD, account=SF_ACCOUNT)

    # select role
    query_text = f'USE ROLE ACCOUNTADMIN'
    logging.info(f'thread {idx}: {query_text}')
    run_query(conn, query_text)

    # select proper warehouse
    query_text = f'CREATE OR REPLACE WAREHOUSE WH_{idx} WITH WAREHOUSE_SIZE="X-SMALL";'
    logging.info(f'thread {idx}: {query_text}')
    run_query(conn, query_text)
    query_text = f'ALTER WAREHOUSE WH_{idx} RESUME'
    logging.info(f'thread {idx}: {query_text}')
    run_query(conn, query_text)
    query_text = f'USE WAREHOUSE WH_{idx}'
    logging.info(f'thread {idx}: {query_text}')
    run_query(conn, query_text)

    # select proper database
    query_text = f'USE DATABASE {SF_DATABASE}'
    logging.info(f'thread {idx}: {query_text}')
    run_query(conn, query_text)

    # loop through gcs filepaths
    for gcs_filepath in sorted(files):
        logging.info(f'\tthread {idx}: [{datetime.datetime.now()}] importing file: {gcs_filepath}')
        query_text = (f"copy into {table} from '{gcs_filepath}'  storage_integration={STORAGE_INTEGRATION} file_format=(format_name=csv_file_format);")
        logging.info(query_text)
        run_query(conn, query_text)
        logging.info(f'\tthread {idx}: finished import file @ {datetime.datetime.now().time()}')

    query_text = f'ALTER WAREHOUSE WH_{idx} SUSPEND'
    logging.info(f'thread {idx}: {query_text}')
    run_query(conn, query_text)

    logging.info("END thread %s: (%s)", idx, table)
    return conn


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