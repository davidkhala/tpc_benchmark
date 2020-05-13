import logging
import threading
import time
import snowflake.connector

SF_USERNAME = 'dauren'
SF_PASSWORD = '239nj8834uffe'
SF_ACCOUNT = 'wja13212'
SF_DATABASE = 'TEST_CONCURRENT_DS_100GB'
STORAGE_INTEGRATION = ''
GCS_FILEPATH = ''

def run_query(conn, query):
    row_count = 0
    rows = []
    cs = conn.cursor()
    try:
        cs.execute(query)
        row_count = cs.rowcount
        rows = cs.fetchall()
    except Exception as ex:
        print(f'Error running query """{query}""", error: {ex}')
    finally:
        cs.close()

    return row_count, rows


def upload(idx, table):
    logging.info("START thread %s: table (%s)", idx, table)
    # open snowflake connection
    conn = snowflake.connector.connect(user=SF_USERNAME, password=SF_PASSWORD, account=SF_ACCOUNT)

    # select role
    query_text = f'USE ROLE ACCOUNTADMIN'
    print(query_text)
    run_query(conn, query_text)

    # select proper warehouse
    query_text = f'ALTER WAREHOUSE WH_{idx} RESUME'
    print(query_text)
    run_query(conn, query_text)
    query_text = f'USE WAREHOUSE WH_{idx}'
    print(query_text)
    run_query(conn, query_text)

    # select proper database
    query_text = f'USE DATABASE {SF_DATABASE}'
    print(query_text)
    run_query(conn, query_text)



    # close connection
    conn.close()
    logging.info("END thread %s: (%s)", idx, table)

if __name__ == "__main__":
    format = "%(asctime)s: %(message)s"
    logging.basicConfig(format=format, level=logging.INFO,
                        datefmt="%H:%M:%S")

    logging.info("Main    : before creating thread")
    x = threading.Thread(target=upload, args=(1, "hi"))
    y = threading.Thread(target=upload, args=(2, "2hi"))
    z = threading.Thread(target=upload, args=(3, "3hi"))

    logging.info("Main    : before running threads")
    x.start()
    y.start()
    z.start()

    logging.info("Main    : wait for the thread to finish")
    # x.join()
    logging.info("Main    : all done")