# TPC-H refresh query generator

# read all update/delete files into memory:

# list directory /home/vagrant/bq_snowflake_benchmark/ds/v2.11.0rc2/tools/upd
from os import listdir
from os.path import isfile, join

upd_dir = '/home/vagrant/bq_snowflake_benchmark/ds/v2.11.0rc2/tools/upd/'
onlyfiles = [f for f in listdir(upd_dir) if isfile(join(upd_dir, f))]
FILES = {}
SUFFIX = '_0.dat'
PREFIX = 's_'
SEPARATOR = '|'

# hash filenames to table names
for file in onlyfiles:
    name = file[:len(file) - len(SUFFIX)]
    if name.startswith(PREFIX):
        FILES[name] = file

# generate SQL statements
for table, filename in FILES.items():
    print(f'-- processing file {upd_dir + filename}')
    with open(upd_dir + filename) as f:
        # read all lines
        lines = f.readlines()
        lines = [x.strip() for x in lines]

        # read each line and split into tokens
        for line in lines:
            tokens = line.split(SEPARATOR)
            quoted_tokens = [f"'{token}'" for token in tokens]

            # trailing separator issue
            quoted_tokens = quoted_tokens[:len(quoted_tokens) - 1]

            sql_stmt = f"INSERT INTO {table} VALUES ({','.join(quoted_tokens)})"
            print(sql_stmt)