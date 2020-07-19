# TPC-H refresh query generator

# read all update/delete files into memory:
FILE_ORDERS = './h/2.18.0_rc2/dbgen/refresh/orders.tbl.u1.1'
FILE_LINEITEMS = './h/2.18.0_rc2/dbgen/refresh/lineitem.tbl.u1.1'
FILE_DELETE = './h/2.18.0_rc2/dbgen/refresh/delete.u1.1'
SEPARATOR = '|'
DB = {}
DELETE_LIST = []
# read orders file into DB
with open(FILE_ORDERS) as f:
    lines = f.readlines()
lines = [x.strip() for x in lines]
for line in lines:
    # 9|12658|O|129872.68|1998-07-06|4-NOT SPECIFIED|Clerk#000000248|0| blithely final packages cajole. regular waters are final requests. regular ac
    cols = line.split(SEPARATOR)
    order_id = int(cols[0])
    if order_id not in DB.keys():
        DB[order_id] = {
            'cols': cols,
            'lineitems': [],
            'delete': False
        }
print(f'-- loaded orders: {len(DB.keys())}')


# read lineitems file into DB and reconcile based on order_id
with open(FILE_LINEITEMS) as f:
    lines = f.readlines()
lines = [x.strip() for x in lines]
for line in lines:
    # 5996|101235|1236|6|41|50685.43|0.10|0.06|A|F|1994-08-17|1994-07-24|1994-09-01|DELIVER IN PERSON|FOB| print furiously except the special,
    cols = line.split(SEPARATOR)
    order_id = int(cols[0])
    if order_id not in DB.keys():
        print(f'can not find order [{order_id}] !!!')

    # add line item columns to order
    DB[order_id]['lineitems'].append(cols)

print(f'-- loaded line items')

# debug
# for order_id, order_data in DB.items():
#     print(f'{order_id}: {len(order_data["lineitems"])}')


# load delete file and validate that all delete IDs are present in DB
with open(FILE_DELETE) as f:
    lines = f.readlines()
lines = [x.strip() for x in lines]
for line in lines:
    order_id = int(line)
    if order_id in DB.keys():
        print(f'found order_id to delete that is in DB [{order_id}] !!!')
        DB[order_id]['delete'] = True
    DELETE_LIST.append(line)
# validate that IDs in DB are not to be deleted
for order_id, order_data in DB.items():
    if order_data["delete"]:
        print(f'found order_id [{order_id}] to be deleted in DB!!!')

print(f'-- loaded delete file')

sql_insert_lines = []
sql_delete_lines = []
# generate SQL query files
for order_id, order_data in DB.items():
    # generate INSERT ORDER record
    o_orderkey = str(order_id)
    o_cols = order_data['cols']
    o_custkey = o_cols[1]
    o_orderstatus = f"'{o_cols[2]}'"
    o_totalprice = o_cols[3]
    o_orderdate = f"to_date('{o_cols[4]}')"
    o_orderpriority = f"'{o_cols[5]}'"
    o_clerk = f"'{o_cols[6]}'"
    o_shippingpriority = o_cols[7]
    o_comment = f"'{o_cols[8]}'"
    values = (o_orderkey, o_custkey, o_orderstatus, o_totalprice, o_orderdate, o_orderpriority, o_clerk, o_shippingpriority, o_comment)

    insert_order_sql = f'insert into orders select {",".join(values)};'
    print(insert_order_sql)

    # generate INSERT LINEITEM records for a given order
    # 9|127857|5394|1|45|84818.25|0.09|0.05|N|O|1998-10-20|1998-09-10|1998-11-15|COLLECT COD|SHIP|es haggle blithely above the silent ac
    for lineitem in order_data['lineitems']:
        l_returnflag = f"'{lineitem[8]}'"
        l_linestatus = f"'{lineitem[9]}'"
        l_shipdate = f"to_date('{lineitem[10]}')"
        l_commitdate = f"to_date('{lineitem[11]}')"
        l_receiptdate = f"to_date('{lineitem[12]}')"
        l_shipinstruct = f"'{lineitem[13]}'"
        l_shipmode = f"'{lineitem[14]}'"
        l_comment = f"'{lineitem[15]}'"
        values = lineitem[:8] + [l_returnflag, l_linestatus, l_shipdate, l_commitdate, l_receiptdate, l_shipinstruct, l_shipmode, l_comment]

        insert_lineitem_sql = f'insert into lineitem select {",".join(values)};'

        print(f'{insert_lineitem_sql}')


# generate delete statements
for order_id_to_delete in DELETE_LIST:
    delete_order_sql = f'delete from orders where o_orderkey = {order_id_to_delete};'
    print(delete_order_sql)
    delete_lineitems_sql = f'delete from lineitem where l_orderkey = {order_id_to_delete};'
    print(delete_lineitems_sql)
