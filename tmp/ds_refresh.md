Method 1: Fact Table Load

for every row v in view V corresponding to fact table F
# V: crv
# F: catalog_returns
# SQL: select * from crv
# row: 
# CR_RETURN_DATE_SK	CR_RETURN_TIME_SK	CR_ITEM_SK	CR_REFUNDED_CUSTOMER_SK	CR_REFUNDED_CDEMO_SK	CR_REFUNDED_HDEMO_SK	CR_REFUNDED_ADDR_SK	CR_RETURNING_CUSTOMER_SK	CR_RETURNING_CDEMO_SK	CR_RETURNING_HDEMO_SK	CR_RETURING_ADDR_SK	CR_CALL_CENTER_SK	CR_CATALOG_PAGE_SK	CR_SHIP_MODE_SK	CR_WAREHOUSE_SK	CR_REASON_SK	CR_ORDER_NUMBER	CR_RETURN_QUANTITY	CR_RETURN_AMT	CR_RETURN_TAX	CR_RETURN_AMT_INC_TAX	CR_FEE	CR_RETURN_SHIP_COST	CR_REFUNDED_CASH	CR_REVERSED_CHARGE	CR_MERCHANT_CREDIT	CR_NET_LOSS
#	37852	2389	68339	1133591	673	356202	68339	1133591	673	356202	6	5	13		26	2	7	364.42	18.22	382.64	0.00	475.79	0.00	0.00	0.00	382.64

    get row v into local variable lv
    # SQL: 
    
    for every type 1 business key column bkc in v
        get row d from dimension table D corresponding to bkc
            where the business keys of v and d are equal
        update bkc of lv with surrogate key of d
    end for
 
    for every type 2 business key column bkc in v
        get row d from dimension table D corresponding to bkc
            where the business keys of v and d are equal and rec_end_date is NULL
        update bkc of lv with surrogate key of d
    end for
    
    insert lv into F

end for




