create table s_purchase_lineitem
(
    plin_purchase_id integer not null,
    plin_line_number integer not null,
    plin_item_id char(16),
    plin_promotion_id char(16),
    plin_quantity integer,
    plin_sale_price numeric(7,2),
    plin_coupon_amt numeric(7,2),
    plin_comment char(100),
    primary key (plin_purchase_id)
);

create table s_purchase
(
    purc_purchase_id integer not null,
    purc_store_id char(16),
    purc_customer_id char(16),
    purc_purchase_date char(16),
    purc_purchase_time integer,
    purc_register_id integer,
    purc_clerk_id integer,
    purc_comment char(100),
    primary key (purc_purchase_id)
);

create table s_catalog_order
(
    cord_order_id integer not null,
    cord_bill_customer_id char(16),
    cord_ship_customer_id char(16),
    cord_order_date char(16),
    cord_order_time integer,
    cord_ship_mode_id char(16),
    cord_call_center_id char(16),
    cord_order_comments char(100),
    primary key (cord_order_id)
);

create table s_web_order
(
    word_order_id integer not null,
    word_bill_customer_id char(16),
    word_ship_customer_id char(16),
    word_order_date char(16),
    word_order_time integer,
    word_ship_mode_id char(16),
    word_web_site_id char(16),
    word_order_comments char(100),
    primary key (word_order_id)
);

create table s_catalog_order_lineitem
(
    clin_order_id integer not null,
    clin_line_number integer,
    clin_item_id char(16),
    clin_promotion_id char(16),
    clin_quantity integer,
    clin_sales_price numeric(7,2),
    clin_coupon_amt numeric(7,2),
    clin_warehouse_id char(16),
    clin_ship_date char(10),
    clin_catalog_number integer,
    clin_catalog_page_number integer,
    clin_ship_cost numeric(7,2),
    primary key (clin_order_id)
);

create table s_web_order_lineitem
(
    wlin_order_id integer not null,
    wlin_line_number integer,
    wlin_item_id char(16),
    wlin_promotion_id char(16),
    wlin_quantity integer,
    wlin_sales_price numeric(7,2),
    wlin_coupon_amt numeric(7,2),
    wlin_warehouse_id char(16),
    wlin_ship_date char(10),
    wlin_ship_cost numeric(7,2),
    wlin_web_page_id char(16),
    primary key (wlin_order_id)
);

create table s_store_returns
(
    sret_store_id char(16),
    sret_purchase_id char(16),
    sret_line_number integer,
    sret_item_id char(16),
    sret_customer_id char(16),
    sret_return_date char(10),
    sret_return_time char(10),
    sret_ticket_number char(20),
    sret_return_qty integer,
    sret_return_amount numeric(7,2),
    sret_return_tax numeric(7,2),
    sret_return_fee numeric(7,2),
    sret_return_ship_cost numeric(7,2),
    sret_refunded_cash numeric(7,2),
    sret_reversed_charge numeric(7,2),
    sret_store_credit numeric(7,2),
    sret_reason_id char(16)
);

create table s_catalog_returns
(
    cret_call_center_id char(16),
    cret_order_id integer,
    cret_line_number integer,
    cret_item_id char(16),
    cret_return_customer_id char(16),
    cret_refund_customer_id char(16),
    cret_return_date char(10),
    cret_return_time char(10),
    cret_return_qty integer,
    cret_return_amt numeric(7,2),
    cret_return_tax numeric(7,2),
    cret_return_fee numeric(7,2),
    cret_return_ship_cost numeric(7,2),
    cret_refunded_cash numeric(7,2),
    cret_reversed_charge numeric(7,2),
    cret_merchant_credit numeric(7,2),
    cret_reason_id char(16),
    cret_shipmode_id char(16),
    cret_catalog_page_id char(16),
    cret_warehouse_id char(16)
);

create table s_web_returns
(
    wret_web_page_id char(16),
    wret_order_id integer,
    wret_line_number integer,
    wret_item_id char(16),
    wret_return_customer_id char(16),
    wret_refund_customer_id char(16),
    wret_return_date char(10),
    wret_return_time char(10),
    wret_return_qty integer,
    wret_return_amt numeric(7,2),
    wret_return_tax numeric(7,2),
    wret_return_fee numeric(7,2),
    wret_return_ship_cost numeric(7,2),
    wret_refunded_cash numeric(7,2),
    wret_reversed_charge numeric(7,2),
    wret_account_credit numeric(7,2),
    wret_reason_id char(16)
);

create table s_inventory
(
    invn_warehouse_id char(16),
    invn_item_id char(16),
    invn_date char(10),
    invn_qty_on_hand integer
);

