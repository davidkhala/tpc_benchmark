--
-- Legal Notice 
-- 
-- This document and associated source code (the "Work") is a part of a 
-- benchmark specification maintained by the TPC. 
-- 
-- The TPC reserves all right, title, and interest to the Work as provided 
-- under U.S. and international laws, including without limitation all patent 
-- and trademark rights therein. 
-- 
-- No Warranty 
-- 
-- 1.1 TO THE MAXIMUM EXTENT PERMITTED BY APPLICABLE LAW, THE INFORMATION 
--     CONTAINED HEREIN IS PROVIDED "AS IS" AND WITH ALL FAULTS, AND THE 
--     AUTHORS AND DEVELOPERS OF THE WORK HEREBY DISCLAIM ALL OTHER 
--     WARRANTIES AND CONDITIONS, EITHER EXPRESS, IMPLIED OR STATUTORY, 
--     INCLUDING, BUT NOT LIMITED TO, ANY (IF ANY) IMPLIED WARRANTIES, 
--     DUTIES OR CONDITIONS OF MERCHANTABILITY, OF FITNESS FOR A PARTICULAR 
--     PURPOSE, OF ACCURACY OR COMPLETENESS OF RESPONSES, OF RESULTS, OF 
--     WORKMANLIKE EFFORT, OF LACK OF VIRUSES, AND OF LACK OF NEGLIGENCE. 
--     ALSO, THERE IS NO WARRANTY OR CONDITION OF TITLE, QUIET ENJOYMENT, 
--     QUIET POSSESSION, CORRESPONDENCE TO DESCRIPTION OR NON-INFRINGEMENT 
--     WITH REGARD TO THE WORK. 
-- 1.2 IN NO EVENT WILL ANY AUTHOR OR DEVELOPER OF THE WORK BE LIABLE TO 
--     ANY OTHER PARTY FOR ANY DAMAGES, INCLUDING BUT NOT LIMITED TO THE 
--     COST OF PROCURING SUBSTITUTE GOODS OR SERVICES, LOST PROFITS, LOSS 
--     OF USE, LOSS OF DATA, OR ANY INCIDENTAL, CONSEQUENTIAL, DIRECT, 
--     INDIRECT, OR SPECIAL DAMAGES WHETHER UNDER CONTRACT, TORT, WARRANTY,
--     OR OTHERWISE, ARISING IN ANY WAY OUT OF THIS OR ANY OTHER AGREEMENT 
--     RELATING TO THE WORK, WHETHER OR NOT SUCH AUTHOR OR DEVELOPER HAD 
--     ADVANCE NOTICE OF THE POSSIBILITY OF SUCH DAMAGES. 
-- 
-- Contributors:
-- 
 define YEAR= random(1998,2002, uniform); 
 define MONTH = random(1,7,uniform); 
 define CINDX = random(1,rowcount("categories"),uniform);
 define CATEGORY = distmember(categories,[CINDX],1);
 define CLASS = dist(distmember(categories,[CINDX],2),1,1); 
 define _LIMIT=100;
 
 WITH my_customers AS 
( 
                SELECT DISTINCT c_customer_sk , 
                                c_current_addr_sk 
                FROM            ( 
                                       SELECT cs_sold_date_sk     sold_date_sk, 
                                              cs_bill_customer_sk customer_sk, 
                                              cs_item_sk          item_sk 
                                       FROM   catalog_sales 
                                       UNION ALL 
                                       SELECT ws_sold_date_sk     sold_date_sk, 
                                              ws_bill_customer_sk customer_sk, 
                                              ws_item_sk          item_sk 
                                       FROM   web_sales ) cs_or_ws_sales, 
                                item, 
                                date_dim, 
                                customer 
                WHERE           sold_date_sk = d_date_sk 
                AND             item_sk = i_item_sk 
                AND             i_category = '[CATEGORY]' 
                AND             i_class = '[CLASS]' 
                AND             c_customer_sk = cs_or_ws_sales.customer_sk 
                AND             d_moy = [MONTH] 
                AND             d_year = [YEAR] ) , my_revenue AS 
( 
         SELECT   c_customer_sk, 
                  Sum(ss_ext_sales_price) AS revenue 
         FROM     my_customers, 
                  store_sales, 
                  customer_address, 
                  store, 
                  date_dim 
         WHERE    c_current_addr_sk = ca_address_sk 
         AND      ca_county = s_county 
         AND      ca_state = s_state 
         AND      ss_sold_date_sk = d_date_sk 
         AND      c_customer_sk = ss_customer_sk 
         AND      d_month_seq BETWEEN 
                                       ( 
                                       SELECT DISTINCT d_month_seq+1 
                                       FROM            date_dim 
                                       WHERE           d_year = [YEAR] 
                                       AND             d_moy = [MONTH]) 
         AND 
                  ( 
                                  SELECT DISTINCT d_month_seq+3 
                                  FROM            date_dim 
                                  WHERE           d_year = [YEAR] 
                                  AND             d_moy = [MONTH]) 
         GROUP BY c_customer_sk ) , segments AS 
( 
       SELECT Cast((revenue/50) AS INT) AS segment 
       FROM   my_revenue ) [_LIMITA] 
SELECT   [_LIMITB]    segment, 
         count(*)   AS num_customers, 
         segment*50 AS segment_base 
FROM     segments 
GROUP BY segment 
ORDER BY segment, 
         num_customers
[_LIMITC];
