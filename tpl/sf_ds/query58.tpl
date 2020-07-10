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
 define YEAR = random(1998, 2002, uniform); 
 define SALES_DATE=date([YEAR]+"-01-01",[YEAR]+"-07-24",sales); 
 define _LIMIT=100;
 
WITH ss_items AS 
( 
         SELECT   i_item_id               item_id , 
                  Sum(ss_ext_sales_price) ss_item_rev 
         FROM     store_sales , 
                  item , 
                  date_dim 
         WHERE    ss_item_sk = i_item_sk 
         AND      d_date IN 
                  ( 
                         SELECT d_date 
                         FROM   date_dim 
                         WHERE  d_week_seq = 
                                ( 
                                       SELECT d_week_seq 
                                       FROM   date_dim 
                                       WHERE  d_date = '[SALES_DATE]')) 
         AND      ss_sold_date_sk = d_date_sk 
         GROUP BY i_item_id), cs_items AS 
( 
         SELECT   i_item_id               item_id , 
                  Sum(cs_ext_sales_price) cs_item_rev 
         FROM     catalog_sales , 
                  item , 
                  date_dim 
         WHERE    cs_item_sk = i_item_sk 
         AND      d_date IN 
                  ( 
                         SELECT d_date 
                         FROM   date_dim 
                         WHERE  d_week_seq = 
                                ( 
                                       SELECT d_week_seq 
                                       FROM   date_dim 
                                       WHERE  d_date = '[SALES_DATE]')) 
         AND      cs_sold_date_sk = d_date_sk 
         GROUP BY i_item_id), ws_items AS 
( 
         SELECT   i_item_id               item_id , 
                  Sum(ws_ext_sales_price) ws_item_rev 
         FROM     web_sales , 
                  item , 
                  date_dim 
         WHERE    ws_item_sk = i_item_sk 
         AND      d_date IN 
                  ( 
                         SELECT d_date 
                         FROM   date_dim 
                         WHERE  d_week_seq = 
                                ( 
                                       SELECT d_week_seq 
                                       FROM   date_dim 
                                       WHERE  d_date = '[SALES_DATE]')) 
         AND      ws_sold_date_sk = d_date_sk 
         GROUP BY i_item_id) [_LIMITA] 
SELECT   [_LIMITB] ss_items.item_id , 
         ss_item_rev , 
         ss_item_rev/((ss_item_rev+cs_item_rev+ws_item_rev)/3) * 100 ss_dev , 
         cs_item_rev , 
         cs_item_rev/((ss_item_rev+cs_item_rev+ws_item_rev)/3) * 100 cs_dev , 
         ws_item_rev , 
         ws_item_rev/((ss_item_rev+cs_item_rev+ws_item_rev)/3) * 100 ws_dev , 
         (ss_item_rev+cs_item_rev+ws_item_rev)/3                     average 
FROM     ss_items, 
         cs_items, 
         ws_items 
WHERE    ss_items.item_id=cs_items.item_id 
AND      ss_items.item_id=ws_items.item_id 
AND      ss_item_rev BETWEEN 0.9 * cs_item_rev AND      1.1 * cs_item_rev 
AND      ss_item_rev BETWEEN 0.9 * ws_item_rev AND      1.1 * ws_item_rev 
AND      cs_item_rev BETWEEN 0.9 * ss_item_rev AND      1.1 * ss_item_rev 
AND      cs_item_rev BETWEEN 0.9 * ws_item_rev AND      1.1 * ws_item_rev 
AND      ws_item_rev BETWEEN 0.9 * ss_item_rev AND      1.1 * ss_item_rev 
AND      ws_item_rev BETWEEN 0.9 * cs_item_rev AND      1.1 * cs_item_rev 
ORDER BY item_id , 
         ss_item_rev [_LIMITC];