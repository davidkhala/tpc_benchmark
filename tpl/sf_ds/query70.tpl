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
 define DMS = random(1176,1224,uniform);
 define _LIMIT=100;
 SELECT   Sum(ss_net_profit) AS total_sum, 
         s_state, 
         s_county, 
         Rank() OVER ( partition BY s_state, s_county ORDER BY Sum(ss_net_profit) DESC nulls last) AS rank_within_parent
FROM     store_sales, 
         date_dim AS d1, 
         store 
WHERE    d1.d_month_seq BETWEEN [DMS] AND      [DMS]+11 
AND      d1.d_date_sk = ss_sold_date_sk 
AND      s_store_sk = ss_store_sk 
AND      s_state IN 
         ( 
                SELECT s_state 
                FROM   ( 
                                SELECT   s_state                                                              AS s_state,
                                         Rank() OVER ( partition BY s_state ORDER BY Sum(ss_net_profit) DESC) AS ranking
                                FROM     store_sales, 
                                         store, 
                                         date_dim 
                                WHERE    d_month_seq BETWEEN [DMS] AND      [DMS]+11 
                                AND      d_date_sk = ss_sold_date_sk 
                                AND      s_store_sk = ss_store_sk 
                                GROUP BY s_state ) AS tmp1 
                WHERE  ranking <= 5 ) 
GROUP BY s_state, 
         s_county 
ORDER BY s_state nulls last, 
         rank_within_parent nulls last
;