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
 define YEAR=random(1999,2001,uniform);
 define _LIMIT=100;
 define SELECTONE= text({"v1.i_category",1},{"v1.i_brand",1},{"v1.i_category, v1.i_brand",1},{"v1.s_store_name",1},{"v1.s_company_name",1},{"v1.s_store_name, v1.s_company_name",1},{"v1.i_category, v1.i_brand, v1.s_store_name, v1.s_company_name",1}); 
 define SELECTTWO= text({",v1.d_year",1},{",v1.d_year, v1.d_moy",1}); 
 define ORDERBY= text({"avg_monthly_sales",1},{"sum_sales",1},{"psum",1},{"nsum",1});

WITH v1 AS 
( 
         SELECT   i_category, 
                  i_brand, 
                  s_store_name, 
                  s_company_name, 
                  d_year, 
                  d_moy, 
                  Sum(ss_sales_price)                                                                                    sum_sales,
                  Avg(Sum(ss_sales_price)) OVER (partition BY i_category, i_brand, s_store_name, s_company_name, d_year) avg_monthly_sales,
                  Rank() OVER (partition BY i_category, i_brand, s_store_name, s_company_name ORDER BY d_year, d_moy)    rn
         FROM     item, 
                  store_sales, 
                  date_dim, 
                  store 
         WHERE    ss_item_sk = i_item_sk 
         AND      ss_sold_date_sk = d_date_sk 
         AND      ss_store_sk = s_store_sk 
         AND      ( 
                           d_year = [YEAR] 
                  OR       ( 
                                    d_year = [YEAR]-1 
                           AND      d_moy =12) 
                  OR       ( 
                                    d_year = [YEAR]+1 
                           AND      d_moy =1) ) 
         GROUP BY i_category, 
                  i_brand, 
                  s_store_name, 
                  s_company_name, 
                  d_year, 
                  d_moy), v2 AS 
( 
       SELECT [SELECTONE] [SELECTTWO] , 
              v1.avg_monthly_sales , 
              v1.sum_sales, 
              v1_lag.sum_sales  psum, 
              v1_lead.sum_sales nsum 
       FROM   v1, 
              v1 v1_lag, 
              v1 v1_lead 
       WHERE  v1.i_category = v1_lag.i_category 
       AND    v1.i_category = v1_lead.i_category 
       AND    v1.i_brand = v1_lag.i_brand 
       AND    v1.i_brand = v1_lead.i_brand 
       AND    v1.s_store_name = v1_lag.s_store_name 
       AND    v1.s_store_name = v1_lead.s_store_name 
       AND    v1.s_company_name = v1_lag.s_company_name 
       AND    v1.s_company_name = v1_lead.s_company_name 
       AND    v1.rn = v1_lag.rn  + 1 
       AND    v1.rn = v1_lead.rn - 1) [_LIMITA] 
SELECT   [_LIMITB] * 
FROM     v2 
WHERE    d_year = [YEAR] 
AND      avg_monthly_sales > 0 
AND 
         CASE 
                  WHEN avg_monthly_sales > 0 THEN abs(sum_sales - avg_monthly_sales) / avg_monthly_sales
                  ELSE NULL 
         END > 0.1 
ORDER BY sum_sales - avg_monthly_sales, 
         [ORDERBY] [_LIMITC];