set hive.execution.engine=tez;
with emp_codes as (
    select
        distinct substr(concat('00000000', mgr_emp_code),-8) as emp_code, mgr_emp_name, valid_date, customer_code
        from dm_share.cdh_cmdm_protocol_customer
        where mgr_emp_code != '' and valid_date != '' and customer_code != '' and valid_date is not null and customer_code is not null and mgr_emp_code is not null
),
distinct_emp as (
     select * from 
  (
    select emp_code, valid_date, customer_code, row_number() over (partition by emp_code) r1 from emp_codes
  ) a
  where a.r1 = 1
),
emp_message as (
    select v1.emp_name, v2.emp_code, v1.curr_area, v1.position_name, v1.hire_date, v1.job_name, v1.job_id, v2.valid_date, v2.customer_code
    from
    (
           select * from 
  (
    select emp_code,emp_name,curr_area,position_name,job_name,hire_date, job_id,row_number() over (partition by emp_code) r1 from gdl.tt_emp_info where inc_day = '20200215' and position_name = '客户经理' and curr_area is not null
  ) a
  where a.r1 = 1
     )v1
    join
    (select emp_code, valid_date, customer_code from distinct_emp) v2
    on v1.emp_code = v2.emp_code
),
-- customer_codes as (
--   select v1.emp_code, v2.customer_code, v2.valid_date 
--   from
--   (select emp_code from emp_message) v1
--   join
--   (select emp_code, customer_code, valid_date from distinct_emp) v2
--   on v1.emp_code = v2.emp_code
-- ),
-- distinct_customer_codes as (
--      select * from 
--   (
--     select emp_code,row_number() over (partition by emp_code) r1 from emp_message
--   ) a
--   where a.r1 = 1
-- ),
new_new_year_target as (
    select substr(concat('00000000', people_follow),-8) as emp_code, month_01, month_02, month_03, month_04, month_05, month_06, month_07, month_08, month_09, month_10, month_11, month_12, modify_tm from dm_sops.rpt_m_sasp_index_online_new_sign_info where people_follow != ''  
),
new_last_year_target as (
    select substr(concat('00000000', people_follow),-8) as emp_code, month_01, month_02, month_03, month_04, month_05, month_06, month_07, month_08, month_09, month_10, month_11, month_12, modify_tm from dm_sops.rpt_m_sasp_index_online_new_sign_info where people_follow != ''  and modify_tm like '2019%' 
),
maint_new_target as (
    select v1.emp_code, v2.month_01, v2.month_02, v2.month_03, v2.month_04, v2.month_05, v2.month_06, v2.month_07, v2.month_08, v2.month_09, v2.month_10, v2.month_11, v2.month_12
    from
    (select distinct substr(concat('00000000', people_follow),-8) as emp_code, business_account from ods_sasp.sasp_archives where inc_day = '20200215' and people_follow != '' and business_account != '') v1
    left join
    (
         select * from 
  (
    select cust_code, month_01, month_02, month_03, month_04, month_05, month_06, month_07, month_08, month_09, month_10, month_11, month_12, row_number() over (partition by cust_code order by modify_tm desc) r1 from dm_sops.rpt_m_sasp_index_online_maint_info where  cust_code != ''
  ) a
  where a.r1 = 1
    ) v2
    on v1.business_account = v2.cust_code
),
maint_last_target as (
    select v1.emp_code, v2.month_01, v2.month_02, v2.month_03, v2.month_04, v2.month_05, v2.month_06, v2.month_07, v2.month_08, v2.month_09, v2.month_10, v2.month_11, v2.month_12
    from
    (select distinct substr(concat('00000000', people_follow),-8) as emp_code, business_account from ods_sasp.sasp_archives where inc_day = '20200215' and people_follow != '' and business_account != '') v1
    left join
    (
         select * from 
  (
    select cust_code, month_01, month_02, month_03, month_04, month_05, month_06, month_07, month_08, month_09, month_10, month_11, month_12, row_number() over (partition by cust_code order by modify_tm desc) r1 from dm_sops.rpt_m_sasp_index_online_maint_info where  cust_code != '' and modify_tm like '2019%'
  ) a
  where a.r1 = 1
    ) v2
    on v1.business_account = v2.cust_code
),
month_target_01 as (
  select v1.emp_code, (v2.month_01 + v2.month_02 + v2.month_03 + v2.month_04 + v2.month_05 + v2.month_06 + v2.month_07 + v2.month_08 + v2.month_09 + v2.month_10 + v2.month_11 + v2.month_12) as new_month_target,(v3.month_01 + v3.month_02 + v3.month_03 + v3.month_04 + v3.month_05 + v3.month_06 + v3.month_07 + v3.month_08 + v3.month_09 + v3.month_10 + v3.month_11 + v3.month_12) as maint_month_target
  from
  (select emp_code from emp_message) v1
  left join
  (
         select * from 
  (
    select  emp_code, month_01, month_02, month_03, month_04, month_05, month_06, month_07, month_08, month_09, month_10, month_11, month_12, row_number() over (partition by emp_code order by modify_tm desc) r1 from new_last_year_target
  ) a
  where a.r1 = 1
    ) v2
  on v1.emp_code = v2.emp_code
    left join
    (select emp_code, sum(month_01) as month_01, sum(month_02) as month_02, sum(month_03) as month_03, sum(month_04) as month_04, sum(month_05) as month_05, sum(month_06) as month_06, sum(month_07) as month_07, sum(month_08) as month_08, sum(month_09) as month_09, sum(month_10) as month_10, sum(month_11) as month_11, sum(month_12) as month_12 from maint_last_target group by emp_code) v3
    on v1.emp_code = v3.emp_code
),
month_target_02 as (
  select v1.emp_code, v2.month_01 as new_month_target, v3.month_01 as maint_month_target
  from
  (select emp_code from emp_message) v1
  left join
  (
         select * from 
  (
    select emp_code, month_01, row_number() over (partition by emp_code order by modify_tm desc) r1 from new_new_year_target
  ) a
  where a.r1 = 1
    ) v2
  on v1.emp_code = v2.emp_code
    left join
    (select emp_code, sum(month_01) as month_01 from maint_new_target group by emp_code) v3
    on v1.emp_code = v3.emp_code
),
month_target_03 as (
  select v1.emp_code, (v2.month_01 + v2.month_02) as new_month_target, (v3.month_01 + v3.month_02) as maint_month_target
  from
 (select emp_code from emp_message) v1
  left join
    (
         select * from 
  (
    select emp_code, month_01, month_02, row_number() over (partition by emp_code order by modify_tm desc) r1 from new_new_year_target
  ) a
  where a.r1 = 1
    ) v2
  on v1.emp_code = v2.emp_code
   left join
     (select emp_code, sum(month_01) as month_01, sum(month_02) as month_02 from maint_new_target group by emp_code) v3
    on v1.emp_code = v3.emp_code
),
month_target_04 as (
  select v1.emp_code, (v2.month_01 + v2.month_02 + v2.month_03) as new_month_target, (v3.month_01 + v3.month_02 + v3.month_03) as maint_month_target
  from
  (select emp_code from emp_message) v1
  left join
  (
         select * from 
  (
    select emp_code, month_01, month_02, month_03, row_number() over (partition by emp_code order by modify_tm desc) r1 from new_new_year_target
  ) a
  where a.r1 = 1
    ) v2
  on v1.emp_code = v2.emp_code
   left join
     (select emp_code, sum(month_01) as month_01, sum(month_02) as month_02, sum(month_03) as month_03 from maint_new_target group by emp_code) v3
    on v1.emp_code = v3.emp_code
),
month_target_05 as (
  select v1.emp_code, (v2.month_01 + v2.month_02 + v2.month_03 + v2.month_04) as new_month_target, (v3.month_01 + v3.month_02 + v3.month_03 + v3.month_04) as maint_month_target
  from
  (select emp_code from emp_message) v1
  left join
   (
         select * from 
  (
   select emp_code, month_01, month_02, month_03, month_04, row_number() over (partition by emp_code order by modify_tm desc) r1 from new_new_year_target
  ) a
  where a.r1 = 1
    ) v2
  on v1.emp_code = v2.emp_code
   left join
     (select emp_code, sum(month_01) as month_01, sum(month_02) as month_02, sum(month_03) as month_03, sum(month_04) as month_04 from maint_new_target group by emp_code) v3
    on v1.emp_code = v3.emp_code
),
month_target_06 as (
  select v1.emp_code, (v2.month_01 + v2.month_02 + v2.month_03 + v2.month_04 + v2.month_05) as new_month_target, (v3.month_01 + v3.month_02 + v3.month_03 + v3.month_04 + v3.month_05) as maint_month_target
  from
  (select emp_code from emp_message) v1
  left join
   (
         select * from 
  (
  select emp_code, month_01, month_02, month_03, month_04, month_05, row_number() over (partition by emp_code order by modify_tm desc) r1 from new_new_year_target
  ) a
  where a.r1 = 1
    ) v2
  on v1.emp_code = v2.emp_code
   left join
     (select emp_code, sum(month_01) as month_01, sum(month_02) as month_02, sum(month_03) as month_03, sum(month_04) as month_04, sum(month_05) as month_05 from maint_new_target group by emp_code) v3
    on v1.emp_code = v3.emp_code
),
month_target_07 as (
  select v1.emp_code, (v2.month_01 + v2.month_02 + v2.month_03 + v2.month_04 + v2.month_05 + v2.month_06) as new_month_target, (v3.month_01 + v3.month_02 + v3.month_03 + v3.month_04 + v3.month_05 + v3.month_06) as maint_month_target
  from
     (select emp_code from emp_message) v1
  left join
    (
         select * from 
  (
  select emp_code, month_01, month_02, month_03, month_04, month_05, month_06, row_number() over (partition by emp_code order by modify_tm desc) r1 from new_new_year_target
  ) a
  where a.r1 = 1
    ) v2
  on v1.emp_code = v2.emp_code
   left join
     (select emp_code, sum(month_01) as month_01, sum(month_02) as month_02, sum(month_03) as month_03, sum(month_04) as month_04, sum(month_05) as month_05, sum(month_06) as month_06 from maint_new_target group by emp_code) v3
    on v1.emp_code = v3.emp_code
),
month_target_08 as (
  select v1.emp_code, (v2.month_01 + v2.month_02 + v2.month_03 + v2.month_04 + v2.month_05 + v2.month_06 + v2.month_07) as new_month_target, (v3.month_01 + v3.month_02 + v3.month_03 + v3.month_04 + v3.month_05 + v3.month_06 + v3.month_07) as maint_month_target
  from
     (select emp_code from emp_message) v1
  left join
    (
         select * from 
  (
  select emp_code, month_01, month_02, month_03, month_04, month_05, month_06, month_07, row_number() over (partition by emp_code order by modify_tm desc) r1 from new_new_year_target
  ) a
  where a.r1 = 1
    ) v2
  on v1.emp_code = v2.emp_code
   left join
     (select emp_code, sum(month_01) as month_01, sum(month_02) as month_02, sum(month_03) as month_03, sum(month_04) as month_04, sum(month_05) as month_05, sum(month_06) as month_06, sum(month_07) as month_07 from maint_new_target group by emp_code) v3
    on v1.emp_code = v3.emp_code
),
month_target_09 as (
  select v1.emp_code, (v2.month_01 + v2.month_02 + v2.month_03 + v2.month_04 + v2.month_05 + v2.month_06 + v2.month_07 + v2.month_08) as new_month_target, (v3.month_01 + v3.month_02 + v3.month_03 + v3.month_04 + v3.month_05 + v3.month_06 + v3.month_07 + v3.month_08) as maint_month_target
  from
     (select emp_code from emp_message) v1
  left join
    (
         select * from 
  (
  select emp_code, month_01, month_02, month_03, month_04, month_05, month_06, month_07, month_08, row_number() over (partition by emp_code order by modify_tm desc) r1 from new_new_year_target
  ) a
  where a.r1 = 1
    ) v2
  on v1.emp_code = v2.emp_code
   left join
     (select emp_code, sum(month_01) as month_01, sum(month_02) as month_02, sum(month_03) as month_03, sum(month_04) as month_04, sum(month_05) as month_05, sum(month_06) as month_06, sum(month_07) as month_07, sum(month_08) as month_08 from maint_new_target group by emp_code) v3
    on v1.emp_code = v3.emp_code
),
month_target_10 as (
  select v1.emp_code, (v2.month_01 + v2.month_02 + v2.month_03 + v2.month_04 + v2.month_05 + v2.month_06 + v2.month_07 + v2.month_08 + v2.month_09) as new_month_target, (v3.month_01 + v3.month_02 + v3.month_03 + v3.month_04 + v3.month_05 + v3.month_06 + v3.month_07 + v3.month_08 + v3.month_09) as maint_month_target
  from
     (select emp_code from emp_message) v1
  left join
    (
         select * from 
  (
  select emp_code, month_01, month_02, month_03, month_04, month_05, month_06, month_07, month_08, month_09, row_number() over (partition by emp_code order by modify_tm desc) r1 from new_new_year_target
  ) a
  where a.r1 = 1
    ) v2
  on v1.emp_code = v2.emp_code
   left join
     (select emp_code, sum(month_01) as month_01, sum(month_02) as month_02, sum(month_03) as month_03, sum(month_04) as month_04, sum(month_05) as month_05, sum(month_06) as month_06, sum(month_07) as month_07, sum(month_08) as month_08, sum(month_09) as month_09 from maint_new_target group by emp_code) v3
    on v1.emp_code = v3.emp_code
),
month_target_11 as (
  select v1.emp_code, (v2.month_01 + v2.month_02 + v2.month_03 + v2.month_04 + v2.month_05 + v2.month_06 + v2.month_07 + v2.month_08 + v2.month_09 + v2.month_10) as new_month_target, (v3.month_01 + v3.month_02 + v3.month_03 + v3.month_04 + v3.month_05 + v3.month_06 + v3.month_07 + v3.month_08 + v3.month_09 + v3.month_10) as maint_month_target
  from
     (select emp_code from emp_message) v1
  left join
    (
         select * from 
  (
  select emp_code, month_01, month_02, month_03, month_04, month_05, month_06, month_07, month_08, month_09, month_10, row_number() over (partition by emp_code order by modify_tm desc) r1 from new_new_year_target
  ) a
  where a.r1 = 1
    ) v2
  on v1.emp_code = v2.emp_code
   left join
     (select emp_code, sum(month_01) as month_01, sum(month_02) as month_02, sum(month_03) as month_03, sum(month_04) as month_04, sum(month_05) as month_05, sum(month_06) as month_06, sum(month_07) as month_07, sum(month_08) as month_08, sum(month_09) as month_09, sum(month_10) as month_10 from maint_new_target group by emp_code) v3
    on v1.emp_code = v3.emp_code
),
month_target_12 as (
  select v1.emp_code, (v2.month_01 + v2.month_02 + v2.month_03 + v2.month_04 + v2.month_05 + v2.month_06 + v2.month_07 + v2.month_08 + v2.month_09 + v2.month_10 + v2.month_11) as new_month_target, (v3.month_01 + v3.month_02 + v3.month_03 + v3.month_04 + v3.month_05 + v3.month_06 + v3.month_07 + v3.month_08 + v3.month_09 + v3.month_10 + v3.month_11) as maint_month_target
  from
     (select emp_code from emp_message) v1
  left join
    (
         select * from 
  (
  select emp_code, month_01, month_02, month_03, month_04, month_05, month_06, month_07, month_08, month_09, month_10, month_11, row_number() over (partition by emp_code order by modify_tm desc) r1 from new_new_year_target
  ) a
  where a.r1 = 1
    ) v2
  on v1.emp_code = v2.emp_code
   left join
     (select emp_code, sum(month_01) as month_01, sum(month_02) as month_02, sum(month_03) as month_03, sum(month_04) as month_04, sum(month_05) as month_05, sum(month_06) as month_06, sum(month_07) as month_07, sum(month_08) as month_08, sum(month_09) as month_09, sum(month_10) as month_10, sum(month_11) as month_11 from maint_new_target group by emp_code) v3
    on v1.emp_code = v3.emp_code
),

all_month as (
    select emp_code, if(new_month_target is null,'0',new_month_target) as new_month_target,if(maint_month_target is null,'0', maint_month_target) as maint_month_target , (if(new_month_target is null,'0',new_month_target) + if(maint_month_target is null,'0', maint_month_target)) as total_month_target, '01' as num from month_target_01
  union all
    select emp_code, if(new_month_target is null,'0',new_month_target) as new_month_target,if(maint_month_target is null,'0', maint_month_target) as maint_month_target , (if(new_month_target is null,'0',new_month_target) + if(maint_month_target is null,'0', maint_month_target)) as total_month_target, '02' as num from month_target_02
  union all
    select emp_code, if(new_month_target is null,'0',new_month_target) as new_month_target,if(maint_month_target is null,'0', maint_month_target) as maint_month_target , (if(new_month_target is null,'0',new_month_target) + if(maint_month_target is null,'0', maint_month_target)) as total_month_target, '03' as num from month_target_03
  union all
    select emp_code, if(new_month_target is null,'0',new_month_target) as new_month_target,if(maint_month_target is null,'0', maint_month_target) as maint_month_target , (if(new_month_target is null,'0',new_month_target) + if(maint_month_target is null,'0', maint_month_target)) as total_month_target, '04' as num from month_target_04
  union all
    select emp_code, if(new_month_target is null,'0',new_month_target) as new_month_target,if(maint_month_target is null,'0', maint_month_target) as maint_month_target , (if(new_month_target is null,'0',new_month_target) + if(maint_month_target is null,'0', maint_month_target)) as total_month_target, '05' as num from month_target_05
  union all
    select emp_code, if(new_month_target is null,'0',new_month_target) as new_month_target,if(maint_month_target is null,'0', maint_month_target) as maint_month_target , (if(new_month_target is null,'0',new_month_target) + if(maint_month_target is null,'0', maint_month_target)) as total_month_target, '06' as num from month_target_06
  union all
    select emp_code, if(new_month_target is null,'0',new_month_target) as new_month_target,if(maint_month_target is null,'0', maint_month_target) as maint_month_target , (if(new_month_target is null,'0',new_month_target) + if(maint_month_target is null,'0', maint_month_target)) as total_month_target, '07' as num from month_target_07
  union all
    select emp_code, if(new_month_target is null,'0',new_month_target) as new_month_target,if(maint_month_target is null,'0', maint_month_target) as maint_month_target , (if(new_month_target is null,'0',new_month_target) + if(maint_month_target is null,'0', maint_month_target)) as total_month_target, '08' as num from month_target_08
  union all
    select emp_code, if(new_month_target is null,'0',new_month_target) as new_month_target,if(maint_month_target is null,'0', maint_month_target) as maint_month_target , (if(new_month_target is null,'0',new_month_target) + if(maint_month_target is null,'0', maint_month_target)) as total_month_target, '09' as num from month_target_09
  union all
    select emp_code, if(new_month_target is null,'0',new_month_target) as new_month_target,if(maint_month_target is null,'0', maint_month_target) as maint_month_target , (if(new_month_target is null,'0',new_month_target) + if(maint_month_target is null,'0', maint_month_target)) as total_month_target, '10' as num from month_target_10
  union all
      select emp_code, if(new_month_target is null,'0',new_month_target) as new_month_target,if(maint_month_target is null,'0', maint_month_target) as maint_month_target , (if(new_month_target is null,'0',new_month_target) + if(maint_month_target is null,'0', maint_month_target)) as total_month_target, '11' as num from month_target_11
  union all
      select emp_code, if(new_month_target is null,'0',new_month_target) as new_month_target,if(maint_month_target is null,'0', maint_month_target) as maint_month_target , (if(new_month_target is null,'0',new_month_target) + if(maint_month_target is null,'0', maint_month_target)) as total_month_target, '12' as num from month_target_12
),
get_all_rmb_new as (
  select if(v1.all_item_rmb is null,'0',v1.all_item_rmb) as all_item_rmb, if(v1.serv_item_1 is null,'0',v1.serv_item_1) as serv_item_1, if(v1.fee_rebate_rmb is null,'0',v1.fee_rebate_rmb) as fee_rebate_rmb, if(v1.serv_rebate_rmb is null,'0', v1.serv_rebate_rmb) as serv_rebate_rmb, v2.customer_code, v2.emp_code, v2.valid_date
  from 
  (select emp_code, customer_code, valid_date from emp_message) v2
   left join
  (select all_item_rmb, serv_item_1, fee_rebate_rmb, serv_rebate_rmb, cust_code from dm_bie.fact_bie_sale_month_report where inc_month>'201901' and inc_month<'202002' ) v1
  on v2.customer_code = v1.cust_code
),
get_all_rmb_last as (
  select if(v1.all_item_rmb is null,'0',v1.all_item_rmb) as all_item_rmb, if(v1.serv_item_1 is null,'0',v1.serv_item_1) as serv_item_1, if(v1.fee_rebate_rmb is null,'0',v1.fee_rebate_rmb) as fee_rebate_rmb, if(v1.serv_rebate_rmb is null,'0', v1.serv_rebate_rmb) as serv_rebate_rmb, v2.customer_code, v2.emp_code, v2.valid_date
  from 
  (select emp_code, customer_code, valid_date from emp_message) v2
  left  join
  (select all_item_rmb, serv_item_1, fee_rebate_rmb, serv_rebate_rmb, cust_code from dm_bie.fact_bie_sale_month_report where inc_month>'201901' and inc_month<'202002' ) v1
  on v2.customer_code = v1.cust_code
),
get_all_sum_rmb_new as (
  select (sum(all_item_rmb)+sum(serv_item_1)-sum(fee_rebate_rmb)-sum(serv_rebate_rmb)) as rmb, customer_code, emp_code, valid_date from get_all_rmb_new  where valid_date is not null group by customer_code, emp_code, valid_date
  ),
get_all_sum_rmb_last as (
  select (sum(all_item_rmb)+sum(serv_item_1)-sum(fee_rebate_rmb)-sum(serv_rebate_rmb)) as rmb, customer_code, emp_code, valid_date from get_all_rmb_last where valid_date is not null group by customer_code ,emp_code, valid_date
  ),
get_all_sum_rmb_new_new_sign as (
  select sum(rmb) as new_sign_rmb, emp_code, '01' as num from get_all_sum_rmb_new where valid_date >= '2020-01-01' and valid_date is not null group by emp_code
  ),
get_all_sum_rmb_new_maint as (
  select sum(rmb) as maint_rmb, emp_code, '01' as num from get_all_sum_rmb_new where valid_date < '2020-01-01' and valid_date is not null group by emp_code
  ),
get_all_sum_rmb_last_new_sign as (
  select sum(rmb) as new_sign_rmb, emp_code, '01' as num from get_all_sum_rmb_last where valid_date >= '2020-01-01' and valid_date is not null group by emp_code
  ),
get_all_sum_rmb_last_maint as (
  select sum(rmb) as maint_rmb, emp_code, '01' as num from get_all_sum_rmb_last where valid_date < '2020-01-01' and valid_date is not null group by emp_code
  ),
get_all_new_sum_rmb as (
  select sum(rmb) as all_rmb, emp_code, '01' as num from get_all_sum_rmb_new where valid_date is not null group by emp_code
  ),
get_all_last_sum_rmb as (
  select sum(rmb) as all_rmb, emp_code, '01' as num from get_all_sum_rmb_last where valid_date is not null group by emp_code
  ),
monthly as (
  select v1.emp_code, v2.new_sign_rmb, v3.maint_rmb, v4.all_rmb
  from
  (select emp_code from emp_message) v1
  left join
  (
    select if(new_sign_rmb is null,'0',new_sign_rmb) as new_sign_rmb, emp_code from get_all_sum_rmb_new_new_sign where '02' != num
    union all
    select if(new_sign_rmb is null,'0',new_sign_rmb) as new_sign_rmb, emp_code from get_all_sum_rmb_last_new_sign where '02' = num
  ) v2
  on v1.emp_code = v2.emp_code
  left join
    (
    select if(maint_rmb is null,'0',maint_rmb) as maint_rmb, emp_code from get_all_sum_rmb_new_maint where '02' != num
    union all
    select if(maint_rmb is null,'0',maint_rmb) as maint_rmb, emp_code from get_all_sum_rmb_last_maint where '02' = num
  ) v3
  on v1.emp_code = v3.emp_code
  left join
      (
    select if(all_rmb is null,'0',all_rmb) as all_rmb, emp_code from get_all_new_sum_rmb where '02' != num
    union all
    select if(all_rmb is null,'0',all_rmb) as all_rmb, emp_code from get_all_last_sum_rmb where '02' = num
  ) v4
  on v1.emp_code = v4.emp_code
),
monthly_end as (
  select v1.emp_code, v1.new_sign_rmb, v1.maint_rmb, v1.all_rmb, v2.new_month_target, v2.maint_month_target, v2.total_month_target, (v1.new_sign_rmb/v2.new_month_target) as new_get, (v1.maint_rmb/v2.maint_month_target) as maint_get, (v1.all_rmb/v2.total_month_target) as all_get from
  (
    (
  select emp_code, new_sign_rmb, maint_rmb, all_rmb from monthly
  ) v1
  left join
  (select emp_code, new_month_target, maint_month_target, total_month_target from all_month where num = '04') v2
  on v1.emp_code = v2.emp_code
  )
),
total_message as (
  select v1.emp_name, v1.emp_code, v1.curr_area, v1.position_name, v1.hire_date, v1.job_name, v1.job_id, if(v2.new_sign_rmb is null,'0',v2.new_sign_rmb) as new_sign_rmb, if(v2.maint_rmb is null,'0',v2.maint_rmb) as maint_rmb, if(v2.all_rmb is null,'0',all_rmb) as all_rmb, v2.new_month_target, v2.maint_month_target, v2.total_month_target, if(v2.new_get is null,'0', v2.new_get) as new_get, if(v2.maint_get is null,'0', v2.maint_get) as maint_get, if(v2.all_get is null,'0', v2.all_get) as all_get
  from
  (
    (select emp_name, emp_code, curr_area, position_name, hire_date, job_name, job_id from emp_message) v1
    left join
    (select emp_code, new_sign_rmb, maint_rmb, all_rmb, new_month_target, maint_month_target, total_month_target, new_get, maint_get, all_get from monthly_end) v2
    on v1.emp_code = v2.emp_code
  )
),
base_message as(
  select emp_name, emp_code, curr_area, position_name, hire_date, job_name, job_id, new_sign_rmb, maint_rmb, all_rmb, new_month_target, maint_month_target, total_month_target, new_get, maint_get, all_get, rank() over(partition by curr_area order by new_get) new_rank, rank() over(partition by curr_area order by maint_get) maint_rank, rank() over(partition by curr_area order by all_get) all_rank
  from total_message
),
label_01 as (
    select count(emp_code) as person_num, curr_area
  from base_message group by curr_area
),
label_02_01 as (
    select person_num, curr_area, round(person_num * 0.1) as label_num from label_01 where person_num >= 5
    union all
    select person_num, curr_area, '1' as label_num from label_01 where person_num < 5
),
label_03 as (
  select v1.emp_name, v1.emp_code, v1.curr_area, v1.position_name, v1.hire_date, v1.job_name, v1.job_id, v1.new_sign_rmb, v1.maint_rmb, v1.all_rmb, v1.new_month_target, v1.maint_month_target, v1.total_month_target, v1.new_get, v1.maint_get, v1.all_get, v1.new_rank, v1.maint_rank, v1.all_rank, v2.label_num
  from 
  (select emp_name, emp_code, curr_area, position_name, hire_date, job_name, job_id, new_sign_rmb, maint_rmb, all_rmb, new_month_target, maint_month_target, total_month_target, new_get, maint_get, all_get, new_rank, maint_rank, all_rank
  from base_message) v1
  left join
  (select curr_area, label_num from label_02_01) v2
  on v1.curr_area = v2.curr_area
),
new_label as (
    select if(new_get >=1 and new_rank <= label_num, '新签王;',';') as label, emp_name, emp_code, curr_area, position_name, hire_date, job_name, job_id, new_sign_rmb, maint_rmb, all_rmb, new_month_target, maint_month_target, total_month_target, new_get, maint_get, all_get, new_rank, maint_rank, all_rank, label_num
    from 
    label_03
),
maint_label as (
    select if(maint_get >=1 and maint_rank <= label_num, concat(label,'客户维护能手;'),concat(label,';')) as label, emp_name, emp_code, curr_area, position_name, hire_date, job_name, job_id, new_sign_rmb, maint_rmb, all_rmb, new_month_target, maint_month_target, total_month_target, new_get, maint_get, all_get, new_rank, maint_rank, all_rank, label_num
    from 
    new_label
),
all_label as (
    select if(all_get >=1 and all_rank <= label_num, concat(label,'销售冠军;'),concat(label,';')) as label, emp_name, emp_code, curr_area, position_name, hire_date, job_name, job_id, new_sign_rmb, maint_rmb, all_rmb, new_month_target, maint_month_target, total_month_target, new_get, maint_get, all_get, new_rank, maint_rank, all_rank, label_num
    from 
    maint_label
),
risk_label as(
    select if(v2.last_risk_manage_score='100', concat(v1.label,'风险管理高手;'),concat(v1.label,';')) as label, v1.emp_name, v1.emp_code, v1.curr_area, v1.position_name, v1.hire_date, v1.job_name, v1.job_id, v1.new_sign_rmb, v1.maint_rmb, v1.all_rmb, v1.new_month_target, v1.maint_month_target, v1.total_month_target, v1.new_get, v1.maint_get, v1.all_get, v1.new_rank, v1.maint_rank, v1.all_rank, v1.label_num from
    (select label, emp_name, emp_code, curr_area, position_name, hire_date, job_name, job_id, new_sign_rmb, maint_rmb, all_rmb, new_month_target, maint_month_target, total_month_target, new_get, maint_get, all_get, new_rank, maint_rank, all_rank, label_num
    from
    all_label
    ) v1
    left join
    (
           select * from 
   (
    select user_no, last_risk_manage_score,row_number() over (partition by user_no) r1 from dm_prod_sasp.rpt_kpi_person where inc_month = '202002'
   ) a
  where a.r1 = 1
     )v2
     on v1.emp_code = v2.user_no
),
get_industry_new as (
    select v1.rmb, v1.customer_code, v1.emp_code, v1.valid_date, v2.first_classify
    FROM
    (select rmb, customer_code, emp_code, valid_date from get_all_sum_rmb_new) v1
    left join
    (select customer_code, first_classify from dm_bdp_wh.cust_fact_user_profile_summary_month) v2
    on v1.customer_code = v2.customer_code
),
group_classify_rmb_new as (
    select sum(rmb) as first_rmb, emp_code, first_classify from get_industry_new where first_classify is not null group by emp_code, first_classify
),
group_classify_new as (
    select first_rmb, emp_code, first_classify, row_number() over (partition by first_classify order by first_rmb) first_rank from group_classify_rmb_new
),
get_industry_tags_new as(
    select emp_code, concat_ws(',', collect_set(first_classify)) as industry_tags, '01' as num from group_classify_new where first_rank <3 group by emp_code
),
get_industry_last as (
    select v1.rmb, v1.customer_code, v1.emp_code, v1.valid_date, v2.first_classify
    FROM
    (select rmb, customer_code, emp_code, valid_date from get_all_sum_rmb_last) v1
    left join
    (select customer_code, first_classify from dm_bdp_wh.cust_fact_user_profile_summary_month) v2
    on v1.customer_code = v2.customer_code
),
group_classify_rmb_last as (
    select sum(rmb) as first_rmb, emp_code, first_classify from get_industry_last where first_classify is not null group by emp_code, first_classify
),
group_classify_last as (
    select first_rmb, emp_code, first_classify, row_number() over (partition by first_classify order by first_rmb) first_rank from group_classify_rmb_last
),
get_industry_tags_last as(
    select emp_code, concat_ws(',', collect_set(first_classify)) as industry_tags, '01' as num from group_classify_last where first_rank <3 group by emp_code
),
get_final_industry_tags as(
    select emp_code, industry_tags from get_industry_tags_new where '02' != num
    union all
    select emp_code, industry_tags from get_industry_tags_last where '02' = num
),
industry_label as (
     select concat('擅长', v2.industry_tags,'行业;') as cust_label, v1.label, v1.emp_name, v1.emp_code, v1.curr_area, v1.position_name, v1.hire_date, v1.job_name, v1.job_id, v1.new_sign_rmb, v1.maint_rmb, v1.all_rmb, v1.new_month_target, v1.maint_month_target, v1.total_month_target, v1.new_get, v1.maint_get, v1.all_get, v1.new_rank, v1.maint_rank, v1.all_rank, v1.label_num 
     from
    (select label, emp_name, emp_code, curr_area, position_name, hire_date, job_name, job_id, new_sign_rmb, maint_rmb, all_rmb, new_month_target, maint_month_target, total_month_target, new_get, maint_get, all_get, new_rank, maint_rank, all_rank, label_num
    from risk_label) v1
    left join
    (select emp_code, industry_tags from get_final_industry_tags) v2
    on v1.emp_code = v2.emp_code
),
experience_label as (
    select if(v2.layer_id='16659545' or v2.layer_id = '16659548' or v2.layer_id = '16659550', concat(v1.cust_label,'大客户管理经验;'),concat(v1.cust_label,';')) as cust_label, v1.label, v1.emp_name, v1.emp_code, v1.curr_area, v1.position_name, v1.hire_date, v1.job_name, v1.job_id, v1.new_sign_rmb, v1.maint_rmb, v1.all_rmb, v1.new_month_target, v1.maint_month_target, v1.total_month_target, v1.new_get, v1.maint_get, v1.all_get, v1.new_rank, v1.maint_rank, v1.all_rank, v1.label_num 
     from
    (select cust_label, label, emp_name, emp_code, curr_area, position_name, hire_date, job_name, job_id, new_sign_rmb, maint_rmb, all_rmb, new_month_target, maint_month_target, total_month_target, new_get, maint_get, all_get, new_rank, maint_rank, all_rank, label_num
    from industry_label) v1
    left join
    (
           select * from 
   (
    select substr(concat('00000000', mgr_emp_code),-8) as emp_code, layer_id, update_time, row_number() over (partition by mgr_emp_code order by update_time desc) r1 from ods_cdm.cdm_customer_manage
   ) a
  where a.r1 = 1
     ) v2
     on v1.emp_code = v2.emp_code
)
select * from experience_label