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
daily_rmb as (
    select v1.emp_code, v1.customer_code, v1.valid_date, (if(v2.fee_item_rmb is null,'0',v2.fee_item_rmb)+if(v2.serv_item_rmb is null,'0',v2.serv_item_rmb)) as cust_code_rmb
    from
    (select emp_code, customer_code, valid_date from emp_message) v1
    left join
    (select cust_code, fee_item_rmb, serv_item_rmb from dm_bie.sale_day_billing where inc_day >= '20191201' and inc_day < '20200114') v2
    on v1.customer_code = v2.cust_code
),
sum_daily_rmb as (
    select emp_code, customer_code, valid_date, sum(cust_code_rmb) as sum_cust_rmb from daily_rmb group by emp_code, customer_code, valid_date
),
get_month_rmb as (
  select if(v1.all_item_rmb is null, '0', v1.all_item_rmb) as all_item_rmb, if(v1.serv_item_1 is null, '0', v1.serv_item_1) as serv_item_1, if(v1.fee_rebate_rmb is null,'0', v1.fee_rebate_rmb) as fee_rebate_rmb, if(v1.serv_rebate_rmb is null,'0',v1.serv_rebate_rmb) as serv_rebate_rmb, v2.customer_code, v2.emp_code, v2.valid_date
  from 
  (select emp_code, customer_code, valid_date from emp_message) v2
  left  join
  (select all_item_rmb, serv_item_1, fee_rebate_rmb, serv_rebate_rmb, cust_code from dm_bie.fact_bie_sale_month_report where inc_month='201912' ) v1
  on v2.customer_code = v1.cust_code
),
month_discount as (
  select (sum(all_item_rmb)+sum(serv_item_1)-sum(fee_rebate_rmb)-sum(serv_rebate_rmb))/(sum(all_item_rmb)+sum(serv_item_1)) as discount, customer_code, emp_code, valid_date from get_month_rmb group by customer_code, emp_code, valid_date
),
month_discount_02 as (
    select if(discount!='',discount,1) as discount_02, customer_code, emp_code, valid_date from month_discount
),
daily_discount as (
    select if(v2.discount!='', v2.discount,1) as discount_02, v1.sum_cust_rmb, v1.customer_code, v1.emp_code, v2.valid_date from
    (select emp_code, customer_code, valid_date, sum_cust_rmb from sum_daily_rmb) v1
    left join
    (select discount, customer_code, emp_code, valid_date from month_discount) v2
    on v1.customer_code = v2.customer_code and v1.emp_code = v2.emp_code and v1.valid_date = v2.valid_date
),
daily_rmb_total as (
    select sum(discount_02 * sum_cust_rmb) as daily_sum_rmb, customer_code, emp_code, valid_date from daily_discount group by customer_code, emp_code, valid_date
),
daily_total as (
    select sum(daily_sum_rmb) as total_rmb, emp_code from daily_rmb_total where valid_date is not null group by emp_code
),
daily_rmb_new as (
    select sum(daily_sum_rmb) as new_rmb, emp_code from daily_rmb_total where valid_date >= '2020-01-01' and valid_date is not null group by emp_code
),
daily_rmb_maint as (
    select sum(daily_sum_rmb) as maint_rmb, emp_code from daily_rmb_total where valid_date < '2020-01-01' and valid_date is not null group by emp_code
),
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
  select v1.emp_code, v2.month_12 as new_month_target, v3.month_12 as maint_month_target
  from
  (select emp_code from emp_message) v1
  left join
  (
         select * from 
  (
    select emp_code, month_12, row_number() over (partition by emp_code order by modify_tm desc) r1 from new_last_year_target
  ) a
  where a.r1 = 1
    ) v2
  on v1.emp_code = v2.emp_code
    left join
    (select emp_code, sum(month_12) as month_12 from maint_last_target group by emp_code) v3
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
  select v1.emp_code, v2.month_02 as new_month_target, v3.month_02 as maint_month_target
  from
  (select emp_code from emp_message) v1
  left join
  (
         select * from 
  (
    select emp_code, month_02, row_number() over (partition by emp_code order by modify_tm desc) r1 from new_new_year_target
  ) a
  where a.r1 = 1
    ) v2
  on v1.emp_code = v2.emp_code
    left join
    (select emp_code, sum(month_02) as month_02 from maint_new_target group by emp_code) v3
    on v1.emp_code = v3.emp_code
),
month_target_04 as (
  select v1.emp_code, v2.month_03 as new_month_target, v3.month_03 as maint_month_target
  from
  (select emp_code from emp_message) v1
  left join
  (
         select * from 
  (
    select emp_code, month_03, row_number() over (partition by emp_code order by modify_tm desc) r1 from new_new_year_target
  ) a
  where a.r1 = 1
    ) v2
  on v1.emp_code = v2.emp_code
    left join
    (select emp_code, sum(month_03) as month_03 from maint_new_target group by emp_code) v3
    on v1.emp_code = v3.emp_code
),
month_target_05 as (
  select v1.emp_code, v2.month_04 as new_month_target, v3.month_04 as maint_month_target
  from
  (select emp_code from emp_message) v1
  left join
  (
         select * from 
  (
    select emp_code, month_04, row_number() over (partition by emp_code order by modify_tm desc) r1 from new_new_year_target
  ) a
  where a.r1 = 1
    ) v2
  on v1.emp_code = v2.emp_code
    left join
    (select emp_code, sum(month_04) as month_04 from maint_new_target group by emp_code) v3
    on v1.emp_code = v3.emp_code
),
month_target_06 as (
  select v1.emp_code, v2.month_05 as new_month_target, v3.month_05 as maint_month_target
  from
  (select emp_code from emp_message) v1
  left join
  (
         select * from 
  (
    select emp_code, month_05, row_number() over (partition by emp_code order by modify_tm desc) r1 from new_new_year_target
  ) a
  where a.r1 = 1
    ) v2
  on v1.emp_code = v2.emp_code
    left join
    (select emp_code, sum(month_05) as month_05 from maint_new_target group by emp_code) v3
    on v1.emp_code = v3.emp_code
),
month_target_07 as (
  select v1.emp_code, v2.month_06 as new_month_target, v3.month_06 as maint_month_target
  from
  (select emp_code from emp_message) v1
  left join
  (
         select * from 
  (
    select emp_code, month_06, row_number() over (partition by emp_code order by modify_tm desc) r1 from new_new_year_target
  ) a
  where a.r1 = 1
    ) v2
  on v1.emp_code = v2.emp_code
    left join
    (select emp_code, sum(month_06) as month_06 from maint_new_target group by emp_code) v3
    on v1.emp_code = v3.emp_code
),
month_target_08 as (
  select v1.emp_code, v2.month_07 as new_month_target, v3.month_07 as maint_month_target
  from
  (select emp_code from emp_message) v1
  left join
  (
         select * from 
  (
    select emp_code, month_07, row_number() over (partition by emp_code order by modify_tm desc) r1 from new_new_year_target
  ) a
  where a.r1 = 1
    ) v2
  on v1.emp_code = v2.emp_code
    left join
    (select emp_code, sum(month_07) as month_07 from maint_new_target group by emp_code) v3
    on v1.emp_code = v3.emp_code
),
month_target_09 as (
  select v1.emp_code, v2.month_08 as new_month_target, v3.month_08 as maint_month_target
  from
  (select emp_code from emp_message) v1
  left join
  (
         select * from 
  (
    select emp_code, month_08, row_number() over (partition by emp_code order by modify_tm desc) r1 from new_new_year_target
  ) a
  where a.r1 = 1
    ) v2
  on v1.emp_code = v2.emp_code
    left join
    (select emp_code, sum(month_08) as month_08 from maint_new_target group by emp_code) v3
    on v1.emp_code = v3.emp_code
),
month_target_10 as (
  select v1.emp_code, v2.month_09 as new_month_target, v3.month_09 as maint_month_target
  from
  (select emp_code from emp_message) v1
  left join
  (
         select * from 
  (
    select emp_code, month_09, row_number() over (partition by emp_code order by modify_tm desc) r1 from new_new_year_target
  ) a
  where a.r1 = 1
    ) v2
  on v1.emp_code = v2.emp_code
    left join
    (select emp_code, sum(month_09) as month_09 from maint_new_target group by emp_code) v3
    on v1.emp_code = v3.emp_code
),
month_target_11 as (
  select v1.emp_code, v2.month_10 as new_month_target, v3.month_10 as maint_month_target
  from
  (select emp_code from emp_message) v1
  left join
  (
         select * from 
  (
    select emp_code, month_10, row_number() over (partition by emp_code order by modify_tm desc) r1 from new_new_year_target
  ) a
  where a.r1 = 1
    ) v2
  on v1.emp_code = v2.emp_code
    left join
    (select emp_code, sum(month_10) as month_10 from maint_new_target group by emp_code) v3
    on v1.emp_code = v3.emp_code
),
month_target_12 as (
  select v1.emp_code, v2.month_11 as new_month_target, v3.month_11 as maint_month_target
  from
  (select emp_code from emp_message) v1
  left join
  (
         select * from 
  (
    select emp_code, month_11, row_number() over (partition by emp_code order by modify_tm desc) r1 from new_new_year_target
  ) a
  where a.r1 = 1
    ) v2
  on v1.emp_code = v2.emp_code
    left join
    (select emp_code, sum(month_11) as month_11 from maint_new_target group by emp_code) v3
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
get_dailys_message as (
    select v1.emp_name, v1.emp_code, v1.curr_area, v1.position_name, v1.hire_date, v1.job_name, v1.job_id, if(v2.total_rmb is null,'0',v2.total_rmb) as total_rmb, if(v3.new_rmb is null,'0',v3.new_rmb) as new_rmb, if(v4.maint_rmb is null,'0',v4.maint_rmb) as maint_rmb, if(v5.new_month_target is null,'0',v5.new_month_target) as new_month_target, if(v5.maint_month_target is null,'0',v5.maint_month_target) as maint_month_target, if(v5.total_month_target is null,'0',v5.total_month_target) as total_month_target, (datediff(CURRENT_DATE,'2020-02-01')+2) as datedays, (datediff(last_day(DATE_SUB(CURRENT_DATE, 1)),'2020-02-01')+1) as monthdays
    from
    (select emp_name, emp_code, curr_area, position_name, hire_date, job_name, job_id from emp_message) v1
    left join
    (select if(total_rmb is null,'0',total_rmb) as total_rmb, emp_code from daily_total) v2
    on v1.emp_code = v2.emp_code
    left join
    (select if(new_rmb is null,'0',new_rmb) as new_rmb, emp_code from daily_rmb_new) v3
    on v1.emp_code = v3.emp_code
    left join
    (select if(maint_rmb is null,'0',maint_rmb) as maint_rmb, emp_code from daily_rmb_maint) v4
    on v1.emp_code = v4.emp_code
    left join
    (select emp_code, new_month_target, maint_month_target, total_month_target from all_month where num = '12') v5
    on v1.emp_code = v5.emp_code
),
daily_revenue_completion_rate as (
    select emp_name, emp_code, curr_area, position_name, hire_date, job_name, job_id, total_rmb, new_rmb, maint_rmb, new_month_target, maint_month_target, total_month_target, (total_rmb*total_month_target)/(datedays*monthdays) as total_completion_rate, (new_rmb*new_month_target)/(datedays*monthdays) as new_completion_rate, (maint_rmb*maint_month_target)/(datedays*monthdays) as last_completion_rate
    from 
    get_dailys_message
),

final as (
    select emp_name, emp_code, curr_area, position_name, hire_date, job_name, job_id, total_rmb, new_rmb, maint_rmb, new_month_target, maint_month_target, total_month_target, total_completion_rate, new_completion_rate, last_completion_rate, rank() over(partition by curr_area order by new_completion_rate) new_rank, rank() over(partition by curr_area order by last_completion_rate) maint_rank, rank() over(partition by curr_area order by total_completion_rate) all_rank
    from
     daily_revenue_completion_rate
)
select * from final
