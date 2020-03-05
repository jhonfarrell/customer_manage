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
kpi_person as (
    select v1.emp_name, v1.emp_code, v1.curr_area, v1.position_name, v1.hire_date, v1.job_name, v1.job_id, v2.last_kpi_score, v2.last_kpi_area_range, v2.trend, v2.last_income_score, v2.last_six_income_score, v2.last_new_sign_score, v2.last_six_new_sign_score, v2.last_maintenance_score, v2.last_six_maintenance_score, v2.last_sale_manage_score, v2.last_six_sale_manage_score, v2.last_risk_manage_score, v2.last_six_risk_manage_score
    from
    (select emp_name, emp_code, curr_area, position_name, hire_date, job_name, job_id from emp_message) v1
    left join
        (
           select * from 
  (
    select user_no, last_kpi_score, last_kpi_area_range, trend, last_income_score, last_six_income_score, last_new_sign_score, last_six_new_sign_score, last_maintenance_score, last_six_maintenance_score, last_sale_manage_score, last_six_sale_manage_score, last_risk_manage_score, last_six_risk_manage_score, row_number() over (partition by user_no) r1 from dm_prod_sasp.rpt_kpi_person
  where inc_month = '202002'
  ) a
  where a.r1 = 1
     ) v2
     on v1.emp_code = v2.user_no
)