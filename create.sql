CREATE TABLE `dm_acsp.account_manager_kpi_score`(
`emp_name` string COMMENT '姓名',
`emp_code` string COMMENT '工号',
`area` string COMMENT '地区',
`position_name` string COMMENT '岗位',
`hire_date` string COMMENT '已加入公司时间',
`job_name` string COMMENT '销售星级',
`job_id` string COMMENT '岗位经验',
`last_kpi_score` string COMMENT '上月kpi得分',
`last_kpi_range` string COMMENT '上月kpi地区排名',
`trend` string COMMENT '趋势',
`last_income_score` string COMMENT '上月收入目标达成率得分',
`last_six_income_score` string COMMENT '过往六个月平均收入目标达成率得分',
`last_new_sign_score` string COMMENT '上月新签目标达成率得分',
`last_six_new_sign_score` string COMMENT '过往六个月平均新签目标达成率得分',
`last_maintenance_score` string COMMENT '上月维护目标达成得分',
`last_six_maintenance_score` string COMMENT '过往六个月维护目标达成率得分',
`last_sale_manage_score` string COMMENT '上月销售过程管理得分',
`last_six_sale_manage_score` string COMMENT '过往六个月销售过程管理得分平均分',
`last_risk_manage_score` string COMMENT '上月风险管理评价得分',
`last_six_risk_manage_score` string COMMENT '过往六个月风险管理评价得分平均分'
)
COMMENT ''
PARTITIONED BY (
`inc_month` string COMMENT '年月'
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\001'
LINES TERMINATED BY '\n'
STORED AS TEXTFILE;




CREATE TABLE `dm_acsp.account_manager_monthly_income`(
`emp_name` string COMMENT '姓名',
`emp_code` string COMMENT '工号',
`area` string COMMENT '地区',
`position_name` string COMMENT '岗位',
`hire_date` string COMMENT '已加入公司时间',
`job_name` string COMMENT '销售星级',
`job_id` string COMMENT '岗位经验',
`monthly_overall_income` string COMMENT '整体收入',
`monthly_overall_goal` string COMMENT '整体目标',
`monthly_overall_target_completion_rate` string COMMENT '整体目标收入达成率',
`monthly_overall_regional_ranking` string COMMENT '整体地区排名',
`monthly_new_income` string COMMENT '新签收入',
`monthly_new_goal` string COMMENT '新签目标',
`monthly_new_target_completion_rate` string COMMENT '新签目标收入达成率',
`monthly_new_regional_ranking` string COMMENT '新签地区排名',
`monthly_maint_income` string COMMENT '维护收入',
`monthly_maint_goal` string COMMENT '维护目标',
`monthly_maint_target_completion_rate` string COMMENT '维护目标收入达成率',
`monthly_maint_regional_ranking` string COMMENT '维护地区排名',
`business_performance_sign` string COMMENT '业务表现标签',
`customer_management_sign` string COMMENT '客户管理标签'
)
COMMENT ''
PARTITIONED BY (
`inc_month` string COMMENT '年月'
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\001'
LINES TERMINATED BY '\n'
STORED AS TEXTFILE;


CREATE TABLE `dm_acsp.account_manager_daily_income`(
`emp_name` string COMMENT '姓名',
`emp_code` string COMMENT '工号',
`area` string COMMENT '地区',
`position_name` string COMMENT '岗位',
`hire_date` string COMMENT '已加入公司时间',
`job_name` string COMMENT '销售星级',
`job_id` string COMMENT '岗位经验',
`daily_overall_income` string COMMENT '整体收入',
`daily_overall_goal` string COMMENT '整体目标',
`daily_overall_target_completion_rate` string COMMENT '整体目标收入达成率',
`daily_overall_regional_ranking` string COMMENT '整体地区排名',
`daily_new_income` string COMMENT '新签收入',
`daily_new_goal` string COMMENT '新签目标',
`daily_new_target_completion_rate` string COMMENT '新签目标收入达成率',
`daily_new_regional_ranking` string COMMENT '新签地区排名',
`daily_maint_income` string COMMENT '维护收入',
`daily_maint_goal` string COMMENT '维护目标',
`daily_maint_target_completion_rate` string COMMENT '维护目标收入达成率',
`daily_maint_regional_ranking` string COMMENT '维护地区排名'
)
COMMENT ''
PARTITIONED BY (
`inc_day` string COMMENT '年月日'
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\001'
LINES TERMINATED BY '\n'
STORED AS TEXTFILE;


CREATE TABLE `dm_acsp.account_manager_monthly_middle`(
`emp_name` string COMMENT '姓名',
`emp_code` string COMMENT '工号',
`area` string COMMENT '地区',
`position_name` string COMMENT '岗位',
`hire_date` string COMMENT '已加入公司时间',
`job_name` string COMMENT '销售星级',
`job_id` string COMMENT '岗位经验',
`monthly_overall_income` string COMMENT '整体收入',
`monthly_overall_goal` string COMMENT '整体目标',
`monthly_overall_target_completion_rate` string COMMENT '整体目标收入达成率',
`monthly_overall_regional_ranking` string COMMENT '整体地区排名',
`monthly_new_income` string COMMENT '新签收入',
`monthly_new_goal` string COMMENT '新签目标',
`monthly_new_target_completion_rate` string COMMENT '新签目标收入达成率',
`monthly_new_regional_ranking` string COMMENT '新签地区排名',
`monthly_maint_income` string COMMENT '维护收入',
`monthly_maint_goal` string COMMENT '维护目标',
`monthly_maint_target_completion_rate` string COMMENT '维护目标收入达成率',
`monthly_maint_regional_ranking` string COMMENT '维护地区排名'
)
COMMENT ''
PARTITIONED BY (
`inc_month` string COMMENT '年月'
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\001'
LINES TERMINATED BY '\n'
STORED AS TEXTFILE;