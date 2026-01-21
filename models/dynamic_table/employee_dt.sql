{{
    config(
        materialized='dynamic_table',
        target_lag='2 minutes',
        snowflake_warehouse='compute_wh',
        on_schema_change='sync_all_columns'
) }}

select
    emp.id as employee_id,
    dept.id as department_id,
    dept.department_name
from {{ ref('epms_employee') }} emp
join {{ ref('epms_department') }} dept
    on emp.id = dept.employee_id
