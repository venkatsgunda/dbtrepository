{{
    config(
        materialized='view',
        schema='BI'
    )
}}

select  ID,DEPARTMENT_NAME,EMPLOYEE_ID,DATE_OF_BIRTH,CLIENT_NAME,CLIENT_REGION from DEV_DB.DBT_SCHEMA.epms_department