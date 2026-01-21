{{
    config(
        materialized='view',
        schema='BI'
    )
}}

select  ID,EMPLOYEE_NAME,DATE_OF_BIRTH,CLIENT_NAME,CLIENT_REGION from DEV_DB.DBT_SCHEMA.epms_employee 