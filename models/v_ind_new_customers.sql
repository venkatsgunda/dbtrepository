{{
    config(
        materialized='table'
    )
}}

select * from DEV.sales.customer