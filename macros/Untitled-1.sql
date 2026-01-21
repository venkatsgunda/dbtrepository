{{
    config(
        materialized='incremental',
        unique_key='id'
    )
}}