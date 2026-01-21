-- on_schema_change options:
-- 'ignore' : ignore any changes
-- 'fail' : fail for any change
-- 'sync_all_columns' : sync any changes (can break reporting)
-- 'append_new_columns' : append new columns automatically, fail if remove or change type

{% macro stg_generic(raw_table, unique_key, order_col='AUDIT_TIMESTAMP', lookback_days=3) %}
{{ config(
materialized='incremental',
unique_key=unique_key,
incremental_strategy='delete+insert',
on_schema_change='append_new_columns',
schema='EDP_STG',
pre_hook="{% if is_incremental() %}TRUNCATE TABLE {{ this }}{% endif %}"
) }}

-- Smart 3-day lookback window implementation:
-- First run or full-refresh: Load ALL data from RAW (handles stale data)
-- Subsequent runs: Keep only last N days for CDC to snapshot
-- delete+insert strategy ensures clean state each run
-- Snapshots use hard_deletes='ignore' to preserve history

{% set source_relation = ref(raw_table) %}
{% set relation = adapter.get_relation(
database=source_relation.database,
schema=source_relation.schema,
identifier=source_relation.identifier
) %}

{% if relation is not none %}

with source_data as (
select *
from {{ ref(raw_table) }}
{% if is_incremental() %}
-- Incremental: Use lookback window (default 3 days)
where {{ order_col }} >= dateadd(day, -{{ lookback_days }}, current_timestamp())
{% else %}
-- First run or full-refresh: Load ALL available data
where 1=1
{% endif %}
),

ranked_data as (
select
*,
row_number() over (
partition by {{ unique_key | join(', ') }}
order by {{ order_col }} desc
) as rn
from source_data
)

select {{ select_exclude(ref(raw_table), ['rn']) }}
from ranked_data
where rn = 1

{% else %}
-- Skipping model: relation '{{ raw_table }}' not found in
-- {{ source_relation.database }}.{{ source_relation.schema }}
{% endif %}

{% endmacro %}