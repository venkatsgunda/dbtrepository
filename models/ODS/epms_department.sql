{% set unique_keys = ['ID', 'CLIENT_NAME', 'CLIENT_REGION'] %}

{{ config(
materialized='incremental',
unique_key=unique_keys,
incremental_strategy='merge',
merge_exclude_columns=['EDP_CREATED_TIMESTAMP', 'DBT_UPDATED_AT'],
cluster_by=['CLIENT_NAME', 'CLIENT_REGION'],
transient=false,
on_schema_change='append_new_columns'
) }}

{% if is_incremental() %}
{% do run_query(incremental_delete_prehook('snap_stg_epms_department', unique_keys)) %}
{% endif %}
-- Current state ODS table - Deletion handled by incremental_delete_prehook macro
-- This happens because we have 'D' records in the snapshot which need to be removed from the edp_ods table

-- Current state ODS table - Standardized incremental logic using dbt_updated_at
SELECT
-- Exclude technical tracking fields: LOAD_TS, ROW_STATUS_IND, DBT_VALID_FROM, DBT_VALID_TO (keep DBT_UPDATED_AT for incremental)
{{ dbt_utils.star(
from=ref('snap_stg_epms_department'),
except=["LOAD_TS", "ROW_STATUS_IND", "DBT_VALID_FROM", "DBT_VALID_TO"],
relation_alias='snapshot_data'
) }},
CURRENT_TIMESTAMP() AS EDP_CREATED_TIMESTAMP,
'EDP_ODS' AS DATA_LAYER
FROM {{ ref('snap_stg_epms_department') }} snapshot_data
WHERE snapshot_data.dbt_valid_to IS NULL -- Current records only (automatically handles deletes)
AND (snapshot_data.ROW_STATUS_IND IS NULL OR snapshot_data.ROW_STATUS_IND != 'D') -- Exclude soft-deleted records

{% if is_incremental() %}
-- Only process records that have been updated since last run
AND snapshot_data.dbt_updated_at > (SELECT MAX(dbt_updated_at) FROM {{ this }})
{% endif %}