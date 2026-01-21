{% macro incremental_delete_prehook(snapshot_ref, unique_keys) %}
{% if is_incremental() %}
DELETE FROM {{ this }}
WHERE ({{ unique_keys | join(', ') }}) IN (
SELECT {{ unique_keys | join(', ') }}
FROM {{ ref(snapshot_ref) }}
WHERE ROW_STATUS_IND = 'D'
AND DBT_VALID_TO IS NULL
)
{% endif %}
{% endmacro %}