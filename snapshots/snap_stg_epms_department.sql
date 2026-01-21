{% snapshot snap_stg_epms_department %}

{{
    config(
        target_database='DEV_DB',
        target_schema='EDP_SNAPSHOT',
        unique_key=['ID','CLIENT_NAME','CLIENT_REGION'],
        strategy='timestamp',
        updated_at='AUDIT_TIMESTAMP',
        invalidate_hard_deletes=false
    )
}}

select *
from {{ ref('STG_EPMS_DEPARTMENT') }}

{% endsnapshot %}
