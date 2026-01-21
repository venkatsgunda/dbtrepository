{% macro select_exclude(source_relation, exclude_cols) %}
{# Accepts either a string table name or a relation object #}

{% if source_relation is string -%}
{%- set relation = adapter.get_relation(
database=target.database,
schema=target.schema,
identifier=source_relation
) -%}
{% else -%}
{%- set relation = source_relation -%}
{% endif -%}

{% if relation is none -%}
{{ exceptions.raise_compiler_error(
"Relation '" ~ source_relation ~ "' not found in "
~ target.database ~ "." ~ target.schema
) }}
{% endif -%}

{%- set cols = adapter.get_columns_in_relation(relation) -%}
{%- set exclude_set = exclude_cols | map('upper') | list -%}

{{
cols
| map(attribute='name')
| reject('in', exclude_set)
| join(', ')
}}

{% endmacro %}