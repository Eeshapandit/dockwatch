{# Use the folder schema as is (staging, marts...) instead of dbt's default
   "analytics_staging" prefixing. Cleaner names for the dashboard. #}
{% macro generate_schema_name(custom_schema_name, node) -%}
    {{ custom_schema_name if custom_schema_name else target.schema }}
{%- endmacro %}
