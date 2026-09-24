-- ============================================================
-- Custom schema naming macro.
--
-- By default dbt prepends the target.schema to the custom schema
-- name (e.g., target=STAGING + custom=staging -> STAGING_staging).
--
-- This macro uses ONLY the custom schema name when one is provided,
-- so dbt creates objects in the correct schemas:
--   staging models -> schema "staging"
--   marts models   -> schema "mdm"
--
-- When no custom schema is provided, falls back to target.schema.
-- ============================================================

{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{% endmacro %}
