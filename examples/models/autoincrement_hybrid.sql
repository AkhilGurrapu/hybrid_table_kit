{#
    Example: AUTOINCREMENT / IDENTITY Column
    Features: Auto-generated PK, non-CTAS path, INSERT-only incremental

    When ALL primary key columns are AUTOINCREMENT, incremental runs use
    INSERT-only (no MERGE possible since PKs aren't in source data).
    Snowflake auto-generates the row_id values.
#}
{{ config(
    materialized='hybrid_table',
    column_definitions={
        'row_id': 'INT NOT NULL AUTOINCREMENT',
        'customer_name': 'VARCHAR(200)',
        'email': 'VARCHAR(200)',
        'country': 'VARCHAR(50)'
    },
    primary_key=['row_id'],
    comment='Hybrid table with AUTOINCREMENT primary key'
) }}

SELECT
    customer_name::VARCHAR(200) as customer_name,
    country::VARCHAR(50) as country,
    email::VARCHAR(200) as email
FROM {{ ref('raw_customers') }}
