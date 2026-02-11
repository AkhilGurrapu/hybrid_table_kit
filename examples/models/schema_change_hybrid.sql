{#
    Example: Schema Evolution with sync_all_columns
    Features: on_schema_change='sync_all_columns' to add/drop columns incrementally

    How to test schema evolution:
    1. Run with --full-refresh to create the initial table
    2. Add a new column to column_definitions and SELECT
    3. Run incrementally — the macro will ALTER TABLE ADD COLUMN
    4. Remove a column from column_definitions and SELECT
    5. Run incrementally — the macro will ALTER TABLE DROP COLUMN
    6. Change a column type → macro will error (requires --full-refresh)
#}
{{ config(
    materialized='hybrid_table',
    incremental_strategy='merge',
    on_schema_change='sync_all_columns',
    column_definitions={
        'order_id': 'INT NOT NULL',
        'customer_name': 'VARCHAR(200)',
        'order_total': 'DECIMAL(12,2)'
    },
    primary_key=['order_id'],
    comment='Hybrid table testing on_schema_change column evolution'
) }}

SELECT
    customer_name::VARCHAR(200) as customer_name,
    id::INT as order_id,
    order_total::DECIMAL(12,2) as order_total
FROM {{ ref('raw_orders') }}
