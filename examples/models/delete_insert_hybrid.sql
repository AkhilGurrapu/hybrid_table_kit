{#
    Example: delete+insert Incremental Strategy
    Features: delete+insert strategy, on_schema_change='sync_all_columns'

    delete+insert first DELETEs rows matching source PKs, then INSERTs all source rows.
    Useful when you want to fully replace matched rows rather than selectively update columns.
#}
{{ config(
    materialized='hybrid_table',
    incremental_strategy='delete+insert',
    on_schema_change='sync_all_columns',
    column_definitions={
        'order_id': 'INT NOT NULL',
        'customer_name': 'VARCHAR(200)',
        'updated_at': 'TIMESTAMP_NTZ',
        'priority': 'VARCHAR(20)'
    },
    primary_key=['order_id'],
    comment='Hybrid table using delete+insert incremental strategy'
) }}

SELECT
    customer_name::VARCHAR(200) as customer_name,
    id::INT as order_id,
    'normal'::VARCHAR(20) as priority,
    CURRENT_TIMESTAMP()::TIMESTAMP_NTZ as updated_at
FROM {{ ref('raw_orders') }}
