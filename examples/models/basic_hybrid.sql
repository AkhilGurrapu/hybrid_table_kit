{#
    Example: Basic Hybrid Table
    Features: Simple PK, secondary indexes, table comment, MERGE incremental
#}
{{ config(
    materialized='hybrid_table',
    column_definitions={
        'id': 'INT NOT NULL',
        'customer_name': 'VARCHAR(200)',
        'order_total': 'DECIMAL(12,2)',
        'order_date': 'DATE',
        'status': 'VARCHAR(50)',
        'updated_at': 'TIMESTAMP_NTZ'
    },
    primary_key=['id'],
    indexes=[
        {'name': 'idx_customer', 'columns': ['customer_name']},
        {'name': 'idx_date', 'columns': ['order_date']}
    ],
    comment='Basic hybrid table for orders'
) }}

SELECT
    customer_name::VARCHAR(200) as customer_name,
    id::INT as id,
    order_date::DATE as order_date,
    order_total::DECIMAL(12,2) as order_total,
    status::VARCHAR(50) as status,
    CURRENT_TIMESTAMP()::TIMESTAMP_NTZ as updated_at
FROM {{ ref('raw_orders') }}
