{#
    Example: Composite Primary Key
    Features: Multi-column PK, INDEX with INCLUDE columns
#}
{{ config(
    materialized='hybrid_table',
    column_definitions={
        'order_date': 'DATE NOT NULL',
        'order_id': 'INT NOT NULL',
        'customer_name': 'VARCHAR(200)',
        'order_total': 'DECIMAL(12,2)',
        'status': 'VARCHAR(50)'
    },
    primary_key=['order_date', 'order_id'],
    indexes=[
        {
            'name': 'idx_customer_with_total',
            'columns': ['customer_name'],
            'include': ['order_total', 'status']
        }
    ],
    comment='Hybrid table with composite primary key and index INCLUDE'
) }}

SELECT
    customer_name::VARCHAR(200) as customer_name,
    order_date::DATE as order_date,
    id::INT as order_id,
    order_total::DECIMAL(12,2) as order_total,
    status::VARCHAR(50) as status
FROM {{ ref('raw_orders') }}
