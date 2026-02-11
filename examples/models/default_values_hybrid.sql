{#
    Example: DEFAULT Values
    Features: DEFAULT on integer, boolean, and timestamp columns
#}
{{ config(
    materialized='hybrid_table',
    column_definitions={
        'product_id': 'INT NOT NULL',
        'product_name': 'VARCHAR(200)',
        'category': 'VARCHAR(100)',
        'price': 'DECIMAL(10,2)',
        'stock_qty': 'INT DEFAULT 0',
        'is_active': 'BOOLEAN DEFAULT TRUE',
        'last_updated': 'TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()'
    },
    primary_key=['product_id'],
    indexes=[
        {'name': 'idx_category', 'columns': ['category']},
        {'name': 'idx_price', 'columns': ['price']}
    ],
    comment='Hybrid table with DEFAULT values'
) }}

SELECT
    category::VARCHAR(100) as category,
    TRUE as is_active,
    CURRENT_TIMESTAMP()::TIMESTAMP_NTZ as last_updated,
    price::DECIMAL(10,2) as price,
    product_id::INT as product_id,
    product_name::VARCHAR(200) as product_name,
    stock_qty::INT as stock_qty
FROM {{ ref('raw_products') }}
