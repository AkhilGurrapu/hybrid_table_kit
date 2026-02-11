{#
    Example: UNIQUE Constraint
    Features: Named UNIQUE constraint, secondary index
#}
{{ config(
    materialized='hybrid_table',
    column_definitions={
        'customer_id': 'INT NOT NULL',
        'customer_name': 'VARCHAR(200)',
        'email': 'VARCHAR(255) NOT NULL',
        'country': 'VARCHAR(100)',
        'created_date': 'DATE'
    },
    primary_key=['customer_id'],
    unique_constraints=[
        {'name': 'uq_customer_email', 'columns': ['email']}
    ],
    indexes=[
        {'name': 'idx_country', 'columns': ['country']}
    ],
    comment='Hybrid table with UNIQUE constraint on email'
) }}

SELECT
    country::VARCHAR(100) as country,
    created_date::DATE as created_date,
    customer_id::INT as customer_id,
    customer_name::VARCHAR(200) as customer_name,
    email::VARCHAR(255) as email
FROM {{ ref('raw_customers') }}
