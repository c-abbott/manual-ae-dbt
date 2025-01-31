SELECT 
    customer_id, 
    customer_country
FROM
    {{ source('manual', 'customers') }}