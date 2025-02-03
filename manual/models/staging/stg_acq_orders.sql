SELECT 
    customer_id, 
    taxonomy_business_category_group AS business_category
FROM 
    {{ source('manual', 'acq_orders') }}