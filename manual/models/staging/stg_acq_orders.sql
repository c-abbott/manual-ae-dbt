SELECT 
    customer_id, 
    taxonomy_business_category_group AS acquisition_category
FROM 
    {{ source('manual', 'acq_orders') }}