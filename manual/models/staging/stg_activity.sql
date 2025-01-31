SELECT 
    customer_id, 
    subscription_id, 
    CAST(from_date AS DATE) AS subscription_start_date,
    CAST(to_date AS DATE) AS subscription_end_date
FROM
    {{ source('manual', 'activity') }}