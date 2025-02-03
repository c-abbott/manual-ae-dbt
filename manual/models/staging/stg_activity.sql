-- Deduplicate activity date
WITH activity_raw AS (
    SELECT 
        customer_id, 
        subscription_id, 
        CAST(from_date AS DATE) AS subscription_start_date,
        CAST(to_date AS DATE) AS subscription_end_date,
        ROW_NUMBER() OVER (
            PARTITION BY customer_id, subscription_id, from_date, to_date 
            ORDER BY from_date DESC
        ) AS row_num
    FROM {{ source('manual', 'activity') }}
)

SELECT 
    customer_id, 
    subscription_id, 
    subscription_start_date,
    subscription_end_date
FROM activity_raw
WHERE row_num = 1
