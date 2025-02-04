WITH base AS (
    SELECT * FROM {{ ref('subscriptions') }}
),

retention AS (
    SELECT 
        b.customer_id,
        b.active_month,
        DATE_ADD(b.active_month, INTERVAL 1 MONTH) AS next_month,
        b.customer_country,
        b.business_category,
        -- Check if the customer has an active subscription in the next month
        CASE 
            WHEN EXISTS (
                SELECT 1 FROM base next_sub
                WHERE next_sub.customer_id = b.customer_id
                AND next_sub.active_month = DATE_ADD(b.active_month, INTERVAL 1 MONTH)
            ) THEN 1 ELSE 0 
        END AS is_retained_next_month
    FROM base b
)

SELECT 
    active_month,
    next_month,
    customer_id,
    customer_country,
    business_category,
    is_retained_next_month  -- 1 if retained next month, 0 if not
FROM retention