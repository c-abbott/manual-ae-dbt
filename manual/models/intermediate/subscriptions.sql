WITH subscriptions AS (
    SELECT 
        s.customer_id,
        s.subscription_id,
        DATE_TRUNC(s.subscription_start_date, MONTH) AS active_month,
        DATE_TRUNC(s.subscription_end_date, MONTH) AS subscription_end_month,
        c.customer_country,
        b.business_category
    FROM {{ ref('stg_activity') }} s
    LEFT JOIN {{ ref('stg_customers') }} c ON s.customer_id = c.customer_id
    LEFT JOIN {{ ref('stg_acq_orders') }} b ON s.customer_id = b.customer_id
),

active_subscriptions AS (
    -- Get all months where the customer had an active subscription
    SELECT DISTINCT
        customer_id,
        active_month,
        subscription_end_month,
        customer_country,
        business_category
    FROM subscriptions
    WHERE active_month <= subscription_end_month
)

SELECT 
    customer_id,
    active_month,
    subscription_end_month,
    customer_country,
    business_category
FROM active_subscriptions
