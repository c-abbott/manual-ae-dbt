WITH base AS (
    SELECT * FROM {{ ref('subscriptions') }}
),

customer_tenure AS (
    SELECT
        customer_id,
        customer_country,
        business_category,
        MIN(active_month) AS first_subscription_month,
        MAX(subscription_end_month) AS last_subscription_month,
        DATE_DIFF(MAX(subscription_end_month), MIN(active_month), MONTH) AS tenure_months
    FROM base
    GROUP BY customer_id, customer_country, business_category
),

average_tenure AS (
    SELECT 
        customer_country,
        business_category,
        COUNT(DISTINCT customer_id) AS customer_count,
        ROUND(AVG(tenure_months), 1) AS avg_tenure_months
    FROM customer_tenure
    GROUP BY customer_country, business_category
)

SELECT 
    customer_country,
    business_category,
    avg_tenure_months,
    customer_count
FROM average_tenure
