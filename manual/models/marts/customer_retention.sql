WITH subscriptions AS (
    SELECT 
        s.customer_id,
        s.subscription_id,
        s.subscription_start_date,
        s.subscription_end_date,
        c.customer_country,
        b.business_category
    FROM {{ ref('stg_activity') }} s
    LEFT JOIN {{ ref('stg_customers') }} c ON s.customer_id = c.customer_id
    LEFT JOIN {{ ref('stg_acq_orders') }} b ON s.customer_id = b.customer_id
),

cohorts AS (
    -- Assign each customer to their first-ever subscription start date (cohort)
    SELECT 
        customer_id,
        MIN(subscription_start_date) AS cohort_start_date
    FROM subscriptions
    GROUP BY customer_id
),

retention AS (
    SELECT DISTINCT  
        c.cohort_start_date AS cohort_month,
        DATE_ADD(c.cohort_start_date, INTERVAL 90 DAY) AS check_retention_date,
        s.customer_id,
        s.customer_country,
        s.business_category,
        -- Check if the customer is active at the 90-day mark
        CASE 
            WHEN EXISTS (
                SELECT 1 FROM subscriptions sub
                WHERE sub.customer_id = s.customer_id
                AND sub.subscription_start_date <= DATE_ADD(c.cohort_start_date, INTERVAL 90 DAY)
                AND sub.subscription_end_date >= DATE_ADD(c.cohort_start_date, INTERVAL 90 DAY)
            ) THEN 1 ELSE 0 
        END AS is_retained
    FROM cohorts c
    JOIN subscriptions s ON c.customer_id = s.customer_id
)

SELECT 
    cohort_month,
    check_retention_date,
    customer_id,
    customer_country,
    business_category,
    is_retained  -- 1 if retained at the 90-day mark, 0 if not
FROM retention