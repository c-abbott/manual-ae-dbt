WITH base AS (
    SELECT * FROM {{ ref('subscriptions') }}
),

cohorts AS (
    -- Assign each customer to their first-ever subscription start date (cohort)
    SELECT 
        customer_id,
        MIN(active_month) AS cohort_month
    FROM base
    GROUP BY customer_id
),

retention AS (
    SELECT DISTINCT  
        c.cohort_month,
        DATE_ADD(c.cohort_month, INTERVAL 60 DAY) AS check_retention_date,
        b.customer_id,
        b.customer_country,
        b.business_category,
        -- Check if the customer is active at the 60-day mark
        CASE 
            WHEN EXISTS (
                SELECT 1 FROM base sub
                WHERE sub.customer_id = b.customer_id
                AND sub.active_month <= DATE_ADD(c.cohort_month, INTERVAL 60 DAY)
                AND sub.subscription_end_month >= DATE_ADD(c.cohort_month, INTERVAL 60 DAY)
            ) THEN 1 ELSE 0 
        END AS is_retained_60_day,
         -- Check if the customer is active at the 6 month mark
        CASE 
            WHEN EXISTS (
                SELECT 1 FROM base sub
                WHERE sub.customer_id = b.customer_id
                AND sub.active_month <= DATE_ADD(c.cohort_month, INTERVAL 6 MONTH)
                AND sub.subscription_end_month >= DATE_ADD(c.cohort_month, INTERVAL 6 MONTH)
            ) THEN 1 ELSE 0 
        END AS is_retained_6_month,
        -- Check if the customer is active at the 1  year mark
        CASE 
            WHEN EXISTS (
                SELECT 1 FROM base sub
                WHERE sub.customer_id = b.customer_id
                AND sub.active_month <= DATE_ADD(c.cohort_month, INTERVAL 1 YEAR)
                AND sub.subscription_end_month >= DATE_ADD(c.cohort_month, INTERVAL 1 YEAR)
            ) THEN 1 ELSE 0 
        END AS is_retained_1_year
    FROM cohorts c
    JOIN base b ON c.customer_id = b.customer_id
)

SELECT 
    cohort_month,
    check_retention_date,
    customer_id,
    customer_country,
    business_category,
    is_retained_60_day,
    is_retained_6_month,
    is_retained_1_year
FROM retention
