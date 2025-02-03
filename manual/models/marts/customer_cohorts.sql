-- This query assigns each customer to a cohort based on their first subscription month
WITH cohort AS (
    SELECT
        a.customer_id,
        c.customer_country,
        o.business_category,
        MIN(DATE_TRUNC(a.subscription_start_date, MONTH)) AS cohort_month
    FROM {{ ref('stg_activity') }} a
    JOIN {{ ref('stg_customers') }} c
    ON a.customer_id = c.customer_id
    JOIN {{ ref('stg_acq_orders') }} o
    ON a.customer_id = o.customer_id
    GROUP BY a.customer_id, c.customer_country, o.business_category
)
SELECT * FROM cohort
