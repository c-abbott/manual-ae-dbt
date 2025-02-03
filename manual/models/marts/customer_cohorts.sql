-- This query assigns each customer to a cohort based on their first subscription month
WITH cohort AS (
    SELECT
        customer_id,
        MIN(DATE_TRUNC(subscription_start_date, MONTH)) AS cohort_month
    FROM {{ ref('stg_activity') }}
    GROUP BY customer_id
)
SELECT * FROM cohort
