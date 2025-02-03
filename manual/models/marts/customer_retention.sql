-- This query joins cohort data with activity data to track subscription activity over time
WITH cohort_data AS (
    SELECT
        a.customer_id,
        c.cohort_month,
        c.customer_country,
        c.business_category,
        DATE_TRUNC(a.subscription_start_date, MONTH) AS activity_month,
        a.subscription_end_date
    FROM {{ ref('stg_activity') }} a
    JOIN {{ ref('customer_cohorts') }} c
    ON a.customer_id = c.customer_id
),
-- Calculates the number of active users for each cohort and month
cohort_retention AS (
    SELECT
        cohort_month,
        activity_month,
        customer_country,
        business_category,
        COUNT(DISTINCT customer_id) AS total_active_users
    FROM cohort_data
    WHERE activity_month >= cohort_month
    GROUP BY cohort_month, activity_month, customer_country, business_category
),
-- Determines the total number of users in each cohort, segmented by country and business category
cohort_size AS (
    SELECT
        cohort_month,
        customer_country,
        business_category,
        COUNT(DISTINCT customer_id) AS cohort_size
    FROM cohort_data
    GROUP BY cohort_month, customer_country, business_category
),
-- Identifies the initial activity month for each customer
initial_cohort_retention AS (
    SELECT
        c.cohort_month,
        c.customer_country,
        c.business_category,
        a.customer_id,
        MIN(DATE_TRUNC(a.subscription_start_date, MONTH)) OVER (PARTITION BY c.customer_id) AS initial_activity_month
    FROM {{ ref('stg_activity') }} a
    JOIN {{ ref('customer_cohorts') }} c
    ON a.customer_id = c.customer_id
)
-- Final query to compute retention rates and track cohort activity, segmented by country and business category
SELECT 
    cr.cohort_month,
    cr.activity_month,
    cr.customer_country,
    cr.business_category,
    cr.total_active_users,
    cs.cohort_size,
    ROUND(100.0 * cr.total_active_users / cs.cohort_size, 2) AS retention_rate,
    icr.initial_activity_month
FROM cohort_retention cr
JOIN cohort_size cs
ON cr.cohort_month = cs.cohort_month 
AND cr.customer_country = cs.customer_country 
AND cr.business_category = cs.business_category
JOIN initial_cohort_retention icr
ON cr.cohort_month = icr.cohort_month 
AND cr.customer_country = icr.customer_country 
AND cr.business_category = icr.business_category
