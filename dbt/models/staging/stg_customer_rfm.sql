{{
    config(
        materialized='view'
    )
}}

/*
=============================================================================
STAGING: Customer RFM Segmentation (ClickHouse over Iceberg)
=============================================================================
Customer segmentation from Iceberg tables.

Input: lakehouse.customer_rfm (Iceberg on MinIO)
Output: RFM analysis view with action recommendations
=============================================================================
*/

SELECT
    user_id,
    recency,
    frequency,
    monetary,
    first_purchase_date,
    last_purchase_date,
    avg_order_value,
    unique_products_bought,
    r_score,
    f_score,
    m_score,
    rfm_score,
    rfm_string,
    customer_segment,
    segment_date,
    -- Segment action recommendation
    CASE 
        WHEN customer_segment = 'Champions' THEN 'VIP - Priority support'
        WHEN customer_segment IN ('Loyal Customers', 'Potential Loyalist') THEN 'High Value - Retention focus'
        WHEN customer_segment = 'New Customers' THEN 'Nurture - Welcome program'
        WHEN customer_segment IN ('At Risk', 'About to Sleep') THEN 'Win Back - Re-engagement'
        ELSE 'Standard'
    END as action_recommendation
FROM lakehouse.customer_rfm
