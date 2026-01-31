{{
    config(
        materialized='table',
        file_format='iceberg'
    )
}}

/*
=============================================================================
MARTS: Customer RFM Segmentation
=============================================================================
RFM Analysis (Recency, Frequency, Monetary) để phân khúc khách hàng.

Segments:
- Champions: Best customers
- Loyal Customers: Regular buyers
- New Customers: Recently acquired
- At Risk: Haven't purchased recently
- Lost Customers: Churned customers
=============================================================================
*/

{% set reference_date = "CURRENT_DATE()" %}

WITH purchases AS (
    SELECT
        user_id,
        event_date,
        price,
        product_id
    FROM {{ ref('stg_events') }}
    WHERE event_type = 'purchase'
),

-- Calculate RFM metrics
rfm_base AS (
    SELECT
        user_id,
        
        -- Recency: Days since last purchase
        DATEDIFF({{ reference_date }}, MAX(event_date)) AS recency,
        
        -- Frequency: Number of purchases
        COUNT(*) AS frequency,
        
        -- Monetary: Total spending
        ROUND(SUM(price), 2) AS monetary,
        
        -- Additional metrics
        MIN(event_date) AS first_purchase_date,
        MAX(event_date) AS last_purchase_date,
        ROUND(AVG(price), 2) AS avg_order_value,
        COUNT(DISTINCT product_id) AS unique_products_bought,
        DATEDIFF(MAX(event_date), MIN(event_date)) AS customer_lifespan_days
        
    FROM purchases
    GROUP BY user_id
),

-- Calculate RFM scores (1-5 scale using percentiles)
rfm_scored AS (
    SELECT
        *,
        
        -- R Score: Lower recency = better (5 is best)
        NTILE(5) OVER (ORDER BY recency DESC) AS r_score,
        
        -- F Score: Higher frequency = better (5 is best)
        NTILE(5) OVER (ORDER BY frequency ASC) AS f_score,
        
        -- M Score: Higher monetary = better (5 is best)
        NTILE(5) OVER (ORDER BY monetary ASC) AS m_score
        
    FROM rfm_base
),

-- Calculate combined RFM and assign segments
rfm_segmented AS (
    SELECT
        *,
        
        -- Combined RFM score
        r_score + f_score + m_score AS rfm_score,
        
        -- RFM string (e.g., "555" for best customers)
        CONCAT(CAST(r_score AS STRING), CAST(f_score AS STRING), CAST(m_score AS STRING)) AS rfm_string,
        
        -- Customer segment based on RFM scores
        CASE
            -- Champions: High on all metrics
            WHEN r_score >= 4 AND f_score >= 4 AND m_score >= 4 
                THEN 'Champions'
            
            -- Loyal Customers: Good on all metrics
            WHEN r_score >= 3 AND f_score >= 3 AND m_score >= 3 
                THEN 'Loyal Customers'
            
            -- Potential Loyalists: Recent, moderate frequency
            WHEN r_score >= 4 AND f_score >= 2 AND f_score <= 4 
                THEN 'Potential Loyalists'
            
            -- New Customers: Very recent, low frequency
            WHEN r_score >= 4 AND f_score <= 2 
                THEN 'New Customers'
            
            -- Promising: Recent with some activity
            WHEN r_score >= 3 AND f_score <= 2 
                THEN 'Promising'
            
            -- Need Attention: Above average but slipping
            WHEN r_score >= 2 AND r_score <= 3 AND f_score >= 2 AND f_score <= 3 
                THEN 'Need Attention'
            
            -- About to Sleep: Below average, declining
            WHEN r_score <= 3 AND f_score <= 3 AND m_score <= 3 
                THEN 'About to Sleep'
            
            -- At Risk: Were good customers, haven't returned
            WHEN r_score <= 2 AND f_score >= 3 
                THEN 'At Risk'
            
            -- Can't Lose Them: High value but churning
            WHEN r_score <= 2 AND m_score >= 4 
                THEN 'Cant Lose Them'
            
            -- Hibernating: Low on everything
            WHEN r_score <= 2 AND f_score <= 2 
                THEN 'Hibernating'
            
            -- Lost: Haven't bought in a long time
            WHEN r_score = 1 
                THEN 'Lost'
            
            ELSE 'Other'
        END AS customer_segment,
        
        -- Segment priority (for targeting)
        CASE
            WHEN r_score >= 4 AND f_score >= 4 AND m_score >= 4 THEN 1
            WHEN r_score <= 2 AND m_score >= 4 THEN 2  -- Can't Lose Them - urgent
            WHEN r_score <= 2 AND f_score >= 3 THEN 3  -- At Risk
            WHEN r_score >= 3 AND f_score >= 3 AND m_score >= 3 THEN 4
            WHEN r_score >= 4 AND f_score <= 2 THEN 5  -- New Customers
            ELSE 6
        END AS segment_priority
        
    FROM rfm_scored
)

SELECT
    user_id,
    
    -- RFM Metrics
    recency,
    frequency,
    monetary,
    
    -- RFM Scores
    r_score,
    f_score,
    m_score,
    rfm_score,
    rfm_string,
    
    -- Segment
    customer_segment,
    segment_priority,
    
    -- Customer details
    first_purchase_date,
    last_purchase_date,
    customer_lifespan_days,
    avg_order_value,
    unique_products_bought,
    
    -- Segment date
    {{ reference_date }} AS segment_date,
    
    -- Metadata
    CURRENT_TIMESTAMP() AS _created_at

FROM rfm_segmented
ORDER BY rfm_score DESC, monetary DESC
