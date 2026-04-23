/* =========================================================
PROJECT: E-Commerce Customer Behavior, Retention & Profitability
AUTHOR: Shivam Kumar
DIALECT: MySQL 8+
PURPOSE: Advanced Analytics Portfolio Project
========================================================= */

/* =========================================================
SECTION 1: ANALYTICAL SEMANTIC LAYER
Creates a clean, unified view of order delivery, SLA, and financial metrics.
========================================================= */
-- Q1. How do we create a unified, order-level semantic layer for all downstream delivery and financial analysis?
CREATE OR REPLACE VIEW vw_order_level_metrics AS
SELECT
    o.order_id,
    o.customer_id,
    c.customer_unique_id,
    c.customer_state,
    c.customer_city,
    o.order_status,
    o.order_purchase_timestamp,
    o.order_delivered_customer_date,
    o.order_estimated_delivery_date,
    TIMESTAMPDIFF(DAY, o.order_purchase_timestamp, o.order_delivered_customer_date) AS actual_delivery_days,
    CASE
        WHEN o.order_delivered_customer_date > o.order_estimated_delivery_date THEN 'Late'
        WHEN o.order_delivered_customer_date <= o.order_estimated_delivery_date THEN 'On Time / Early'
        ELSE 'Unknown'
    END AS delivery_experience,
    COALESCE(op.total_payment, 0) AS payment_value,
    r.review_score
FROM orders o
JOIN customers c ON o.customer_id = c.customer_id
LEFT JOIN (
    SELECT order_id, SUM(payment_value) as total_payment 
    FROM order_payments 
    GROUP BY order_id
) op ON o.order_id = op.order_id
LEFT JOIN order_reviews r ON o.order_id = r.order_id;


/* =========================================================
SECTION 2: DATA QUALITY & FINANCIAL RECONCILIATION
Ensures data hygiene before downstream aggregations.
========================================================= */
-- Q2.1. Are there any critical data quality issues, such as missing purchase timestamps?
SELECT COUNT(*) AS missing_timestamp_count 
FROM orders 
WHERE order_purchase_timestamp IS NULL;

-- Q2.2. Does the sum of items and freight match the actual payment values (Financial Reconciliation)?
SELECT
    oi.order_id,
    SUM(oi.price + oi.freight_value) AS total_items_cost,
    MAX(op.total_payment) AS total_payment,
    SUM(oi.price + oi.freight_value) - MAX(op.total_payment) AS difference
FROM order_items oi
JOIN (
    SELECT order_id, SUM(payment_value) as total_payment 
    FROM order_payments 
    GROUP BY order_id
) op ON oi.order_id = op.order_id
GROUP BY oi.order_id
HAVING ABS(difference) > 1
LIMIT 10;


/* =========================================================
SECTION 3: EXECUTIVE KPI SCORECARD
Tracks Month-over-Month core growth metrics.
========================================================= */
-- Q3. What is the month-over-month executive KPI scorecard for revenue, active customers, and AOV?
WITH monthly_kpi AS (
    SELECT
        DATE_FORMAT(order_purchase_timestamp, '%Y-%m-01') AS cohort_month,
        COUNT(DISTINCT order_id) AS total_orders,
        COUNT(DISTINCT customer_unique_id) AS active_customers,
        SUM(payment_value) AS total_revenue
    FROM vw_order_level_metrics
    WHERE order_status = 'delivered'
    GROUP BY DATE_FORMAT(order_purchase_timestamp, '%Y-%m-01')
)
SELECT
    cohort_month,
    total_orders,
    active_customers,
    total_revenue,
    ROUND(total_revenue / NULLIF(active_customers, 0), 2) AS aov,
    LAG(total_revenue) OVER(ORDER BY cohort_month) AS prev_month_revenue,
    ROUND(100.0 * (total_revenue - LAG(total_revenue) OVER(ORDER BY cohort_month)) / NULLIF(LAG(total_revenue) OVER(ORDER BY cohort_month), 0), 2) AS mom_growth_pct
FROM monthly_kpi
ORDER BY cohort_month;


/* =========================================================
SECTION 4: DYNAMIC RFM CUSTOMER SEGMENTATION
========================================================= */
-- Q4. How can we segment customers dynamically using an RFM (Recency, Frequency, Monetary) matrix?
WITH customer_rfm AS (
    SELECT
        customer_unique_id,
        DATEDIFF((SELECT MAX(order_purchase_timestamp) FROM xorders), MAX(order_purchase_timestamp)) AS recency,
        COUNT(DISTINCT order_id) AS frequency,
        SUM(payment_value) AS monetary
    FROM vw_order_level_metrics
    WHERE order_status = 'delivered'
    GROUP BY customer_unique_id
),
rfm_scores AS (
    SELECT
        customer_unique_id,
        recency, frequency, monetary,
        NTILE(5) OVER (ORDER BY recency DESC) AS r_score,
        NTILE(5) OVER (ORDER BY frequency ASC) AS f_score,
        NTILE(5) OVER (ORDER BY monetary ASC) AS m_score
    FROM customer_rfm
)
SELECT
    customer_unique_id,
    recency, frequency, monetary,
    r_score, f_score, m_score,
    CASE
        WHEN r_score >= 4 AND f_score >= 4 AND m_score >= 4 THEN '1. Champions'
        WHEN r_score <= 2 AND f_score >= 4 AND m_score >= 4 THEN '2. At Risk Whales'
        WHEN r_score >= 4 AND f_score <= 2 THEN '3. New Promising'
        ELSE '4. General'
    END AS segment
FROM rfm_scores
ORDER BY monetary DESC
LIMIT 50;


/* =========================================================
SECTION 5: COHORT RETENTION MATRIX WITH REVENUE DEPTH
Outputs in a 'Tall' format (month_number rows instead of M1..M6 columns)
so BI tools can dynamically pivot to any length of time.
========================================================= */
-- Q5. What is the month-by-month cohort retention rate and retained revenue depth?
WITH first_purchases AS (
    SELECT
        customer_unique_id,
        MIN(order_purchase_timestamp) AS first_purchase_date,
        DATE_FORMAT(MIN(order_purchase_timestamp), '%Y-%m-01') AS cohort_month
    FROM vw_order_level_metrics
    WHERE order_status = 'delivered'
    GROUP BY customer_unique_id
),
cohort_sizes AS (
    SELECT
        cohort_month,
        COUNT(DISTINCT customer_unique_id) AS cohort_size
    FROM first_purchases
    GROUP BY cohort_month
),
customer_months AS (
    SELECT
        f.customer_unique_id,
        f.cohort_month,
        TIMESTAMPDIFF(MONTH, f.first_purchase_date, o.order_purchase_timestamp) AS month_number,
        o.payment_value
    FROM first_purchases f
    JOIN vw_order_level_metrics o
      ON f.customer_unique_id = o.customer_unique_id
     AND o.order_status = 'delivered'
)
SELECT
    cm.cohort_month,
    cs.cohort_size,
    cm.month_number,
    COUNT(DISTINCT cm.customer_unique_id) AS retained_customers,
    ROUND(100.0 * COUNT(DISTINCT cm.customer_unique_id) / cs.cohort_size, 2) AS retention_pct,
    ROUND(SUM(cm.payment_value), 2) AS retained_revenue
FROM customer_months cm
JOIN cohort_sizes cs ON cm.cohort_month = cs.cohort_month
WHERE cm.month_number <= 12
GROUP BY cm.cohort_month, cs.cohort_size, cm.month_number
ORDER BY cm.cohort_month, cm.month_number;

/* =========================================================
SECTION 6: SELLER CONCENTRATION AND OPERATIONAL RISK
Calculates Z-scores for SLA delivery and reviews to statistically
flag outliers instead of hardcoded threshold strings.
========================================================= */
-- Q6. Which top sellers are posing an operational risk due to high late delivery rates (measured via Z-scores)?
WITH seller_metrics AS (
    SELECT
        oi.seller_id,
        s.seller_state,
        COUNT(DISTINCT CASE WHEN o.order_status = 'delivered' THEN o.order_id END) AS delivered_orders,
        ROUND(SUM(CASE WHEN o.order_status = 'delivered' THEN oi.price + oi.freight_value ELSE 0 END), 2) AS gmv,
        ROUND(AVG(CASE
            WHEN o.order_status = 'delivered'
            THEN TIMESTAMPDIFF(DAY, o.order_purchase_timestamp, o.order_delivered_customer_date)
        END), 2) AS avg_delivery_days,
        ROUND(100.0 * AVG(CASE
            WHEN o.order_status = 'delivered'
             AND o.order_delivered_customer_date > o.order_estimated_delivery_date
            THEN 1
            ELSE 0
        END), 2) AS late_delivery_rate_pct,
        ROUND(AVG(CASE WHEN o.order_status = 'delivered' THEN r.review_score END), 2) AS avg_review_score
    FROM order_items oi
    JOIN orders o
        ON oi.order_id = o.order_id
    JOIN sellers s
        ON oi.seller_id = s.seller_id
    LEFT JOIN order_reviews r
        ON o.order_id = r.order_id
    GROUP BY oi.seller_id, s.seller_state
    HAVING delivered_orders >= 30
),
seller_stats AS (
    SELECT
        AVG(late_delivery_rate_pct) AS network_avg_late_rate,
        STDDEV_POP(late_delivery_rate_pct) AS stddev_late_rate,
        AVG(avg_review_score) AS network_avg_review_score,
        STDDEV_POP(avg_review_score) AS stddev_review_score
    FROM seller_metrics
),
seller_ranked AS (
    SELECT
        sm.seller_id,
        sm.seller_state,
        sm.delivered_orders,
        sm.gmv,
        sm.avg_delivery_days,
        sm.late_delivery_rate_pct,
        sm.avg_review_score,
        SUM(sm.gmv) OVER (ORDER BY sm.gmv DESC ROWS UNBOUNDED PRECEDING) AS running_gmv,
        SUM(sm.gmv) OVER () AS total_gmv,
        ROW_NUMBER() OVER (ORDER BY sm.gmv DESC) AS seller_rank,
        COUNT(*) OVER () AS seller_count,
        NTILE(10) OVER (ORDER BY sm.gmv DESC) AS gmv_decile,
        -- Z-score logic for statistical outlier detection
        ROUND((sm.late_delivery_rate_pct - ss.network_avg_late_rate) / NULLIF(ss.stddev_late_rate, 0), 2) AS late_rate_z_score,
        ROUND((sm.avg_review_score - ss.network_avg_review_score) / NULLIF(ss.stddev_review_score, 0), 2) AS review_score_z_score
    FROM seller_metrics sm
    CROSS JOIN seller_stats ss
)
SELECT
    seller_rank,
    seller_id,
    seller_state,
    delivered_orders,
    gmv,
    ROUND(100.0 * gmv / total_gmv, 2) AS gmv_share_pct,
    ROUND(100.0 * running_gmv / total_gmv, 2) AS cumulative_gmv_share_pct,
    avg_delivery_days,
    late_delivery_rate_pct,
    late_rate_z_score,
    avg_review_score,
    review_score_z_score
FROM seller_ranked
WHERE seller_rank <= 50
   OR ROUND(100.0 * running_gmv / total_gmv, 2) <= 80
ORDER BY seller_rank;

-- Insight: Z-scores allow the system to flag sellers automatically if they deviate significantly from the network mean (e.g. z_score > 2.0).
-- Recommendation: Configure BI alerts to trigger when a top-decile seller crosses 1.5 standard deviations in late rate.


/* =========================================================
SECTION 7: CATEGORY UNIT ECONOMICS & CX PRESSURE
Statistical identification of categories driving margin or SLA decay.
========================================================= */
-- Q7. Which categories require immediate margin or CX recovery actions based on Z-scores and GMV share?
WITH category_metrics AS (
    SELECT
        COALESCE(pct.product_category_name_english, p.product_category_name) AS category_name,
        COUNT(DISTINCT o.order_id) AS delivered_orders,
        ROUND(SUM(oi.price), 2) AS product_revenue,
        ROUND(SUM(oi.freight_value), 2) AS freight_cost,
        ROUND(SUM(oi.price + oi.freight_value), 2) AS gross_merchandise_value,
        ROUND(AVG(oi.price), 2) AS avg_item_price,
        ROUND(100.0 * SUM(oi.freight_value) / NULLIF(SUM(oi.price), 0), 2) AS freight_to_revenue_pct,
        ROUND(100.0 * AVG(CASE
            WHEN o.order_delivered_customer_date > o.order_estimated_delivery_date THEN 1
            ELSE 0
        END), 2) AS late_delivery_rate_pct,
        ROUND(AVG(r.review_score), 2) AS avg_review_score
    FROM order_items oi
    JOIN orders o ON oi.order_id = o.order_id
    JOIN products p ON oi.product_id = p.product_id
    LEFT JOIN product_category_name_translation pct ON p.product_category_name = pct.product_category_name
    LEFT JOIN order_reviews r ON o.order_id = r.order_id
    WHERE o.order_status = 'delivered'
    GROUP BY category_name
    HAVING delivered_orders >= 100
),
category_stats AS (
    SELECT
        AVG(freight_to_revenue_pct) AS network_avg_freight_pct,
        STDDEV_POP(freight_to_revenue_pct) AS stddev_freight_pct,
        SUM(gross_merchandise_value) AS total_gmv
    FROM category_metrics
)
SELECT
    cm.category_name,
    cm.delivered_orders,
    cm.gross_merchandise_value,
    ROUND(100.0 * cm.gross_merchandise_value / cs.total_gmv, 2) AS gmv_share_pct,
    cm.avg_item_price,
    cm.freight_to_revenue_pct,
    ROUND((cm.freight_to_revenue_pct - cs.network_avg_freight_pct) / NULLIF(cs.stddev_freight_pct, 0), 2) AS freight_pressure_z_score,
    cm.late_delivery_rate_pct,
    cm.avg_review_score,
    CASE
        WHEN cm.freight_to_revenue_pct >= 40 AND cm.avg_item_price < 80 THEN 'Margin destructive'
        WHEN cm.late_delivery_rate_pct >= 15 AND cm.avg_review_score < 4.00 THEN 'CX recovery needed'
        WHEN (100.0 * cm.gross_merchandise_value / cs.total_gmv) >= 5 THEN 'Core growth category'
        ELSE 'Monitor'
    END AS category_action
FROM category_metrics cm
CROSS JOIN category_stats cs
ORDER BY freight_pressure_z_score DESC, cm.gross_merchandise_value DESC;

-- Insight: Margin pressure is not absolute; using Z-scores identifies categories that are statistical outliers in freight costs.
-- Recommendation: Review shipping contracts or bundle rules for categories with a freight_pressure_z_score > 2.0 or those flagged as 'Margin destructive'.

-- Insight: Not every high-GMV category is healthy; some scale by adding freight drag and service complexity.
-- Recommendation: Bundle low-ticket bulky categories, raise minimum basket thresholds, or renegotiate shipping terms where freight burden is too high.


/* =========================================================
SECTION 8: STATE-LEVEL DEMAND VS SERVICE GAP
Highlights which customer markets deserve service investment first.
========================================================= */
-- Q8. Which customer states have high demand but suffer from below-average delivery service?
WITH overall_benchmark AS (
    SELECT
        ROUND(AVG(actual_delivery_days), 2) AS overall_avg_delivery_days,
        ROUND(100.0 * AVG(CASE WHEN delivery_experience = 'Late' THEN 1 ELSE 0 END), 2) AS overall_late_rate_pct,
        ROUND(AVG(review_score), 2) AS overall_review_score
    FROM vw_order_level_metrics
    WHERE order_status = 'delivered'
),
state_metrics AS (
    SELECT
        customer_state,
        COUNT(DISTINCT order_id) AS delivered_orders,
        COUNT(DISTINCT customer_unique_id) AS active_customers,
        ROUND(SUM(payment_value), 2) AS revenue,
        ROUND(AVG(payment_value), 2) AS aov,
        ROUND(AVG(actual_delivery_days), 2) AS avg_delivery_days,
        ROUND(100.0 * AVG(CASE WHEN delivery_experience = 'Late' THEN 1 ELSE 0 END), 2) AS late_rate_pct,
        ROUND(AVG(review_score), 2) AS avg_review_score
    FROM vw_order_level_metrics
    WHERE order_status = 'delivered'
    GROUP BY customer_state
    HAVING delivered_orders >= 500
),
state_revenue_benchmark AS (
    SELECT ROUND(AVG(revenue), 2) AS avg_state_revenue
    FROM state_metrics
)
SELECT
    sm.customer_state,
    sm.delivered_orders,
    sm.active_customers,
    sm.revenue,
    sm.aov,
    sm.avg_delivery_days,
    sm.late_rate_pct,
    sm.avg_review_score,
    ROUND(sm.avg_delivery_days - ob.overall_avg_delivery_days, 2) AS delivery_days_vs_avg,
    ROUND(sm.late_rate_pct - ob.overall_late_rate_pct, 2) AS late_rate_vs_avg,
    CASE
        WHEN sm.revenue >= srb.avg_state_revenue
         AND sm.late_rate_pct > ob.overall_late_rate_pct
            THEN 'Priority service recovery market'
        WHEN sm.revenue >= srb.avg_state_revenue
            THEN 'Core market to defend'
        WHEN sm.late_rate_pct > ob.overall_late_rate_pct
         AND sm.avg_review_score < ob.overall_review_score
            THEN 'Operational watchlist'
        ELSE 'Stable'
    END AS market_action
FROM state_metrics sm
CROSS JOIN overall_benchmark ob
CROSS JOIN state_revenue_benchmark srb
ORDER BY sm.revenue DESC, sm.late_rate_pct DESC;

-- Insight: Revenue-heavy states with above-average late rate are where logistics improvement produces the highest commercial payoff.
-- Recommendation: Prioritize SLA fixes in big-demand states first, because each service improvement there protects more GMV and more repeat customers.


/* =========================================================
SECTION 9: EXECUTIVE ACTION BOARD
Converts analysis into prioritized actions for leadership.
========================================================= */
-- Q9. What are the prioritized, data-driven action items for the executive team based on the analysis?
WITH delivered_order_sequence AS (
    SELECT
        customer_unique_id,
        order_id,
        order_purchase_timestamp,
        payment_value,
        delivery_experience,
        ROW_NUMBER() OVER (
            PARTITION BY customer_unique_id
            ORDER BY order_purchase_timestamp, order_id
        ) AS order_sequence
    FROM vw_order_level_metrics
    WHERE order_status = 'delivered'
),
first_orders AS (
    SELECT
        customer_unique_id,
        order_purchase_timestamp AS first_purchase_timestamp,
        delivery_experience
    FROM delivered_order_sequence
    WHERE order_sequence = 1
),
repeat_90d AS (
    SELECT
        fo.customer_unique_id,
        fo.delivery_experience,
        CASE
            WHEN COUNT(DISTINCT CASE
                WHEN dos.order_sequence > 1
                 AND dos.order_purchase_timestamp <= DATE_ADD(fo.first_purchase_timestamp, INTERVAL 90 DAY)
                THEN dos.order_id
            END) > 0
            THEN 1
            ELSE 0
        END AS repeated_within_90d
    FROM first_orders fo
    JOIN delivered_order_sequence dos
        ON fo.customer_unique_id = dos.customer_unique_id
    GROUP BY fo.customer_unique_id, fo.delivery_experience
),
retention_gap AS (
    SELECT
        COALESCE(ROUND(
            MAX(CASE WHEN delivery_experience = 'On Time / Early' THEN repeat_rate_90d_pct END)
            - MAX(CASE WHEN delivery_experience = 'Late' THEN repeat_rate_90d_pct END),
            2
        ), 0) AS repeat_rate_gap_pp
    FROM (
        SELECT
            delivery_experience,
            100.0 * AVG(repeated_within_90d) AS repeat_rate_90d_pct
        FROM repeat_90d
        WHERE delivery_experience IN ('On Time / Early', 'Late')
        GROUP BY delivery_experience
    ) retention_rates
),
customer_revenue AS (
    SELECT
        customer_unique_id,
        SUM(payment_value) AS monetary
    FROM vw_order_level_metrics
    WHERE order_status = 'delivered'
    GROUP BY customer_unique_id
),
customer_revenue_ranked AS (
    SELECT
        customer_unique_id,
        monetary,
        NTILE(5) OVER (ORDER BY monetary ASC) AS revenue_quintile
    FROM customer_revenue
),
revenue_concentration AS (
    SELECT
        ROUND(
            100.0 * SUM(CASE WHEN revenue_quintile = 5 THEN monetary ELSE 0 END)
            / NULLIF(SUM(monetary), 0),
            2
        ) AS top_customer_quintile_revenue_share_pct
    FROM customer_revenue_ranked
),
seller_revenue AS (
    SELECT
        oi.seller_id,
        SUM(oi.price + oi.freight_value) AS gmv
    FROM order_items oi
    JOIN orders o
        ON oi.order_id = o.order_id
    WHERE o.order_status = 'delivered'
    GROUP BY oi.seller_id
),
seller_revenue_ranked AS (
    SELECT
        seller_id,
        gmv,
        SUM(gmv) OVER (ORDER BY gmv DESC ROWS UNBOUNDED PRECEDING) AS running_gmv,
        SUM(gmv) OVER () AS total_gmv,
        ROW_NUMBER() OVER (ORDER BY gmv DESC) AS seller_rank,
        COUNT(*) OVER () AS seller_count
    FROM seller_revenue
),
seller_pareto AS (
    SELECT
        ROUND(
            100.0 * MIN(CASE WHEN running_gmv >= total_gmv * 0.80 THEN seller_rank END)
            / MAX(seller_count),
            2
        ) AS seller_pct_for_80pct_gmv
    FROM seller_revenue_ranked
),
category_cost_pressure AS (
    SELECT
        COALESCE(pct.product_category_name_english, p.product_category_name) AS category_name,
        COUNT(DISTINCT o.order_id) AS delivered_orders,
        100.0 * SUM(oi.freight_value) / NULLIF(SUM(oi.price), 0) AS freight_to_revenue_pct
    FROM order_items oi
    JOIN orders o
        ON oi.order_id = o.order_id
    JOIN products p
        ON oi.product_id = p.product_id
    LEFT JOIN product_category_name_translation pct
        ON p.product_category_name = pct.product_category_name
    WHERE o.order_status = 'delivered'
    GROUP BY COALESCE(pct.product_category_name_english, p.product_category_name)
    HAVING delivered_orders >= 100
),
category_alerts AS (
    SELECT COUNT(*) AS margin_destructive_categories
    FROM category_cost_pressure
    WHERE freight_to_revenue_pct >= 40
)
SELECT
    'P1' AS priority,
    'First-order delivery recovery' AS initiative,
    CONCAT('90-day repeat-rate gap between on-time and late first orders: ', repeat_rate_gap_pp, ' pp') AS evidence,
    CASE
        WHEN repeat_rate_gap_pp >= 5
            THEN 'Recalibrate ETA promises, prioritize first-order parcels, and trigger service recovery for late first purchases.'
        ELSE 'Maintain SLA monitoring; first-order delay is not creating a severe repeat-rate penalty.'
    END AS recommendation
FROM retention_gap

UNION ALL

SELECT
    'P1',
    'High-value customer retention',
    CONCAT('Top revenue quintile contributes ', top_customer_quintile_revenue_share_pct, '% of delivered revenue'),
    CASE
        WHEN top_customer_quintile_revenue_share_pct >= 50
            THEN 'Launch VIP retention, proactive support, and win-back programs for top-spend customers.'
        ELSE 'Revenue concentration is moderate; balance acquisition with lifecycle automation.'
    END
FROM revenue_concentration

UNION ALL

SELECT
    'P1',
    'Seller concentration risk',
    CONCAT(seller_pct_for_80pct_gmv, '% of sellers generate 80% of delivered GMV'),
    CASE
        WHEN seller_pct_for_80pct_gmv <= 20
            THEN 'Treat top sellers as critical accounts with SLA scorecards, escalation paths, and assortment backup plans.'
        ELSE 'Seller base is relatively diversified; continue recruiting and scaling reliable mid-tier sellers.'
    END
FROM seller_pareto

UNION ALL

SELECT
    'P2',
    'Category margin cleanup',
    CONCAT(margin_destructive_categories, ' categories exceed a 40% freight-to-revenue ratio'),
    CASE
        WHEN margin_destructive_categories > 0
            THEN 'Bundle bulky low-ticket products, enforce minimum basket rules, or renegotiate shipping economics in flagged categories.'
        ELSE 'No major category breaches the freight pressure threshold; keep monitoring as mix evolves.'
    END
FROM category_alerts;


/* =========================================================
FINAL BUSINESS INSIGHTS AND RECOMMENDATIONS
=========================================================
1. The project should be read as a business operating system, not a set
   of isolated SQL questions. Revenue, retention, SLA performance, review
   quality, and freight economics are intentionally connected.

2. The most important commercial signal is whether first-order delivery
   quality changes 90-day repeat behavior. If late first orders materially
   underperform, logistics investment will likely outperform more acquisition spend.

3. Customer value and seller value are both concentrated in marketplaces.
   Protecting high-value buyers and top-GMV sellers usually delivers faster
   ROI than broad, undifferentiated promotions.

4. Category growth should never be judged on GMV alone. Categories with
   high freight burden or weak post-purchase experience can inflate sales
   while quietly damaging margin and retention.

5. Recommended leadership actions:
   - Make M1 retention, late-delivery rate, and review score part of one weekly operating review.
   - Escalate any high-GMV seller whose late rate worsens or whose review score falls below target.
   - Build win-back and VIP retention programs around Champions and At Risk Whales.
   - Reprice, bundle, or restrict freight-heavy low-ticket categories.
   - Invest in logistics improvement first in high-revenue states with above-average service gaps.
========================================================= */
