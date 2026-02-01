# Superset Dashboard - Chart Recommendations

## Overview

Dựa trên 4 Iceberg tables có sẵn, đây là 10+ biểu đồ được recommend để tạo dashboard insights đầy đủ.

**Datasets có sẵn:**
1. `lakehouse.daily_sales` (1,105 rows)
2. `lakehouse.customer_rfm` (110,518 rows)
3. `lakehouse.funnel_analysis` (152 rows)
4. `lakehouse.product_performance` (66,852 rows)

---

## MUST-HAVE Charts (Top 3 - Yêu cầu đề bài)

### Chart 1: Revenue by Category (ĐÃ TẠO)
**Dataset:** `daily_sales`  
**Chart Type:** Bar Chart  
**Configuration:**
- **Dimension (X-axis):** `category_level1`
- **Metric:** `SUM(total_revenue)`
- **Sort:** Descending by metric
- **Color Scheme:** Sequential (Blues hoặc Greens)

**Insight:** Hiển thị category nào generate revenue nhiều nhất.

---

### Chart 2: Customer Segmentation Distribution
**Dataset:** `customer_rfm`  
**Chart Type:** Pie Chart  
**Configuration:**
- **Dimension:** `customer_segment`
- **Metric:** `COUNT(*)`
- **Label Type:** Percent
- **Show Legend:** Yes

**SQL (nếu cần):**
```sql
SELECT 
    customer_segment,
    COUNT(*) as customer_count
FROM lakehouse.customer_rfm
GROUP BY customer_segment
```

**Insight:** Phân bố khách hàng theo segment (Champions, Loyal, At Risk, etc.)

**Cách tạo:**
1. Charts → + Chart
2. Choose `customer_rfm` dataset
3. Visualization Type: **Pie Chart**
4. Dimensions: `customer_segment`
5. Metrics: `COUNT(*)`
6. Update Chart → Save

---

### Chart 3: Conversion Funnel Over Time
**Dataset:** `funnel_analysis`  
**Chart Type:** Line Chart  
**Configuration:**
- **X-axis:** `event_date`
- **Metrics:** 
  - `SUM(views)`
  - `SUM(carts)`
  - `SUM(purchases)`
- **Show Data Points:** Yes
- **Line Style:** Smooth

**SQL:**
```sql
SELECT 
    event_date,
    SUM(views) as total_views,
    SUM(carts) as total_carts,
    SUM(purchases) as total_purchases
FROM lakehouse.funnel_analysis
GROUP BY event_date
ORDER BY event_date
```

**Insight:** Tracking conversion funnel metrics theo thời gian.

**Cách tạo:**
1. Charts → + Chart
2. Choose `funnel_analysis` dataset
3. Visualization Type: **Line Chart**
4. X-axis: `event_date`
5. Metrics: Add 3 metrics (`SUM(views)`, `SUM(carts)`, `SUM(purchases)`)
6. Customize → Enable smooth lines
7. Update Chart → Save

---

## RECOMMENDED Additional Charts (7 more)

### Chart 4: Monthly Revenue Trend
**Dataset:** `daily_sales`  
**Chart Type:** Line Chart with Area Fill  
**Configuration:**
- **X-axis:** `CONCAT(sale_year, '-', LPAD(sale_month, 2, '0'))` (create temporal column)
- **Metric:** `SUM(total_revenue)`
- **Area Fill:** Yes
- **Color:** Green gradient

**Custom Column (SQL Lab):**
```sql
SELECT 
    CONCAT(sale_year, '-', LPAD(CAST(sale_month AS String), 2, '0')) as year_month,
    SUM(total_revenue) as revenue
FROM lakehouse.daily_sales
GROUP BY year_month
ORDER BY year_month
```

**Insight:** Revenue trend theo tháng để identify growth/decline patterns.

---

### Chart 5: Top 10 Products by Revenue
**Dataset:** `product_performance`  
**Chart Type:** Horizontal Bar Chart  
**Configuration:**
- **Dimension (Y-axis):** `product_id`
- **Metric:** `total_revenue`
- **Row Limit:** 10
- **Sort:** Descending

**SQL:**
```sql
SELECT 
    product_id,
    total_revenue
FROM lakehouse.product_performance
ORDER BY total_revenue DESC
LIMIT 10
```

**Insight:** Top performers để focus marketing/inventory.

---

### Chart 6: Average Order Value by Category
**Dataset:** `daily_sales`  
**Chart Type:** Bar Chart  
**Configuration:**
- **Dimension:** `category_level1`
- **Metric:** `AVG(avg_order_value)`
- **Sort:** Descending
- **Number Format:** Currency (2 decimals)

**SQL:**
```sql
SELECT 
    category_level1,
    AVG(avg_order_value) as avg_aov
FROM lakehouse.daily_sales
GROUP BY category_level1
ORDER BY avg_aov DESC
```

**Insight:** Categories nào có AOV cao nhất.

---

### Chart 7: RFM Score Distribution
**Dataset:** `customer_rfm`  
**Chart Type:** Histogram  
**Configuration:**
- **Column:** `rfm_score`
- **Bins:** 13 (scores từ 3-15)
- **Normalize:** No

**SQL:**
```sql
SELECT 
    rfm_score,
    COUNT(*) as customer_count
FROM lakehouse.customer_rfm
GROUP BY rfm_score
ORDER BY rfm_score
```

**Insight:** Phân bố RFM score để understand customer quality.

---

### Chart 8: Daily Orders Heatmap
**Dataset:** `daily_sales`  
**Chart Type:** Calendar Heatmap (hoặc Heatmap Table)  
**Configuration:**
- **Date Column:** `event_date`
- **Metric:** `SUM(order_count)`
- **Color Scheme:** YlOrRd (Yellow-Orange-Red)

**SQL:**
```sql
SELECT 
    event_date,
    SUM(order_count) as daily_orders
FROM lakehouse.daily_sales
GROUP BY event_date
```

**Insight:** Identify busy days và seasonal patterns.

---

### Chart 9: Conversion Rate Metrics
**Dataset:** `funnel_analysis`  
**Chart Type:** Big Number with Trendline  
**Configuration:**
- **Metric:** `AVG(overall_conversion_rate)`
- **Comparison:** Previous period
- **Show Trendline:** Yes

**SQL:**
```sql
SELECT 
    AVG(overall_conversion_rate) as avg_conversion_rate,
    AVG(view_to_cart_rate) as avg_view_to_cart,
    AVG(cart_to_purchase_rate) as avg_cart_to_purchase
FROM lakehouse.funnel_analysis
```

**Insight:** Key conversion metrics at a glance.

---

### Chart 10: Customer Lifetime Value by Segment
**Dataset:** `customer_rfm`  
**Chart Type:** Box Plot  
**Configuration:**
- **Distribution Column:** `monetary`
- **Group By:** `customer_segment`
- **Whisker Type:** Min/Max

**SQL:**
```sql
SELECT 
    customer_segment,
    monetary
FROM lakehouse.customer_rfm
WHERE customer_segment IS NOT NULL
```

**Insight:** Compare spending distribution across segments.

---

## Dashboard Layout Suggestions

### Layout 1: Executive Summary (1 Page)

```
┌─────────────────────────────────────────────────┐
│  KPI Cards: Total Revenue | Orders | Customers │
├───────────────────┬─────────────────────────────┤
│                   │                             │
│  Revenue Trend    │   Customer Segments         │
│  (Line Chart)     │   (Pie Chart)               │
│                   │                             │
├───────────────────┴─────────────────────────────┤
│                                                 │
│          Conversion Funnel (Line Chart)         │
│                                                 │
└─────────────────────────────────────────────────┘
```

### Layout 2: Product Performance (1 Page)

```
┌──────────────────────┬──────────────────────────┐
│                      │                          │
│  Revenue by Category │  Top 10 Products         │
│  (Bar Chart)         │  (Horizontal Bar)        │
│                      │                          │
├──────────────────────┴──────────────────────────┤
│                                                 │
│        Average Order Value by Category          │
│               (Bar Chart)                       │
│                                                 │
└─────────────────────────────────────────────────┘
```

### Layout 3: Customer Analytics (1 Page)

```
┌──────────────────────┬──────────────────────────┐
│                      │                          │
│  Customer Segments   │  RFM Score Distribution  │
│  (Pie Chart)         │  (Histogram)             │
│                      │                          │
├──────────────────────┴──────────────────────────┤
│                                                 │
│    Customer LTV by Segment (Box Plot)           │
│                                                 │
└─────────────────────────────────────────────────┘
```

---

## Quick Create Guide

### Method 1: Via UI (Recommended for beginners)

1. **Charts** → **+ Chart**
2. Choose **Dataset** from dropdown
3. Select **Visualization Type**
4. Configure:
   - Dimensions (X-axis, grouping)
   - Metrics (Y-axis, aggregations)
   - Filters (optional)
5. **Update Chart** to preview
6. **Save** → Choose or create dashboard

### Method 2: Via SQL Lab (Advanced)

1. **SQL Lab** → **SQL Editor**
2. Select **Database**: ClickHouse Connect
3. Write custom SQL query
4. **Run** to test
5. **Save** → **Save as Dataset**
6. Create chart from new dataset

**Example:**
```sql
-- Custom metric: Customer acquisition by month
SELECT 
    formatDateTime(segment_date, '%Y-%m') as month,
    customer_segment,
    COUNT(*) as new_customers
FROM lakehouse.customer_rfm
WHERE customer_segment = 'New Customers'
GROUP BY month, customer_segment
ORDER BY month
```

---

## Chart Configuration Tips

### Colors
- **Revenue/Money**: Green palette
- **Customers**: Blue palette
- **Conversion**: Orange/Yellow palette
- **Negative metrics**: Red palette

### Number Formatting
- **Revenue**: `$#,##0.00` (Currency with 2 decimals)
- **Percentages**: `#,##0.00%`
- **Large numbers**: `#,##0` (with thousand separators)

### Time Series
- **Date Format**: `YYYY-MM-DD` for consistency
- **Granularity**: Daily for detailed, Monthly for trends
- **Rolling Averages**: Use 7-day or 30-day for smoothing

### Interactivity
- Enable **Drill Down** for hierarchical data (category_level1 → level2)
- Add **Filters** for date range, category selection
- Use **Cross-filtering** to link charts together

---

## Advanced Chart Ideas

### Chart 11: Cohort Analysis
**Dataset:** Custom SQL from `customer_rfm`  
**Chart Type:** Heatmap Table  
**Purpose:** Track customer retention by signup month

### Chart 12: Product Affinity Network
**Dataset:** Custom SQL joining events  
**Chart Type:** Network Graph (if available)  
**Purpose:** Show products frequently bought together

### Chart 13: Geographic Revenue Map
**Dataset:** Would need location data  
**Chart Type:** Map (Country/Region)  
**Purpose:** Revenue distribution by geography

---

## Troubleshooting

### Chart không load data
**Check:**
1. Dataset connection active?
2. Query syntax correct?
3. ClickHouse table có data không?

```bash
# Verify data
docker exec clickhouse clickhouse-client --password clickhouse123 -q "
SELECT COUNT(*) FROM lakehouse.daily_sales
"
```

### Performance slow
**Solutions:**
- Add date range filter (limit to recent data)
- Reduce row limit (default 10,000 → 1,000)
- Use aggregated tables instead of raw data
- Enable caching in Superset

### Chart type không phù hợp
**Guidelines:**
- **Bar Chart**: Comparing categories
- **Line Chart**: Trends over time
- **Pie Chart**: Part-to-whole (< 7 slices)
- **Heatmap**: Two-dimensional patterns
- **Big Number**: Single KPI metric

---

## Next Steps

1. Create **Chart 2** (Customer Segments Pie)
2. Create **Chart 3** (Conversion Funnel Line)
3. Arrange in Dashboard layout
4. Add filters (Date Range, Category)
5. Share dashboard with stakeholders

**Total recommended time**: 30-45 minutes for 3 charts

---

## Resources

- Superset Documentation: https://superset.apache.org/docs/creating-charts-dashboards/creating-your-first-dashboard
- Chart Gallery: Built-in examples in Superset → Charts
- SQL Lab: For custom queries and dataset creation

---

**Prepared by**: Senior Data Engineer  
**Dataset Ready**: 178K+ rows across 4 tables  
**Dashboard Potential**: 10+ meaningful visualizations
