## Ordering Decision Dashboard (Looker Studio)

This document implements the dashboard plan using the existing marts and a
placeholder ML forecast source. It is written for Looker Studio and keeps
all logic within calculated fields, parameters, filters, and tables.

### Data sources and joins

- `fct_products_cohort` (product + warehouse grain):
  - Use for demand baselines and existing order logic:
    `i_last_30d_sold_quantity`, `i_last_7d_sold_quantity`,
    `i_last_3_weeks_avg_sold_quantity`, `daily_demand`, `trend_factor`,
    `seasonality`, `adjusted_demand`, `coverage_days`, `safety_stock`,
    `total_need`, `available_stock`, `order_quantity`, `recommendation`,
    `days_of_supply`, `current_departure_date`, `next_coming_date`.

- `fct_products` (line item / product stock details):
  - Use for operational constraints and risk drivers:
    `in_stock_quantity`, `coming_quantity`, `transit_quantity`,
    `aging_stock_quantity`, `expired_stock_quantity`,
    `incidents_quantity`, `missing_packing_quantity`,
    `missing_quantity_receiving_stage`, `inventory_status`,
    `unit_landed_cost`, `Origin`, `Supplier`, `product_category`,
    `product_subcategory`, `stock_model`, `warehouse`.

- `fct_daily_quantity_events` (product + warehouse + date):
  - Use for trend proof and anomaly context:
    `daily_net_change`, `cumulative_remaining_quantity`, `ordered`,
    `incidents`, `returned`, `extra`, `sold`, `reserved`, `released`.

- ML forecast source (placeholder name `ml_forecast`):
  - Required fields for Looker Studio blending:
    `product`, `warehouse`, `date`, `predicted_sold_quantity`.
  - If your real table uses different names, map them in Looker Studio by
    creating calculated fields with these exact names in the data source.

Blend strategy in Looker Studio:
- Base data source: `fct_products_cohort` (product + warehouse).
- Blend `ml_forecast` on `product` + `warehouse` (and `date` for time series).
- Blend `fct_daily_quantity_events` on `product` + `warehouse` + `date`.
- If blending `fct_products`, use `product` + `warehouse` and aggregate fields
  with SUM/AVG as listed below.

### Decision logic and formulas (Looker Studio)

Create the following parameters:
- `p_coverage_days_threshold` (number, default 3)
- `p_forecast_gap_threshold` (number, default 0.25)
- `p_anomaly_threshold` (number, default 0.40)
- `p_value_filter_min` (number, default 0) for order value filters

Calculated fields (use these names exactly in Looker Studio):

1) Forecast and baseline
- `Forecast_30d`
  - `SUM(predicted_sold_quantity)`
- `Baseline_30d`
  - `SUM(i_last_30d_sold_quantity)`
- `Forecast_vs_Baseline_Gap`
  - `SAFE_DIVIDE(Forecast_30d - Baseline_30d, NULLIF(Baseline_30d, 0))`

2) Supply, coverage, and recommendation
- `Adjusted_Demand_7d`
  - `SUM(adjusted_demand)`
- `Coverage_Days`
  - `AVG(coverage_days)`
- `Available_Stock`
  - `SUM(available_stock)`
- `Recommended_Order_Qty`
  - `MAX(order_quantity)`
- `Order_Recommendation`
  - `MAX(recommendation)`
- `Order_Value`
  - `SUM(unit_landed_cost) * Recommended_Order_Qty`

3) Final decision flag with reasons
- `Order_Reason_Flag`
  - `CASE
      WHEN Order_Recommendation = 'ORDER'
        AND Forecast_vs_Baseline_Gap >= p_forecast_gap_threshold
        THEN 'Order: Forecast above history'
      WHEN Order_Recommendation = 'ORDER'
        AND Coverage_Days < p_coverage_days_threshold
        THEN 'Order: Low coverage days'
      WHEN Order_Recommendation = 'ORDER'
        AND Available_Stock < Adjusted_Demand_7d * p_coverage_days_threshold
        THEN 'Order: Stock risk'
      WHEN Order_Recommendation = 'SKIP'
        AND Forecast_vs_Baseline_Gap <= -p_forecast_gap_threshold
        THEN 'Skip: Forecast below history'
      ELSE 'Review'
    END`

- `Final_Recommendation`
  - `CASE
      WHEN Order_Recommendation = 'ORDER' THEN 'ORDER'
      WHEN Order_Recommendation = 'SKIP'
        AND ABS(Forecast_vs_Baseline_Gap) >= p_anomaly_threshold
        THEN 'REVIEW'
      ELSE Order_Recommendation
    END`

4) Trust and anomaly indicators
- `Anomaly_Flag`
  - `CASE
      WHEN ABS(Forecast_vs_Baseline_Gap) >= p_anomaly_threshold
        THEN 'Anomaly'
      ELSE 'Normal'
    END`
- `Forecast_Confidence_Label`
  - `CASE
      WHEN ABS(Forecast_vs_Baseline_Gap) < 0.10 THEN 'Aligned'
      WHEN ABS(Forecast_vs_Baseline_Gap) < p_anomaly_threshold THEN 'Moderate'
      ELSE 'High gap'
    END`

5) Operational risk summary
- `Risk_Score`
  - `CASE
      WHEN inventory_status = 'Expired Stock' THEN 3
      WHEN inventory_status = 'Expiring Soon Stock' THEN 2
      WHEN inventory_status = 'Stable Stock' THEN 1
      ELSE 0
    END`
- `Risk_Label`
  - `CASE
      WHEN Risk_Score = 3 THEN 'Expiry risk'
      WHEN Risk_Score = 2 THEN 'Aging risk'
      WHEN Risk_Score = 1 THEN 'Stable'
      ELSE 'Check'
    END`

### Dashboard pages (decision flow)

#### Page 1: Executive Order Decision
Purpose: Answer “What should we order for the next departure, and why?”

Key visuals:
- Scorecards: `Recommended_Order_Qty` (sum), `Order_Value`,
  `Forecast_30d`, `Baseline_30d`.
- Table (product + warehouse):
  - Dimensions: `product`, `warehouse`, `product_category`.
  - Metrics: `Final_Recommendation`, `Order_Reason_Flag`,
    `Recommended_Order_Qty`, `Coverage_Days`, `Anomaly_Flag`.
- Filters:
  - `warehouse`, `product_category`, `stock_model`,
    `current_departure_date` (from cohort).

How teams use it:
- Choose warehouse and departure window, review top recommended orders and
  reasons, then approve or flag for review.

#### Page 2: Demand vs Supply Proof
Purpose: Validate the recommendation using actual history and event-driven
movement.

Key visuals:
- Time series (per product or warehouse):
  - `daily_net_change`, `cumulative_remaining_quantity` from
    `fct_daily_quantity_events`.
- Combo chart:
  - Bars: `Forecast_30d`, line: `Baseline_30d` by product + warehouse.
- KPI tiles:
  - `Forecast_vs_Baseline_Gap` and `Forecast_Confidence_Label`.

How teams use it:
- Confirm forecast consistency with actual movement and challenge anomalies
  before finalizing orders.

#### Page 3: Inventory & Risk Drivers
Purpose: Identify constraints that support or challenge ordering.

Key visuals:
- Table:
  - `in_stock_quantity`, `coming_quantity`, `transit_quantity`,
    `aging_stock_quantity`, `expired_stock_quantity`, `days_of_supply`.
- Bar chart:
  - Incidents and missing: `incidents_quantity`,
    `missing_packing_quantity`, `missing_quantity_receiving_stage`.
- KPI:
  - `Risk_Label`, `coverage_days`, `inventory_status`.

How teams use it:
- Verify supply constraints (transit/coming) and risk signals
  (expiry, incidents, missing) that may override base recommendations.

#### Page 4: Operational Order List
Purpose: Provide final actionable export for procurement and logistics.

Key visuals:
- Table (downloadable):
  - `product`, `warehouse`, `Supplier`, `Origin`,
    `Recommended_Order_Qty`, `unit_landed_cost`, `Order_Value`,
    `Order_Reason_Flag`, `current_departure_date`, `next_coming_date`.
- Filters:
  - `Final_Recommendation`, `Order_Value` (>= `p_value_filter_min`),
    `warehouse`, `product_category`.

How teams use it:
- Export the final ordered list with supplier and cost context for the
  next departure execution.

### Notes for ML integration (when available)

1) Add the ML forecast source to Looker Studio.
2) Ensure the fields match `product`, `warehouse`, `date`,
   `predicted_sold_quantity` (create calculated aliases if needed).
3) Use the blend described above to calculate `Forecast_30d` and
   `Forecast_vs_Baseline_Gap` on the same product + warehouse grain.
