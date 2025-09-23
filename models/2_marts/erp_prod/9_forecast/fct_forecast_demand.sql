WITH f AS (
select
    Product,
    date,
    sum(coalesce(requested_quantity, 0))  as requested_quantity,
    sum(coalesce(coming_quantity, 0))     as coming_quantity,
    sum(coalesce(remaining_quantity, 0) ) as remaining_quantity,
    sum(coalesce(actual_quantity, 0) )    as actual_quantity,
    sum(coalesce(forecast_quantity, 0))   as forecast_quantity
  from {{ ref("int_forecast_demand") }}
  group by 1,2
  ),
calc AS (
  SELECT
    Product,
    date,
    requested_quantity,
    coming_quantity,
    remaining_quantity,
    actual_quantity,
    forecast_quantity,

    (requested_quantity + coming_quantity + remaining_quantity) AS total_received_qty,

    (requested_quantity + coming_quantity + remaining_quantity - forecast_quantity) AS daily_variation_all,

    CASE
      WHEN date >= CURRENT_DATE() THEN (requested_quantity + coming_quantity + remaining_quantity - forecast_quantity)
      ELSE NULL END AS variation,

    DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY) AS yday_date,
  FROM f
)

SELECT
  Product,
  date,
  requested_quantity,
  coming_quantity,
  remaining_quantity,
  actual_quantity,
  forecast_quantity,
  total_received_qty,

  variation,

  CASE
    WHEN date >= CURRENT_DATE() THEN
      SUM(
        CASE
          WHEN date >= DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY) THEN COALESCE(daily_variation_all, 0)
          ELSE 0 END) OVER (PARTITION BY Product ORDER BY date ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) ELSE NULL END AS carry_until_yesterday,

  CASE
    WHEN date >= CURRENT_DATE() THEN
      SUM(
        CASE
          WHEN date >= DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
          THEN COALESCE(daily_variation_all, 0)
          ELSE 0
        END
      ) OVER (
        PARTITION BY Product
        ORDER BY date
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
      )
    ELSE NULL
  END AS running_variation,

  DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY) AS carry_reference_date

FROM calc
WHERE date >= CURRENT_DATE()   
ORDER BY Product, date
