

SELECT

    sub_group,
    color,
    Qty as budget_quantity,
    Date ,
    --Week,
    --FORMAT_DATE('%V', DATE_SUB(Date, INTERVAL MOD(EXTRACT(DAYOFWEEK FROM Date) + 5, 7) DAY)) AS week_number,
CONCAT(
CASE 
WHEN EXTRACT(ISOWEEK FROM Date) = 1 AND EXTRACT(MONTH FROM Date) = 12 THEN CAST(EXTRACT(YEAR FROM Date) + 1 AS STRING)
WHEN EXTRACT(ISOWEEK FROM Date) >= 52 AND EXTRACT(MONTH FROM Date) = 1 THEN CAST(EXTRACT(YEAR FROM Date) - 1 AS STRING)
ELSE CAST(EXTRACT(YEAR FROM Date) AS STRING)
END,
' - week ',
CAST(EXTRACT(ISOWEEK FROM Date) AS STRING)
) AS week_number,


FROM {{ source(var('erp_source'), 'astra_budget_2024') }} as b
where Date is not null