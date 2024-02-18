

SELECT

    sub_group,
    color,
    CONCAT(CAST(year AS STRING), ' - week ', CAST((week + 1) AS STRING)) AS week_number,
    budget_quantity,
   -- week,
   -- year,


    FROM {{ source(var('erp_source'), 'astra_budget_2024') }} as b
