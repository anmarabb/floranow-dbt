

SELECT

    sub_group,
    color,
    CONCAT(CAST(year AS STRING), ' - week ', CAST((week) AS STRING)) AS week_number,
     budget_quantity,
   -- week,
   -- year,
     PARSE_DATE("%G-W%V", CONCAT(CAST(year AS STRING), '-W', CAST(week AS STRING))) AS date,



    FROM {{ source(var('erp_source'), 'astra_budget_2024') }} as b
