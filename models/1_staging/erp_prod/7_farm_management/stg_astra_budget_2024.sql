

SELECT

    sub_group,
    LOWER(color) as color,
    CONCAT(CAST(year AS STRING), ' - week ', CAST((week) AS STRING)) AS week_number,
     budget_quantity,
   -- week,
   -- year,
     PARSE_DATE("%G-W%V", CONCAT(CAST(year AS STRING), '-W', CAST(week AS STRING))) AS date,



CASE
    WHEN sub_group IN ('Alstroemeria', 'Aster', 'Cycas', 'Eucalyptus', 'Eustoma', 'Liatris', 'Gerbera', 'Trachelium', 'Sunflower', 'Statice', 'Solidago', 'Ruscus', 'Lily Or Double', 'Lily Or', 'Lily LA', 'Chrysanthemum Santini', 'Chrysanthemum Single', 'Chrysanthemum Spray') THEN 'Contract'
    WHEN sub_group IN ('Antirrhinum', 'Carnation', 'Dianthus barbatus', 'Gypsophila', 'Rose', 'Spray Rose', 'Celosia', 'Greeneries') THEN 'Out Of Contract'
    ELSE 'Unknown'
  END AS contract_status,



    FROM {{ source(var('erp_source'), 'astra_budget_2024') }} as b