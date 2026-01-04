
WITH products_format AS (
    SELECT DISTINCT
        sub_group,
        color
    FROM {{ ref('int_fm_products') }}
    WHERE sub_group IS NOT NULL AND color IS NOT NULL
)

SELECT

    COALESCE(p.sub_group, b.sub_group) as sub_group,
    COALESCE(p.color, b.color) as color,
    CONCAT(CAST(year AS STRING), ' - week ', CAST((week) AS STRING)) AS week_number,
     budget_quantity,
   -- week,
   -- year,
     PARSE_DATE("%G-W%V", CONCAT(CAST(year AS STRING), '-W', CAST(week AS STRING))) AS date,


CASE
    WHEN UPPER(COALESCE(p.sub_group, b.sub_group)) IN ('ALSTROEMERIA', 'ASTER', 'CYCAS', 'EUCALYPTUS', 'EUSTOMA', 'LIATRIS', 'GERBERA', 'TRACHELIUM', 'SUNFLOWER', 'STATICE', 'SOLIDAGO', 'RUSCUS', 'LILY OR DOUBLE', 'LILY OR', 'LILY LA', 'CHRYSANTHEMUM SANTINI', 'CHRYSANTHEMUM SINGLE', 'CHRYSANTHEMUM SPRAY') THEN 'Contract'
    WHEN UPPER(COALESCE(p.sub_group, b.sub_group)) IN ('ANTIRRHINUM', 'CARNATION', 'DIANTHUS BARBATUS', 'GYPSOPHILA', 'ROSE', 'SPRAY ROSE', 'CELOSIA', 'GREENERIES') THEN 'Out Of Contract'
    ELSE 'Unknown'
  END AS contract_status,


    FROM {{ source(var('erp_source'), 'astra_budget_2024') }} as b
    LEFT JOIN products_format as p 
        ON UPPER(b.sub_group) = UPPER(p.sub_group) 
        AND LOWER(b.color) = LOWER(p.color)
