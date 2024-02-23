select

bi.*,
--FORMAT_DATE('%V', DATE_SUB(shipmet_creation_date, INTERVAL MOD(EXTRACT(DAYOFWEEK FROM shipmet_creation_date) + 5, 7) DAY)) AS week_number
CONCAT(
CASE 
WHEN EXTRACT(ISOWEEK FROM shipmet_creation_date) = 1 AND EXTRACT(MONTH FROM shipmet_creation_date) = 12 THEN CAST(EXTRACT(YEAR FROM shipmet_creation_date) + 1 AS STRING)
WHEN EXTRACT(ISOWEEK FROM shipmet_creation_date) >= 52 AND EXTRACT(MONTH FROM shipmet_creation_date) = 1 THEN CAST(EXTRACT(YEAR FROM shipmet_creation_date) - 1 AS STRING)
ELSE CAST(EXTRACT(YEAR FROM shipmet_creation_date) AS STRING)
END,
' - week ',
CAST(EXTRACT(ISOWEEK FROM shipmet_creation_date) AS STRING)
) AS week_number,


case when fm_shipment_id  in (3555,3506,3511) then 'Opening Production Stock' else 'Regular Production Stock' end as fm_report_filter,

from   {{ ref('int_fm_box_items') }} as bi