with 
base as (

SELECT
supplier_name,
tamplate_name,
item_name,
item_group,
item_sub_group,
item_color,
item_s1,
item_s2,
item_s3,
item_s4,
Box_Type,
qty_unit,
MOQ,
Volumetric_Weight,
Packz_Rate,
Stem_Weight,
concat (item_name,'-',item_group,'-',item_color,'-',item_s1,'-',item_s2,'-',item_s3,'-',item_s4) as ASIN,

FROM `floranow.florisoft.suppliers_tamplate` 

)

select 

*,
CASE WHEN COUNT(*) OVER (PARTITION BY ASIN) > 1 THEN 'd' ELSE NULL end as duplicate_checker,

ROW_NUMBER() OVER (PARTITION BY ASIN ORDER BY ASIN) AS row_num,

from base

order by duplicate_checker desc


