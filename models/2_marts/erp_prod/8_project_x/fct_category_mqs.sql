select *,
       CONCAT(coalesce(sub_category,''), coalesce(sub_group,''), coalesce(lower(product_color),'')) as category_linking,

from {{ref ('stg_category_mqs')}}