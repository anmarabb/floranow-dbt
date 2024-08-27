select *,
       CONCAT(coalesce(trim(sub_category),''), coalesce(trim(sub_group),''), coalesce(lower(trim(product_color)),'')) as category_linking,

from {{ref ('stg_category_mqs')}}