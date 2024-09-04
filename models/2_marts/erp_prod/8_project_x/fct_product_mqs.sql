select Product,
       product_color,
       CONCAT(coalesce(trim(Product),''), coalesce(lower(trim(product_color)),'')) as product_linking,
       MQS
       

from {{ref("stg_product_mqs")}}