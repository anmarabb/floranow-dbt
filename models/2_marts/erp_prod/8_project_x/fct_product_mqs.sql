select Product,
       product_color,
       CONCAT(coalesce(Product,''), coalesce(product_color,'')) as product_linking,
       MQS
       

from {{ref("stg_product_mqs")}}