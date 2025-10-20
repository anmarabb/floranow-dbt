select * EXCEPT(product),
       case when product in ('Ruscus Hypophyllum (large Leaf)', 'Ruscus') then 'Ruscus' else product end as product, 
from {{ref ("fct_products")}}