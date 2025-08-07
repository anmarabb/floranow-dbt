select t.warehouse, t.product, date(cli.created_at) as order_date, sum(cli.ordered_quantity) as sold_quantity
from {{ref ("stg_training_data")}} t
left join {{ref ("int_line_items")}} li on li.line_item_id = t.line_item_id
left join {{ref ("int_line_items")}} cli on cli.parent_line_item_id = li.line_item_id

where cli.customer_type = 'retail' and t.warehouse = 'Dubai Warehouse' 
and reseller_label = 'Express'
and t.product in ('Rose Ever Red', 'Rose Athena', 'Chrysanthemum Spray Pina Colada', 'Gypsophila Xlence', 'Rose Madam Red')
group by 1,2,3