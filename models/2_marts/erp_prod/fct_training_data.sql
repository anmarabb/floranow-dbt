select t.warehouse, t.product, date(cli.created_at) as order_date, sum(cli.ordered_quantity) as sold_quantity
from {{ref ("stg_training_data")}} t
left join {{ref ("int_line_items")}} li on li.line_item_id = t.line_item_id
left join {{ref ("int_line_items")}} cli on cli.parent_line_item_id = li.line_item_id
left join {{ref ("stg_feed_sources")}} fs on cli.feed_source_id = fs.feed_source_id
where cli.customer_type = 'retail' -- and p.warehouse = 'Dubai Warehouse' 
and fs.feed_source_id in (277, 271, 578, 991, 445, 886, 1025, 683, 615, 986, 614, 887, 545, 990, 443, 989, 987, 988, 1026)
and t.product in ('Rose Ever Red', 'Rose Athena', 'Chrysanthemum Spray Pina Colada', 'Gypsophila Xlence', 'Rose Madam Red')
group by 1,2,3