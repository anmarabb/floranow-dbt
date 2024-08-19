with pi as (
    select pi.line_item_id,
           sum(pi.incident_quantity) as total_incident_quantity,
           sum(case when pl.Location != "A1 - X" and pl.Location != "X - FN" then pi.incident_quantity end) as outside_incident_quantity,
           sum(case when pl.Location = "A1 - X" then pi.incident_quantity end) as A1_X_incident_qty,
           sum(case when pl.Location = "A1 - X" and pi.incident_type = "DAMAGED" then pi.incident_quantity end) as A1_X_damaged_qty,

           sum(case when pl.Location != "A1 - X" then pi.incident_quantity end) as incident_qty_without_A1X,
           sum(case when pl.Location = "X - FN" then pi.incident_quantity end) as X_FN_incident_qty,
           sum(case when pi.incident_type = "EXTRA" then pi.incident_quantity end) as extra_incident_qty,

  from {{ref('fct_product_incidents')}} as pi
  LEFT JOIN {{ref('fct_product_locations')}}  as pl on pi.incidentable_id = pl.product_location_id
  LEFT JOIN {{ref('fct_order_items')}} as li ON li.line_item_id = pi.line_item_id
  --  where pi.line_item_id=1757046
  --  where li.Reseller = "RUH Project X Stock" and li.order_type != "PICKED_ORDER"
  group by 1
),

invoices as (
    select cli.parent_line_item_id,
           sum(case when cli.order_type = "PICKED_ORDER" then ii.quantity end) as sold_quantity,
           sum(case when cli.order_type != "PICKED_ORDER" then ii.quantity end) as outside_sold_quantity,
           sum(case when cli.order_type = "PICKED_ORDER" then ii.price_without_tax end) as price_without_tax,
           sum(case when cli.order_type != "PICKED_ORDER" then ii.price_without_tax end) as outside_price_without_tax
    from {{ref('fct_order_items')}} as li
    join {{ref('fct_order_items')}} as cli on li.line_item_id = cli.parent_line_item_id 
    join {{ref('fct_invoice_items')}} as ii on ii.line_item_id = cli.line_item_id
 -- where cli.order_type = "PICKED_ORDER"
    group by cli.parent_line_item_id
  
),

product_location as (
    select locationable_id,
           sum(case when pl.Location = 'A1 - X' then pl.location_remaining_quantity end) as remaining_qty_A1_X,
           sum(case when pl.Location = 'X - FN' then pl.location_remaining_quantity end) as remaining_qty_X_FN,
           sum(case when pl.Location = 'A1 - X' then pl.location_quantity end) as entered_qty_A1_X,
    from {{ref('fct_product_locations')}} as pl
  --where pl.locationable_type = "Product"
    group by pl.locationable_id
),

stock_movement as (
    select product_id,
           sum(plm.moved_in_quantity) as moved_in_quantity,
           sum(plm.moved_out_quantity) as moved_out_quantity,

    from {{ref('stg_product_location_movements')}} plm
 
    group by 1
)

select li.line_item_id,
       li.product_id,
       li.Product, 
       li.stem_length, 
       li.customer,
       li.Supplier,
       li.warehouse,
       li.order_date,
 

       li.ordered_quantity,
       li.departure_date, 
       --sum(li.incident_quantity),
       incident_qty_without_A1X as incident_qty_without_A1X,
       
      
      --  sum(case when pl.Location = 'X - FN' then pl.location_quantity end) as entered_qty_X_FN,
       remaining_qty_A1_X,
       remaining_qty_X_FN,
       p.remaining_quantity as actual_remaining_quantity,
       A1_X_incident_qty as A1_X_incident_qty,
       A1_X_damaged_qty,
       X_FN_incident_qty as X_FN_incident_qty,
       extra_incident_qty as extra_incident_qty,
       outside_incident_quantity as outside_incident_quantity,
       ii.sold_quantity as sold_quantity,
       ii.outside_sold_quantity as outside_sold_quantity,
       (li.ordered_quantity -  coalesce(ii.outside_sold_quantity,0) - coalesce(outside_incident_quantity,0)) as FN_X_entered_quantity,
       entered_qty_A1_X, --it may changed if any movement happened on A-X location
       ii.price_without_tax as price_without_tax,
       ii.outside_price_without_tax as outside_price_without_tax,
       moved_in_quantity as a1_x_moved_in_qty,
 





from {{ref('fct_order_items')}} as li
left join {{ref('fct_products')}} as p on li.line_item_id = p.line_item_id
left join product_location as pl on p.product_id = pl.locationable_id
left join pi on pi.line_item_id = li.line_item_id
left join invoices as ii on ii.parent_line_item_id = li.line_item_id 
left join stock_movement as sm on sm.product_id = li.product_id

where li.Reseller = "RUH Project X Stock" and li.order_type != "PICKED_ORDER"