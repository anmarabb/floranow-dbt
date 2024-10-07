with products as (
  select offer_id,

         sum(in_stock_quantity) as in_stock_quantity
  from {{ref("stg_line_items")}} li
  left join {{ref("fct_products")}} p on li.line_item_id = p.line_item_id 
  where feed_source_id = 408 
  group by 1
)

/*

with inbound_stock_transactions as (

select
fm_product_id,
production_date,
transaction_type,

sum(case when transaction_type = 'INBOUND'then quantity else 0 end) as inbound_quantity,
sum(case when transaction_type = 'OUTBOUND'then quantity else 0 end) as outbound_quantity,

from   {{ ref('stg_fm_stock_transactions') }} as st
where transaction_type = 'INBOUND' and production_date is not null and fm_product_id = 2642
group by 1,2,3
order by production_date

),

*/



select
    p.* EXCEPT(sub_group),


    concat( "https://erp.floranow.com/fm/products/", p.fm_product_id) as fm_product_link,



case 
when raw_color in ('Bicolor pink','bicolour','bicolour orange-yellow') then 'bicolor'
when raw_color in ('blue white','light blue','white blue','dark blue') then 'blue'
when raw_color in ('dark red','burgundy') then 'red'
when raw_color in ('Pink White','Light Pink','dark pink') then 'pink'
when raw_color in ('bronze','violet','Mix','apricot','brown','peach','Peach','cream','lilac') then 'other color'
when raw_color in ('fuchsia') then 'cerise'
when raw_color in ('dark green') then 'green'
when raw_color in ('gold') then 'yellow'

else raw_color
end as color,



case 
when p.product_name like '%Double%' THEN 'Lily Or Double'
when p.product_name like '%Lily Or%' THEN 'Lily Or' 
when p.product_name like '%Lily La%' THEN 'Lily LA' 

when p.product_name like '%Chrysanthemum Ying Yang%' THEN 'Chrysanthemum Santini' 
when p.product_name like '%Chrysanthemum Daria%' THEN 'Chrysanthemum Spray' 
when p.product_name like '%Chrysanthemum Doppia%' THEN 'Chrysanthemum Spray' 
when p.product_name like '%Chrysanthemum Harley%' THEN 'Chrysanthemum Spray' 
when p.product_name like '%Chrysanthemum  Calimero%' THEN 'Chrysanthemum Spray' 

when p.product_name like '%Dracaena Massangeana Leaves%' THEN 'Greeneries' 
when p.product_name like '%Strelitzia Leaves%' THEN 'Greeneries' 

when p.product_name like '%Cycas%' THEN 'Cycas' 
when p.sub_group = 'Gerbera Mini' then 'Gerbera'

when p.sub_group is null then p.main_group
else p.sub_group
end as sub_group,
pr.in_stock_quantity,

from   {{ ref('stg_fm_products') }} as p
left join products pr on p.astra_barcode = pr.offer_id
