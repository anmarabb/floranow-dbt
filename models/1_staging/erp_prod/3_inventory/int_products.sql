with 
    product_incidents as (
        select 
        p.product_id,
        count(*) as incidents_count,
        sum(pi.quantity) as incidents_quantity,
        sum(case when incident_type ='DAMAGED' then pi.quantity else 0 end) as damaged_quantity,
        from {{ ref('stg_product_incidents')}}  as pi 
        left join {{ ref('stg_line_items')}}  as li on  pi.line_item_id = li.line_item_id
        left join {{ ref('stg_products')}}  as p on  p.line_item_id = li.line_item_id and p.product_id is not null
        where  pi.deleted_at is null
        group by p.product_id
      ),

    line_items_sold as (
        select
        p.product_id,
        count(li.line_item_id) as item_sold,
        sum(li.quantity) as sold_quantity,         
        from {{ ref('stg_line_items')}} as li
        left join {{ ref('stg_products')}} as p on p.line_item_id = li.parent_line_item_id
        group by 1
      )

select


--products
    p.* EXCEPT(quantity,published_quantity,remaining_quantity,visible,departure_date,created_at),
    p.quantity as ordered_quantity,
    p.published_quantity,
    p.remaining_quantity,
    p.departure_date,
    p.created_at,

    case when p.visible is true then 'Visible' else 'Not Visible' end as Visibility,
    case when p.remaining_quantity > 0 then 'Live Stock'  else 'Total Stock' end as live_stock,

--product_locations
    pl.quantity as location_quantity,
    pl.remaining_quantity as location_remaining_quantity,
    pl.product_location_id,
    pl.location_id,
    pl.locationable_id,
    pl.created_at as fulfilled_at, --the time when the product added to loc in stock.
    --pl.updated_at,
    pl.empty_at, --damaged at.
    pl.locationable_type,
    pl.inventory_cycle_check_status,
    pl.labeled,
    pl.section_cycle_check,


--line_items
    li.ordered_quantity as li_ordered_quantity,
    --li.inventory_quantity,
    li.fulfilled_quantity,
    li.record_type,
    li.record_type_details,
    li.order_status,
    li.fulfillment_status,
    li.warehouse,
    li.loc_status,
    li.fulfillment_mode,
    li.fulfillment,
    li.delivery_date,
    li.User,
    li.order_type,
    li.Shipment,
    li.shipments_status,
    li.master_shipments_status,
    
    case 
    when date_diff(date(li.delivery_date)  ,current_date(), month) > 1 then 'Wrong date' 
    when li.delivery_date > current_date() then "Future" 
    when li.delivery_date = current_date() then "Today" 
    when li.delivery_date < current_date() then "Past" 
    else "cheak" end as select_delivery_date,

    case 
    when date_diff(date(p.departure_date)  ,current_date(), month) > 1 then 'Wrong date' 
    when p.departure_date > current_date() then "Future" 
    when p.departure_date = current_date() then "Today" 
    when p.departure_date < current_date() then "Past" 
    else "cheak" end as select_departure_date,


    case 
    when date_diff(date(case when li.order_type = 'IMPORT_INVENTORY' and p.departure_date is null  then date(p.created_at) else p.departure_date end)  ,current_date(), month) > 1 then 'Wrong date' 
    when case when li.order_type = 'IMPORT_INVENTORY' and p.departure_date is null  then date(p.created_at) else p.departure_date end > current_date() then "Future" 
    when case when li.order_type = 'IMPORT_INVENTORY' and p.departure_date is null  then date(p.created_at) else p.departure_date end = current_date() then "Today" 
    when case when li.order_type = 'IMPORT_INVENTORY' and p.departure_date is null  then date(p.created_at) else p.departure_date end < current_date() then "Past" 
    else "cheak" end as calc_select_departure_date,



    case when li.order_type = 'IMPORT_INVENTORY' and p.departure_date is null  then date(p.created_at) else p.departure_date end as calc_departure_date, 
    case when li.delivery_date is null and li.order_type in ('IMPORT_INVENTORY', 'EXTRA','MOVEMENT') then date(li.order_date) else li.delivery_date end as calc_delivery_date,

st.stock_name as Stock,
reseller.name as Reseller,
concat(st.stock_id, " - ", st.stock_name, " - ", reseller.name  ) as full_stock_name,

--feed_source
    fs.feed_source_name as feed_source_name,
    origin_fs.feed_source_name as origin_feed_name,
    publishing_fs.feed_source_name as publishing_feed_name,
    out_fs.feed_source_name as out_feed_source_name,


s.supplier_name as Supplier,


--line_items_sold
    lis.item_sold,
    lis.sold_quantity,

    pi.incidents_count,
    pi.incidents_quantity,
    pi.damaged_quantity,

    case when li.fulfillment_status = '2. Fulfilled - with Full Item Incident' and  incidents_quantity != p.quantity then 'red_flag' else null end as full_incident_check,

      
      
from {{ ref('stg_products')}} as p
left join {{ ref('base_stocks')}} as st on p.stock_id = st.stock_id and p.reseller_id = st.reseller_id
left join {{ ref('fct_order_items')}} as li on p.line_item_id = li.line_item_id
left join {{ ref('base_suppliers')}} as s on s.supplier_id = p.supplier_id
left join {{ ref('stg_feed_sources')}} as origin_fs on p.origin_feed_source_id = origin_fs.feed_source_id 
left join {{ ref('stg_feed_sources')}} as publishing_fs on p.publishing_feed_source_id = publishing_fs.feed_source_id 
left join {{ ref('stg_feed_sources')}} as fs on p.feed_source_id = fs.feed_source_id 
left join {{ ref('stg_feed_sources')}} as out_fs on st.out_feed_source_id = out_fs.feed_source_id 
left join {{ ref('base_users')}} as reseller on reseller.id = p.reseller_id
left join {{ ref('stg_product_locations')}} as pl on pl.locationable_id = p.product_id and pl.locationable_type = "Product"
left join line_items_sold as lis on lis.product_id = p.product_id
left join product_incidents as pi on pi.product_id = p.product_id
--where p.product_id =76370