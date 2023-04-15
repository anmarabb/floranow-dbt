with

source as ( 

 
select 

line_item_id,
record_type,
record_type_details,
order_type,

--date
    delivery_date,
    departure_date,
    created_at as order_date,
    dim_date,


--Customer
    Customer,
    account_manager,
    warehouse,
    country,
    financial_administration,
    User,

--pod
    proof_of_delivery_id,
    pod_status,
    dispatched_by,
    source_type,

    state,
    fulfillment,




Supplier,
supplier_region as Origin,

--product
    product_name as Product,
    product_crop as Crop,
    product_category,
    product_subcategory,

--order
    li.order_number,
    li.currency,


--line order
    line_item_link,
    li.quantity,
    li.unit_price,
    li.total_price_without_tax, -- (li.quantity * li.unit_price)










li.incidents_count,



current_timestamp() as insertion_timestamp, 


from {{ref('int_line_items')}} as li 
)

select * from source