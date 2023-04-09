with

source as ( 

 
select 

line_item_id,
line_item_type,
order_type,

delivery_date,
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




Supplier,
supplier_region as Origin,

--product
    product_name as Product,
    product_crop as Crop,
    product_category,
    product_subcategory,

li.order_number,


li.quantity,
li.unit_price,
li.currency,
li.total_price_without_tax, -- (li.quantity * li.unit_price)



li.incidents_count,



current_timestamp() as insertion_timestamp, 


from {{ref('int_line_items')}} as li 
)

select * from source