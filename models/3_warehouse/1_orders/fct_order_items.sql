with

source as ( 

 
select 

line_item_id,
line_item_type,
order_type,
Customer,
User,
returned_by, 

supplier_name as Supplier, 
supplier_region as Origin,

product_name as Product,
product_crop as Crop,
product_category,
product_subcategory,

order_number,
current_timestamp() as insertion_timestamp, 


from {{ref('int_line_items')}} as li 
)

select * from source