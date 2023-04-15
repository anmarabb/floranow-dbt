with

source as ( 

 
select 

--line order
    line_item_id,
    line_item_link,
    li.quantity,
    li.unit_price,
    li.total_price_without_tax, -- (li.quantity * li.unit_price)

--status
    record_type,
    record_type_details,
    order_type,
    ops_status1,
    ops_status2,
    ops_status3,
    ops_status4,
    ops_status5,

    state,
    
    fulfillment,
    pod_status,
    order_request_status,
    shipments_status,
    order_payloads_status,



    /*
        - order placed but not received.
        - order received but not fulfilled
        - order received but not added to location in stock (for reselling purchase orders)
        - order inventory received but not picked up
        - order inventory picked put not dispatched
        - order dispatched but not delivered.
    */

    case 

        when state = 'FULFILLED' then '1.fulfilled'
        when state = 'DISPATCHED' then '2.dispatched'
        when state = 'DELIVERED' then '3.delivered'
        when state = 'RETURNED' then '4.returned'
        else '0.Not Fulfilled'
        end as order_state,
    
    
    
    
    
    






internal_invoicing,



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
    
    dispatched_by,
    source_type,

    




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












li.incidents_count,



current_timestamp() as insertion_timestamp, 


from {{ref('int_line_items')}} as li 
)

select * from source