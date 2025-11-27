
With source as 

    (
        select 
            --PK
                li.id as line_item_id,
            --FK
                
                li.invoice_id,
                li.order_id,
                li.order_payload_id,
                li.order_request_id, --Referenced to order_requests, order request which generated this line item

                li.customer_id,
                li.customer_master_id,

                li.user_id,
                li.reseller_id,
                li.supplier_id,
                
                li.offer_id, --No reference offer id where the order placed on. offer id could be referenced to ERP product or external supplier’s offer.
                li.feed_source_id,

                li.shipment_id,
                li.source_shipment_id, --Referenced to shipments, on cancel line item or move line item to another shipment, the line item will be disconnected to the old shipment and keep its data in this column ,related to order cancel process and move line item to feature departure date

                li.root_shipment_id, --Referenced to shipments. related to ordering from reselling stocks



                li.proof_of_delivery_id,
                li.supplier_product_id,

                li.source_line_item_id, --Referenced to line_items, Line item where the generated a copy from, related to reporting extra and returned items
                li.parent_line_item_id, --Referenced to line_items, Line item where the order placed on, related to order from reselling stocks
                li.split_source_id,


                li.dispatched_by_id,
                li.canceled_by_id,
                li.returned_by_id,
                li.created_by_id,
                li.split_by_id, --Referenced to Users, User who split the proof of delivery and cause this line item to split/move to new delivery date
                li.supplier_product_name,
                li.replace_for_id, --Referenced to line_items, Line item which has been replaced by this line item, related to replacement


                li.source_invoice_id, --Referenced to invoices, Invoice related to the line item before cancel or move line item’s delivery date, related to cancel order and move line item
                
                li.import_sheet_id,
            --dim
                --date
                                    -- we need received_at and fulfilled_at to compalte the cycle.
                li.departure_date,
                li.delivery_date,
                li.created_at,      --(order_date)
                li.completed_at,
                li.dispatched_at,   --dispatched
                li.delivered_at,    --delivered
                li.deleted_at,
                li.canceled_at,
                li.split_at,
                li.returned_at,
                li.updated_at,
                li.received_at,

                li.color as colour,


                --order
                li.fulfillment, --SUCCEED (Set on fulfilling, when adding the full quantity to location or proof of delivery), PARTIAL (Set on fulfilling, when adding the part of the quantity to location or proof of delivery), FAILED (Set on fulfilling, when full quantity is missing),UNACCOUNTED (Set on placing order till receiving)
                case when li.location is null then 'null' else li.location end as location, --pod (on proof of delivery), loc (on location in warehouse), null (not fulfilled, not added to location or pod)
                
                li.state, --PENDING (Set on placing order till receiving), FULFILLED (Set on adding line item’s product to location or put the line item to proof of delivery), DISPATCHED(Set on dispatching line item), DELIVERED (Set on delivered item to the end user), CANCELED (Set on canceled the full quantity), RETURNED (Set on returned the full quantity to the warehouse)
                li.creation_stage, --SPLIT (Line item creation stage on split proof of delivery), PACKING (Line item creation stage on packing on reporting additional), INVENTORY (Line item creation stage on inventory on reporting additional),receiving (Line item creation stage on receiving on reporting extra),
                li.ordering_stock_type, --INVENTORY(line item created by ordering from a product while existing in the inventory), FLYING (line item created by ordering from a product while is not received yet in the warehouse), null (line item created by ordering from a external  supplier)
                li.order_type, --ONLINE (line items created by orders placed from the marketplace), OFFLINE (line items created by orders placed by order request or standing order), ADDITIONAL (line items created by reporting additional in receiving stage or inventory stage in ERP), IMPORT_INVENTORY (line items created by importing inventory products from an Excel sheet), EXTRA (line items created by reporting extra quantity during the packing, receiving, or inventory stage), RETURN (line items created by reporting returned items after it has been delivered to the customer), MOVEMENT (line items created by moving items from internal stock to another internal stock)

                li.sales_unit, -- minimum order-fable quantity of the offer where this line item has been placed
                li.sales_unit_name, --piece (Per stem), bunch, box, layer, trolly
                li.permalink,
                li.sequence_number,
                li.barcode,
                li.number,
                li.variety_mask,
                li.product_mask,
                li.previous_moved_proof_of_deliveries,
                li.previous_split_proof_of_deliveries,
                li.previous_shipments,

                
                li.order_number,
                li.tags,
                li.pricing_type, --D2D (Door to door cost), FOB (Free on board cost, supplier cost only.), CIF (FOB, insurance and freight cost)
                
                li.landed_currency,
                li.fob_currency as raw_fob_currency,
                li.currency,

                --product
                li.product_name,
                li.Properties,
                li.categorization,
                li.stem_length,
                li.color,
                li.pn.p1 as spec_1,
                li.pn.p2 as spec_2,
                li.pn.p3 as spec_3,
                li.pn.p4 as spec_4,

            --fct
                li.unit_landed_cost,
                li.unit_fob_price as raw_unit_fob_price,
                li.packing_list_fob_price,
                
                li.unit_price,      --price per unit price calculated by pricing engine
                li.exchange_rate,

                li.total_price_without_tax,
                li.total_price_include_tax,
                li.total_tax,
                

                li.unit_shipment_cost, --shipment cost per unit related to bulk pricing details
                li.unit_additional_cost, -- additional cost per unit related to bulk pricing details

                --quantity
                    li.quantity,
                    li.fulfilled_quantity, 
                    li.received_quantity,
                    --li.inventory_quantity, -- Not used
                    li.missing_quantity, --
                    li.damaged_quantity, --
                    --li.delivered_quantity, -- Not used
                    --li.extra_quantity,     -- Not used
                    li.returned_quantity,
                    li.canceled_quantity,
                    --li.picked_quantity, --Not used
                    li.replaced_quantity,
                    li.splitted_quantity,
                    li.warehoused_quantity,
                    li.published_canceled_quantity,


--li.parent_line_item_id is null and
        case 
            when  li.ordering_stock_type is null and li.reseller_id is not null then 'Reseller Purchase Order For Inventory'
            --when  li.ordering_stock_type is not null and li.reseller_id is not null then 'Reseller Internal Stock Transaction'


            when  li.ordering_stock_type is null and li.reseller_id is null and li.pricing_type in ('FOB','CIF') then 'Customer Bulk Sale Order'
            when  li.ordering_stock_type is null and li.reseller_id is null then 'Customer Sale Order From Direct Supplier' --customer_direct_orders
            when  li.ordering_stock_type = 'INVENTORY' and li.reseller_id is null and li.order_type = 'IN_SHOP' then 'Customer In-Shop Sale Order'


            when  li.ordering_stock_type = 'INVENTORY' and li.reseller_id is null then 'Customer Sale Order From In-stock Inventory' --Customer Sale Order From Inventory
            when  li.ordering_stock_type = 'FLYING' and li.reseller_id is null then 'Customer Sale Order From Fly-stock Inventory' --Customer Sale Order From Inventory
            else 'To Be Scoped'
            end as li_record_type_details,
            

       case 
            when  li.reseller_id is not null  and li.ordering_stock_type is null then 'Purchase Order'
            when  li.reseller_id is null  then 'Sale Order'
            else 'To Be Scoped'
            end as li_record_type,

       case 
            when  li.reseller_id is not null then 'Reseller'
            when  li.reseller_id is null  then 'Customer'
            else 'To Be Scoped'
            end as persona,


        case 
            when  li.ordering_stock_type is null then 'Direct Supplier'
            when  li.ordering_stock_type is not null then 'Express Inventory'
            else 'To Be Scoped'
            end as order_source,




    case 
     when li.state = 'CANCELED' then '1. Not Fulfilled - (Canceled Orders)'
     when li.location is null and li.order_type = 'IN_SHOP' and li.fulfillment = 'SUCCEED' then '5. Fulfilled - In Shop'
     when li.location = 'loc' and li.fulfillment = 'SUCCEED' then '4. Fulfilled - Warehoused Totaly'                                          --  Moveded Totaly to Stock (Warehoused)
     when li.location = 'loc' and li.fulfillment = 'PARTIAL' then '4. Fulfilled - Warehoused Partially (with Incidents)'                      --  Moveded Partially to Stock (Warehoused)
     when li.location = 'loc' and li.fulfillment = 'UNACCOUNTED' then '4. Fulfilled - Warehoused (with Process Breakdown)'
     when li.location = 'pod' and li.fulfillment = 'SUCCEED' then '3. Fulfilled - Moved Totaly to POD'                                        --  Moveded Totaly to Dispatch Area (pod)
     when li.location = 'pod' and li.fulfillment = 'PARTIAL' then '3. Fulfilled - Moved Partially to POD (with Incidents)'                    --  Moveded Partially to Dispatch Area (pod)
     when li.location = 'pod' and li.fulfillment = 'UNACCOUNTED' then '3. Fulfilled - Moved to POD (with Process Breakdown)'
     when li.location is null and li.state != 'CANCELED' and li.fulfillment = 'FAILED' then '2. Fulfilled - with Full Item Incident'
     when li.location is null and li.state != 'CANCELED' and li.fulfillment = 'UNACCOUNTED' then '1. Not Fulfilled - (Investigate)'
     when li.location is null and li.fulfillment in ('PARTIAL','SUCCEED') and li.reseller_id in (2061,1967,2079) then '3. Fulfilled - Other Internal Resellers'
     when li.location is null and li.fulfillment in ('PARTIAL','SUCCEED') then '3. Fulfilled - with Process Breakdown'
     else 'cheack_my_logic'  
     end as fulfillment_status_details,


case 
     when li.state in ('CANCELED','PENDING') and li.fulfillment = 'UNACCOUNTED' and li.location is null then 'Not Fulfilled'
     when li.fulfillment != 'UNACCOUNTED' then 'Fulfilled'
     else 'To Be Scoped'  
     end as fulfillment_status,

case 
when li.order_type = 'IN_SHOP' and li.state = 'DELIVERED' then 'Dispatched'
when li.dispatched_at is not null then 'Dispatched'
else 'Not Dispatched'  
end as dispatched_status,


case 
when li.order_type = 'IN_SHOP' and li.state = 'DELIVERED' and li.state = 'DELIVERED'  then 'Signed'
when li.dispatched_at is not null and li.state = 'DELIVERED'  then 'Signed'
else 'Not Signed' 
end as signed_status,


 ---extra_info
    extra_info.custom_product_name,
    extra_info.line_item_number,
    extra_info.custom_delivery_location,
    extra_info.skip_delivery_time_window,
    extra_info.custom_lpo,

    /*
    extra_info.shopping_cart_info,
        resource_number,
        resource_type,
        shopping_cart_item_id,
        reserved_item_id,
        sales_agent_name,
        total_amount,
        name,
        total_amount_with_tax,
        currency,
        total_tax,
*/




--nested
delivery_time_window.delivery_window_id,
delivery_time_window.delivery_time,
--delivery_time_window.start_time,
--delivery_time_window.end_time,


        REGEXP_EXTRACT(permalink, r'/([^/]+)') AS product_category , --flowers, greeneries
        REGEXP_EXTRACT(permalink, r'/(?:[^/]+)/([^/]+)') AS product_subcategory, --chrysanthemum, tulip
        REGEXP_EXTRACT(permalink, r'/(?:[^/]+/){2}([^/]+)') AS product_subcategory2, --tulip-double
        --REGEXP_EXTRACT(permalink, r'/(?:[^/]+/){3}([^/]+)') AS column4,




--  Note:
    -- Due to the absence of explicit columns for 'product_category' and 'product_subcategory' within the database,
    -- we derive these metrics from the 'permalink' column representing the URL of line_items.
    -- The structure of the permalink allows extraction in the format: 'product_category/product_subcategory'.
    -- It's important to note that these are calculated metrics derived from the URL structure and not directly fetched from the database.

        
        case when li.delivery_date is null and li.order_type in ('IMPORT_INVENTORY', 'EXTRA','MOVEMENT') then date(li.created_at) else li.delivery_date end as mod_delivery_date,
        
        origin_warehouse_id,
        destination_warehouse_id,

        invoice_number,
        local_supplier_name,

        unit_landed_cost * fulfilled_quantity as fulfilled_value,
        origin_line_item_id,
        
        from {{ source(var('erp_source'), 'line_items') }} as li
         where li.deleted_at is null 
         and li.__hevo__marked_deleted is not true
    )




select 

*,


--JASON extraction
--li.Properties, 
--li.categorization,
--li.tags,

current_timestamp() as ingestion_timestamp, 

from source as li
-- where date(created_at) >= '2023-01-01'
