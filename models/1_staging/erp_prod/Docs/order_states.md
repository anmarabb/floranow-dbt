(% docs order status %}


One of the following values:
status  definition

placed -  Order placed but not yet shipped
shipped - Order has been shipped but hasn't yet been delivered
completed - Order has been received by customers
return_pending - Customer has indicated they would like to return this item
returned - Item has been returned

(% enddocs %}

stg_line_items
    fulfillment
        SUCCEED - Set on fulfilling, when adding the full quantity to location or proof of delivery
        PARTIAL - Set on fulfilling, when adding the part of the quantity to location or proof of delivery
        FAILED - Set on fulfilling, when full quantity is missing
        UNACCOUNTED - Set on placing order till receiving
    state
        --PENDING    - Set on placing order till receiving
        FULFILLED  - Set on adding line item’s product to location or put the line item to proof of delivery
        DISPATCHED - Set on dispatching line item                            (li.dispatched_at,)
        DELIVERED  - Set on delivered item to the end user                   (li.delivered_at,)
        --CANCELED   - Set on canceled the full quantity                     (li.canceled_at,)
        RETURNED   - Set on returned the full quantity to the warehouse      (li.returned_at,)
    state and fulfillment
        when state in (PENDING, CANCELED) then fulfillment = UNACCOUNTED

    ordering_stock_type
        INVENTORY - line item created by ordering from a product while existing in the inventory
        FLYING - line item created by ordering from a product while is not received yet in the warehouse
        null - line item created by ordering from a external  supplier

    order_type
        ONLINE           - line items created by orders placed from the marketplace
        OFFLINE          - line items created by orders placed by order request or standing order
        ADDITIONAL       - line items created by reporting additional in receiving stage or inventory stage in ERP
        IMPORT_INVENTORY - line items created by importing inventory products from an Excel sheet
        EXTRA            - line items created by reporting extra quantity during the packing, receiving, or inventory stage
        RETURN           - line items created by reporting returned items after it has been delivered to the customer
        MOVEMENT         - line items created by moving items from internal stock to another internal stock

    creation_stage
        SPLIT - Line item creation stage on split proof of delivery
        PACKING - Line item creation stage on packing on reporting additional
        INVENTORY - Line item creation stage on inventory on reporting additional
        receiving - Line item creation stage on receiving on reporting extra

    
    location
        pod  - on proof of delivery
        loc  - on location in warehouse
        null - not fulfilled, not added to location or pod


    Shipments
        DRAFT
        PACKED
        WAREHOUSED
        CANCELED
        MISSING

    Master Shipments
        DRAFT
        PACKED
        OPENED
        WAREHOUSED
        CANCELED
        MISSING
    
    Order Recqusts
        REQUESTED
        PLACED
        PARTIALLY_PLACED
        REJECTED
        CANCELED



    Proof Of Delivery
        DRAFT
        READY
        DISPATCHED
        DELIVERED
        SKIPPED


