(% docs order status %}


One of the following values:
status  definition

placed -  Order placed but not yet shipped
shipped - Order has been shipped but hasn't yet been delivered
completed - Order has been received by customers
return_pending - Customer has indicated they would like to return this item
returned - Item has been returned

(% enddocs %}


name: user_id
        description: "Unique identifier for a user"
        tests:
            - not_null

- name: item_sale_price
        description: "How much the item sold for"
        tests:
            - not_null

- name: product_department
        description: "Whether the item is Astra or Non Astra"
        tests:
            - not_null

- name: product_cost #item_cost
        description: "How much the product cost the business to purchase"
        tests:
            - not_null

- name: product_retail_price
        description: "How much the product retails for on the online store"
        tests:
            - not_null

- name: item_profit
        description: "item_sale_price minus product_cost"
        tests:
          - not_null
          - dbt_utils.expression_is_true:
              expression: "= (item_sale_price - product_cost)"

- name: item_discount
      description: "product_retail_price minus item_sale_price"
      tests:
        - not_null
        - dbt_utils.expression_is_true:
            expression: "= (product_retail_price - item_sale_price)"





stg_product_incidents
    incident_type
        - MISSING: The line item contains a missing quantity
        - EXTRA: The line item contains a extra quantity
        - DAMAGED: The line item contains a damaged quantity
        - RETURNED: The line item contains a returned quantity ( return the line item from customer )

    stage
        - PACKING: report incidents during packing stage
        - RECEIVING: report incidents during receiving stage
        - INVENTORY: report incidents during inventory stage ( line item in the warehouse )
        - DELIVERY: report incidents during delivery stage
        - AFTER_RETURN: report incidents after the line item returned from customer

    incidentable_type
        - PackageLineItem: Report incidents on package line items ( packing and receiving stage)
        - InvoiceItem: Report incidents on invoice item ( after delivery the line item )
        - LineItem: Report incidents on line item ( incidents on child line items )
        - ProductLocation: Report incidents on product location ( the line item in warehouse )
        - Product: Report incidents on product ( after sold the product and before dispatch the child)






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


stg_invoice_items
    status 
        APPROVED
        CANCELED
        DRAFT
        REJECTED

stg_invoices
    status 
        0= draft - Original invoice not printed/ Pending
        1= signed - Client signed on the invoice during delivery in the mobile app
        2= Open - Need to check with Dev
        3= Printed - Original invoice printed (Dispatched from Fulfilment)
        6= Closed - Need to check with Dev team. Never saw this
        7= Canceled - Invoice/ Credit Note cancelled
        8= Rejected - Invoice/ credit note Rejected
        9= voided

        open     
        printed
        closed
        canceled
        rejected
        voided

