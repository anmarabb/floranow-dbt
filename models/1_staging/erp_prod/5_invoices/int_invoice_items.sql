

with 
prep_registered_clients 
         as (
   
                select 
                financial_administration,
                count(*) as registered_clients 
                from {{ ref('base_users') }}
                where account_type in ('External') 
                group by financial_administration
            )

select     

--Invoice Items

        ii.* EXCEPT(quantity),

case when i.invoice_header_type = 'credit note' then -ii.quantity else ii.quantity end  as quantity,

    case when invoice_header_type = 'invoice' and invoice_item_status = 'APPROVED' then ii.price_without_tax else 0 end as gross_revenue,
    case when invoice_header_type = 'credit note' and invoice_item_status = 'APPROVED' then ii.price_without_tax else 0 end as credit_note,

    case when invoice_header_type = 'invoice' and invoice_item_status = 'APPROVED'  and i.generation_type = 'AUTO' then ii.price_without_tax else 0 end as auto_gross_revenue,
    case when invoice_header_type = 'credit note' and invoice_item_status = 'APPROVED' and i.generation_type = 'AUTO' then ii.price_without_tax else 0 end as auto_credit_note,




case when i.invoice_header_type = 'invoice' then ii.quantity * li.unit_landed_cost else 0 end  as total_cost,

        approved_by_id.name as approved_by,


        customer.name as Customer,
        customer.customer_type,
        customer.user_category,
        customer.debtor_number,
        customer.account_manager,
        customer.Warehouse as warehouse,
        customer.user_validity_filter,
        customer.user_aging_type,
        --customer.company_name,
        

concat(customer.debtor_number,ii.delivery_date) as drop_id, 


--case when i.invoice_header_type = 'credit note' then -ii.quantity else ii.quantity end as invoiced_quantity,



--invoice Header

        i.financial_administration,
        i.invoice_header_created_at,
        i.invoice_header_printed_at,
        i.invoice_header_type,
        i.invoice_header_status,
        i.generation_type,
        i.record_type,
        i.proof_of_delivery_id as proof_of_delivery_id_inv,
        i.invoice_number,

       -- i.company_name,

        

--Line Items

        
        case 
            when li.Supplier is not null then li.Supplier
            when li.Supplier is null and ii.meta_supplier is not null then ii.meta_supplier 
            when li.Supplier is null and ii.meta_supplier is null and ii.meta_supplier_name is not null then ii.meta_supplier_name
            when li.Supplier is null and ii.meta_supplier is null and ii.meta_supplier_name is null  and ii.meta_supplier_code is not null then ii.meta_supplier_code
            else  'To Be Scoped'   end as Supplier,


        case 
            when li.Supplier  = 'ASTRA Farms' then 'Astra'
            when ii.meta_supplier_name in ('Astra Farm','Astra farm Barcode','Astra Farm - Event','Astra Flash Sale - R','Astra Flash sale - W') then 'Astra'
            when ii.meta_supplier in ('Astra Farm','Astra farm Barcode','Astra Farm - Event','Astra Flash Sale - R','Astra Flash sale - W') then 'Astra'
            when li.Supplier is null and ii.meta_supplier is null and ii.meta_supplier_name is null  and ii.meta_supplier_code is null then 'To Be Scoped' 
            else 'Non Astra'
        end as sales_source,

        case 
            WHEN  LOWER(customer.name) LIKE '%tamimi%' THEN 'Tamimi Customer'
            WHEN  customer.name IN ('REMA1','REMA2','REMA3','REMA4','REMA5','REMA6','REMA7','REMA8') THEN 'REMA Customer'
            ELSE 'Normal Customer'
        END as tamimi_rema_customer,




       


        li.supplier_id,
        li.Origin,
        li.fulfillment_mode,
        li.order_status,
        li.li_record_type_details,
        li.feed_source_name,
        li.li_record_type,
        li.stem_length,
        li.tags,
        li.unit_fob_price,
        
          CASE
            WHEN REGEXP_CONTAINS(li.tags, 'Deals') THEN 'Promotion Offer'
            WHEN REGEXP_CONTAINS(li.tags, 'Flash Sale') THEN 'Promotion Offer'
            ELSE 'Regular Offer'
          END AS offer_type,



        li.unit_landed_cost,
        li.ordering_stock_type,



        li.order_number,

        case when li.order_type is null and i.generation_type = 'MANUAL' then 'Unknown - Manual Invoicing' else li.order_type end as order_type,



        






li.proof_of_delivery_id as proof_of_delivery_id_line,

prep_registered_clients.registered_clients,



   
   

li.product_category,

case 
when ii.product_name like '%Lily Ot%' THEN 'Lily Or' 
when ii.product_name like '%Lily Or%' THEN 'Lily Or' 
when ii.product_name like '%Lily La%' THEN 'Lily La' 
when ii.product_name like '%Li La%'  THEN 'Lily La' 
else INITCAP(li.product_subcategory) end as product_subcategory,







case when ii.line_item_id is not null then 'line_item_id' else null end as line_item_id_check,
li.parent_id_check,




concat( "https://erp.floranow.com/invoice_items/", ii.invoice_item_id) as invoice_items_link,
concat( "https://erp.floranow.com/invoices/", ii.invoice_header_id) as invoice_link,
concat( "https://erp.floranow.com/line_items/", ii.line_item_id) as line_items_link,

pod.source_type as pod_source_type,


case 
    when li.order_source = 'Express Inventory' then 'Re-Selling (Express)' --customers purchase flowers that are already in your inventory, allowing for faster delivery.
    when li.order_source = 'Direct Supplier' then 'Pre-Selling' --Direct Supplier Model: customers purchase directly from the marketplace where suppliers list their flowers.
    when i.generation_type = 'MANUAL' then 'Manual Invoice'
    else 'Cheak Logic'
    end as trading_model,

case 
when li.Supplier in ('ASTRA Farms','Ward Flowers') then 'Commission Based'
when li.order_source in ('Express Inventory') then 'Reselling'
when li.order_source in ('Direct Supplier')  then 'Pre-Selling'
when i.generation_type = 'MANUAL' then 'Manual Invoice'
else 'To Be Scoped'
end as stock_model,



li.order_source,




CASE 
    WHEN ROW_NUMBER() OVER (PARTITION BY ii.invoice_header_id ORDER BY ii.invoice_item_id) = 1 THEN i.delivery_charge_amount 
    ELSE 0 
  END as delivery_charge_amount,


current_timestamp() as insertion_timestamp, 

from {{ ref('stg_invoice_items') }} as ii
left join {{ ref('stg_invoices') }} as i on ii.invoice_header_id = i.invoice_header_id

left join {{ ref('base_users') }} as customer on customer.id = ii.customer_id
left join {{ref('base_users')}} as approved_by_id on approved_by_id.id = ii.approved_by_id

left join {{ ref('fct_order_items') }} as li on ii.line_item_id = li.line_item_id

left join {{ ref('stg_proof_of_deliveries') }} as pod on li.proof_of_delivery_id = pod.proof_of_delivery_id

--left join {{ref('base_suppliers')}} as lis on lis.supplier_id = li.supplier_id


left join prep_registered_clients as prep_registered_clients on prep_registered_clients.financial_administration = customer.financial_administration





--where invoice_type = 'credit note' and creditable_id is null
--in the level of invoice_item all the credit note related to creditable_id (where invoice_type = 'credit note' and creditable_id is null)