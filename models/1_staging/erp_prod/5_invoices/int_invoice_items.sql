
with 
prep_registered_clients as (

select 
financial_administration,
count(*) as registered_clients 
from {{ ref('base_users') }}
where account_type in ('External') and deleted_accounts != 'Deleted' 
group by financial_administration
)

select     

--Invoice Items

        ii.*,




ii.quantity * li.unit_landed_cost as total_cost,

        approved_by_id.name as approved_by,


        customer.name as Customer,
        customer.customer_type,
        customer.user_category,
        customer.debtor_number,
        customer.account_manager,
        

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


        

--Line Items

        li.Supplier,
        li.supplier_id,
        li.Origin,
        li.fulfillment_mode,
        li.order_status,
        li.li_record_type_details,
        li.feed_source_name,


        li.unit_landed_cost,
        li.ordering_stock_type,



        li.order_number,

        case when li.order_type is null and i.generation_type = 'MANUAL' then 'Unknown - Manual Invoicing' else li.order_type end as order_type,



        
case 
when li.Supplier  = 'ASTRA Farms' then 'Astra'
when ii.meta_supplier_name in ('Astra Farm','Astra farm Barcode','Astra Farm - Event','Astra Flash Sale - R','Astra Flash sale - W') then 'Astra'
else 'Non Astra'
end as sales_source,





li.proof_of_delivery_id as proof_of_delivery_id_line,

prep_registered_clients.registered_clients,



   
   

li.product_category,

case 
when ii.product_name like '%Lily Ot%' THEN 'Lily Or' 
when ii.product_name like '%Lily Or%' THEN 'Lily Or' 
when ii.product_name like '%Lily La%' THEN 'Lily La' 
when ii.product_name like '%Li La%'  THEN 'Lily La' 
else INITCAP(li.product_subcategory) end as product_subcategory,


/*
case 
when li.feed_source_name in ('Express Jeddah','Express Dammam', 'Express Riyadh', 'Express Tabuk') or li.Supplier in ('Express Jeddah','Express Jeddah', 'Express Jeddah', 'Express Tabuk') or meta_supplier in ('Express Jeddah','Express Jeddah', 'Express Jeddah', 'Express Tabuk') then 'Marketplace'
when li.Supplier in ('Fulfilled by Floranow SA','The Orchid Garden','Solai Roses','Selemo Valley Farms','Lomalinda','Gallica','Galleria Farms','Fresh Cap','Florius','Flores Del Este','Elite Flower Farm','Ecoflor','Capiro','Agroindustria','Smithers Oasis') then 'Re-Selling'
when customer.financial_administration = 'UAE' and li.Supplier in ('Fulfilled by Floranow') then 'Re-Selling'
when li.Supplier in ('Floranow Flash Sale Dammam', 'Floranow Flash Sale Riyadh', 'Floranow Flash Sale Tabuk', 'Floranow Flash Sale Jeddah') then 'Re-Selling'
when meta_supplier in ('Verdissimo - AWS','The Orchid garden Reselling','The Orchid Garden - Event','The Orchid Garden - AWS','The Orchid Garden','Smithers Oasis - AWS','Loma Linda Re-selling','Loma Linda - Event','Loma Linda - AWS','Holland Reselling','Gallica AWS','Galleria Farms Reselling','Galleria Farms','Fulfilled By Floranow-KSA','Fresh cap reselling- AWS','Fresh Cap','Florius - event','Florius','Flores Del Este Reselling','Flores del Este - Event','Flores del Este','Floranow Flash sale Dammam','Floranow Express Flash Sale','Express Store','Express Reselling','Elite Flower Farm - Re-selling','Elite flower farm - event','Elite Flower Farm','Ecoflor Re-selling','Ecoflor Event','Ecoflor AWS','Capiro Re-selling','Capiro Event','Capiro AWS','AgroIndustria Reselling','Agroindustria Colombia - AWS','Agroindustria - Event','Holland Reselling Riyadh','Holland Reselling Dammam','Florius Reselling','Flores Del Este - AWS','Floranow Tabuk','Floranow Riyadh','Floranow Medina','Floranow Jeddah','Floranow Dammam','Floranow Jeddah','Floranow Flash Sale Dammam', 'Floranow Flash Sale Riyadh', 'Floranow Flash Sale Tabuk', 'Floranow Flash Sale Jeddah') then 'Re-Selling'
when li.Supplier in ('Floranow Holland') and li.feed_source_name in ('Holland Reselling','Holland Reselling Dammam','Holland Reselling Riyadh') then 'Re-Selling'
when li.Supplier in ('Floranow Flash sale') and li.feed_source_name in ('Floranow Express Flash sale', 'Floranow Flash Sale Dammam','Floranow Flash Sale Riyadh','Floranow Flash Sale Tabuk', 'Floranow Flash Sale Jeddah') then 'Re-Selling'
when li.Supplier in ('wish flower') then 'Re-Selling'
when li.Supplier in ('ASTRA Farms') and li.feed_source_name in ('Astra DXB out') then 'Marketplace'
when meta_supplier in ('ASTRA Farms') and li.feed_source_name in ('Astra DXB out') then 'Marketplace'
when li.Supplier in ('Ward Flowers') and li.feed_source_name in ('Ward Flower Inventory') then 'Marketplace'
else 'Pre-Selling'
end as trading_model,

*/

case when li.line_item_id is not null then 'line_item_id' else null end as line_item_id_check,
li.parent_id_check,




concat( "https://erp.floranow.com/invoice_items/", ii.invoice_item_id) as invoice_items_link,
concat( "https://erp.floranow.com/invoices/", ii.invoice_header_id) as invoice_link,
concat( "https://erp.floranow.com/line_items/", ii.line_item_id) as line_items_link,

pod.source_type as pod_source_type,


case 
    when li.order_stream_type = 'Customer Sale Order From Inventory' then 'Reselling Model' --customers purchase flowers that are already in your inventory, allowing for faster delivery.
    when li.order_stream_type = 'Customer Sale Order From Direct Supplier' then 'Direct Supplier Model' --Direct Supplier Model: customers purchase directly from the marketplace where suppliers list their flowers.
    when i.generation_type = 'MANUAL' then 'Manual Invoice'
    else 'Cheak Logic'
    end as trading_model,

li.order_stream_type,


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