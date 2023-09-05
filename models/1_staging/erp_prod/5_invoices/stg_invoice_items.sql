With source as (
 select * from {{ source('erp_prod', 'invoice_items') }}
)
select 
            --PK
                ii.id as invoice_item_id,
            --FK
                ii.customer_id,
                ii.line_item_id,
                ii.invoice_id as invoice_header_id,
                ii.creditable_id,
                ii.approved_by_id,
            

            --dim
                --date
                ii.created_at as invoice_item_created_at,  --proforma_at, -- The date and time when the invoice item was created.
                ii.updated_at,
                ii.deleted_at,
                ii.delivery_date,
                ii.order_date,


                --dim
                case 
                    when ii.source_type = 'INTERNAL' then 'ERP'
                    when ii.source_type is null  then 'Florisft'
                    else  'check_my_logic'
                    end as source_type,

                ii.generation_type as invoice_item_generation_type,
                
                ii.currency,
                ii.creditable_type,
                ii.status as invoice_item_status, --APPROVED, CANCELED, DRAFT, REJECTED
                ii.number,
                ii.product_name,
                ii.category,

                case 
                    when ii.invoice_type = 1 then 'credit note' 
                    when ii.invoice_type = 0 then 'invoice'
                    when ii.price_without_tax < 0 then 'credit note'
                    when ii.price_without_tax > 0 then 'invoice' 
                    else 'check' 
                end as invoice_item_type,

                ii.invoice_type as invoice_item_type_row,

                --supplier
                ii.meta_data.supplier as meta_supplier,
                ii.meta_data.supplier_code as meta_supplier_code,
                ii.meta_data.supplier_name as meta_supplier_name,

            --fct
                ii.quantity,
                ii.unit_price,
                ii.unit_tax,
                ii.total_tax,
                ii.price_without_tax,
                ii.price_without_discount,
                ii.price,
                ii.discount_amount,               



        case 
            when ii.currency in ('SAR') then ii.price_without_tax * 0.26666667
            when ii.currency in ('AED') then ii.price_without_tax * 0.27229408
            when ii.currency in ('KWD') then ii.price_without_tax * 3.256648 
            when ii.currency in ('USD') then ii.price_without_tax
            when ii.currency in ('EUR') then ii.price_without_tax * 1.0500713
            when ii.currency in ('QAR', 'QR') then ii.price_without_tax * 0.27472527
            when ii.currency is null then ii.price_without_tax * 0.27229408
            end as usd_price_without_tax,




current_timestamp() as ingestion_timestamp,

from source as ii
where ii.deleted_at is null and  ii.__hevo__marked_deleted is not true