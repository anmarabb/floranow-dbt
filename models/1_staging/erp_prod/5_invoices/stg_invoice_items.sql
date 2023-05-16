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
                ii.created_at as invoice_item_created_at,  --proforma_at,
                ii.updated_at,
                ii.deleted_at,
                ii.delivery_date,
                ii.order_date,


                --dim
                ii.source_type,
                ii.generation_type,
                case when ii.invoice_type = 1 then 'credit note' else 'invoice' end as invoice_item_type,
                ii.currency,
                ii.creditable_type,
                ii.status as invoice_item_status, --APPROVED, CANCELED, DRAFT, REJECTED
                ii.number,
                ii.product_name,
                ii.category,

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





current_timestamp() as ingestion_timestamp,

from source as ii
where ii.deleted_at is null 