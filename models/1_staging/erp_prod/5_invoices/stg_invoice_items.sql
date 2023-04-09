With source as (
 select * from {{ source('erp_prod', 'invoice_items') }}
)
select 
            --PK
                ii.id as invoice_item_id,
            --FK
            customer_id,
            line_item_id,
            invoice_id,
            creditable_id,
            approved_by_id,
            

            --dim
                --date
                created_at,
                updated_at,
                deleted_at,
                delivery_date,
                order_date,


                --dim
                source_type,
                generation_type,
                case when ii.invoice_type = 1 then 'credit note' else 'invoice' end as invoice_type,
                currency,
                creditable_type,
                status,
                number
                
                product_name,
                category,


                --supplier
                meta_data.supplier as meta_supplier,
                meta_data.supplier_code as meta_supplier_code,
                meta_data.supplier_name as meta_supplier_name,

                --fct
                quantity,
                unit_price,
                unit_tax,

                total_tax,
                price_without_tax,
                price_without_discount,
                price,
                discount_amount,


current_timestamp() as ingestion_timestamp,

from source as ii
where ii.deleted_at is null 