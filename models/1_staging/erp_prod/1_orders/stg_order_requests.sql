With source as (
 select * from {{ source(var('erp_source'), 'order_requests') }}
)
select 



id,
supplier_id,
feed_source_id,
standing_order_id,
order_builder_id,


approved_by_id,
--requested_by_id,
created_by_id,
rejected_by_id,
canceled_by_id,
customer_id,


quantity,
confirmed_quantity,
ordered_quantity,

currency,
price,

product_id,
status,

delivery_date,

created_at,
updated_at,
departure_date,
rejected_at,
--reject_reason,
failure_at,
failure_reason,
approved_at,
canceled_at,

product_type,
product_name,
--product_selection_type,
color,
stem_length,
head_size,

--commercial_taxon,
--note,


fob_price,


permalink,
spec2,
rejection_reason,

rejection_note,
generation_version,
product_image,
sales_unit,


requested_price_margin_is_valid,

calculated_final_price,
calculated_price_margin_is_valid,
calculated_avg_fob_price,
calculated_avg_fob_calculated_prices,
calculated_avg_fob_calculated_price_margin_is_valid,

calculated_avg_fob_requested_price_margin_is_valid,

source_product_name,
product_mask,
properties,


current_timestamp() as ingestion_timestamp,
 



 
from source as orr