With source as (
 select * from {{ source('erp_prod', 'order_payloads') }}
)
select 
id as order_payload_id,
third_party_request,
third_party_response,
created_at,
updated_at,
status,
response_code,
meta_data,

marketplace_request.user_id, 
marketplace_request.customer_id, 
marketplace_request.order_type, 
marketplace_request.tags, 
marketplace_request.offer_id,

marketplace_request.extra_info.shopping_cart_info.shopping_cart_item_id,
marketplace_request.packaging.name,
marketplace_request.price.unit_price,


job_id,


current_timestamp() as ingestion_timestamp,
 




from source as opl