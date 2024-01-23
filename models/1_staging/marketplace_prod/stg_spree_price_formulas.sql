select


formula_type,
vendor_id,
feed_id,
user_id,
price_group_id,
region_id,
product_mask_id,
final_currency,
created_at,
updated_at,
user_filter_type,
product_filter_type,
base_currency_source,
final_currency_source,
apply_currency_conversion,
taxon_id,
is_default,
override_other_formulas,
active,
status,
sync_status,
deleted_at,
product_name,
static_currency,
apply_margin,
expire_date,
property_id,
property_values,
second_property_id,
second_property_values,
seller_feed_id,
seller_type,
seller_id,

formula.value as formula_value,
factors.value as factors_value,



from {{ source('marketplace_prod', 'spree_price_formulas') }}
