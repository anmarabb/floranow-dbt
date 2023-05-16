id,
name,	---default inventory stock, default flying stock, Floranow Reselling flying (Hidden)

warehouse_id,
seller_id,
out_feed_source_id,
input_stock_id,
stock_merging_rule_id,
reseller_id,

stock_type, --0 = inventory stock  or 1 = flying stock
status, --0= visible or 1= hidden


created_at,
updated_at,
deleted_at,

entries_type,
default,	--true or false
custom_sales_unit,
has_custom_sales_unit,
availability_type, ---NORMAL or INTERNAL

from `floranow.erp_prod.stocks` as st