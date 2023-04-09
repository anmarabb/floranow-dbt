select
--li.order_type,

li.id,
li.order_type,

li.unit_price,
li.quantity,



total_price_without_tax,

/*
total_tax,
calculated_price,
unit_additional_cost,
unit_shipment_cost,
li.currency,
unit_fob_price,
fob_currency,
exchange_rate,
unit_landed_cost,
li.landed_currency,
price_margin,
case when li.fob_currency = li.landed_currency then 'currency_alignment' else 'currency_exchange' end as exchange_chaeck,
--unit_tax,
--total_price_include_tax,
li.unit_price * li.quantity as ch_total_price_without_tax,
calculated_unit_price,
case when li.unit_price * li.quantity = total_price_without_tax then 'ok' else 'not_ok' end as cheack,
*/


case when li.quantity * li.unit_price != total_price_without_tax then 'price_flag_1' else null end as price_checker,
from `floranow.erp_prod.line_items` as li

where li.quantity * li.unit_price != total_price_without_tax
order by 2
--where calculated_unit_price != unit_price
--where li.unit_price * li.quantity != total_price_without_tax
--where li.fob_currency != li.landed_currency and li.landed_currency != li.currency


--cost_of_goods_sold