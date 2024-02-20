
select

*

from   {{ ref('stg_fm_inbound_stock_items') }} as ins
