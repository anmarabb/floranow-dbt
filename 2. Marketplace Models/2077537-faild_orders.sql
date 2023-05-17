select
distinct florisoft_response.message,
count(*),
--sum(quantity) as quantity,
--avg(price) as avg_price,
sum(quantity * price) as total_price,
from `floranow.marketplace_prod.spree_line_items` as sli
where date_diff(date(created_at)  ,current_date(), year) = 0  and florisoft_response.message is not null
group by 1
order by 3 desc