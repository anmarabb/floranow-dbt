

where date(delivery_date) = current_date() -1  --yesterday 
where date(delivery_date) = current_date()     --Today
where date(delivery_date) > current_date()     --Future
where date(delivery_date) < current_date()     --Past
where date_diff(date(delivery_date)  ,current_date(), month) = 0 --MTD