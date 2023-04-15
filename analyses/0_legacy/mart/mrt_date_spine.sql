select 
date_in_range,
day_number,
week_number,
min(date_in_range) over (partition by week_number) week_start,
max(date_in_range) over (partition by week_number) week_end,
month_start,
month_end,
from (SELECT 
	date_in_range,
	date_diff(date_in_range, cast('2019-07-01' as date), DAY)+1 as day_number,
	cast(trunc(date_diff(date_in_range, cast('2019-07-01' as date), DAY)/7)+1 as int64) as week_number,
  date_trunc( date_in_range, MONTH) month_start,
  last_day (date_in_range) as month_end,
	FROM UNNEST(
    	GENERATE_DATE_ARRAY(DATE('2019-07-01'), CURRENT_DATE(), INTERVAL 1 DAY)
	) AS date_in_range);