SELECT
ii.debtor_number,
week_start,
week_end,
week_number,
sum(CASE WHEN ii.printed_at >= date_spine.week_start AND ii.printed_at <= date_spine.week_end THEN 1 ELSE 0 END) as transactions_created

 
FROM `floranow.Floranow_ERP.date_spine` as date_spine
JOIN `floranow.Floranow_ERP.invoices_items`  as ii 
ON date_spine.date_in_range = ii.printed_at
GROUP BY 1, 2, 3, 4