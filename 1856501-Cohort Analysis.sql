count(DISTINCT CASE WHEN revenue_changes.change_type = 'removed' THEN revenue_changes.subscription_id END) AS churned_subscriptions

lag(subscriptions) OVER (ORDER BY s.month) AS last_month_subscriptions,


date_trunc('month', invoices.date) AS month,
count(DISTINCT invoices.invoice_id) AS orders,
count(DISTINCT invoices.customer_id) AS customers,
sum(amount_dollars) AS mrr

s.month,
s.orders,
s.customers,
s.mrr,
GROUP BY 1,2,3


month,
(mrr / customers) AS arpc,
(churned_subscriptions / last_month_subscriptions::float) AS subscription_churn_rate,
(mrr / subscriptions) / (churned_subscriptions / last_month_subscriptions::float) AS ltv