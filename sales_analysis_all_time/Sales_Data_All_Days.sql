SELECT (report_datetime AT TIME ZONE 'Europe/Chisinau')::date AS date,
	   report_revenue AS revenue,
	   report_purchases AS purchases
FROM public.reports
WHERE outlet_id = 5