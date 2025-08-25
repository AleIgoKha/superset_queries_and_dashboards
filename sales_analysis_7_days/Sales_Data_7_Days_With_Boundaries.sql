WITH sales_7_table AS (
	SELECT (report_datetime AT TIME ZONE 'Europe/Chisinau')::date AS date,
		   EXTRACT(DOW FROM report_datetime AT TIME ZONE 'Europe/Chisinau') AS weekday,
		   report_revenue AS revenue,
		   report_purchases AS purchases,
		   ROUND(report_revenue / report_purchases, 2) AS avg_bill
	FROM public.reports
	WHERE outlet_id = 5
	AND (report_datetime AT TIME ZONE 'Europe/Chisinau') > (SELECT MAX(report_datetime AT TIME ZONE 'Europe/Chisinau')
															FROM public.reports
															WHERE outlet_id = 5) - '7 days'::interval
	),

sales_except_7_table AS (
	SELECT (report_datetime AT TIME ZONE 'Europe/Chisinau')::date AS date,
		   EXTRACT(DOW FROM report_datetime AT TIME ZONE 'Europe/Chisinau') AS weekday,
		   report_revenue AS revenue,
		   report_purchases AS purchases,
		   ROUND(report_revenue / report_purchases, 2) AS avg_bill
	FROM public.reports
	WHERE outlet_id = 5
	AND (report_datetime AT TIME ZONE 'Europe/Chisinau') <= (SELECT MAX(report_datetime AT TIME ZONE 'Europe/Chisinau')
															FROM public.reports
															WHERE outlet_id = 5) - '7 days'::interval
															),

revenue_weekdays_boundaries AS (
	SELECT weekday,
		   round(percentile_cont(0.5) WITHIN GROUP (ORDER BY revenue)) AS revenue_median,
		   round(percentile_cont(0.025) WITHIN GROUP (ORDER BY revenue)) AS revenue_lower_bound,
		   round(percentile_cont(0.975) WITHIN GROUP (ORDER BY revenue)) AS revenue_higher_bound,
		   round(percentile_cont(0.5) WITHIN GROUP (ORDER BY purchases)) AS purchases_median,
		   round(percentile_cont(0.025) WITHIN GROUP (ORDER BY purchases)) AS purchases_lower_bound,
		   round(percentile_cont(0.975) WITHIN GROUP (ORDER BY purchases)) AS purchases_higher_bound,
		   round(percentile_cont(0.5) WITHIN GROUP (ORDER BY avg_bill)) AS avg_bill_median,
		   round(percentile_cont(0.025) WITHIN GROUP (ORDER BY avg_bill)) AS avg_bill_lower_bound,
		   round(percentile_cont(0.975) WITHIN GROUP (ORDER BY avg_bill)) AS avg_bill_higher_bound
	FROM sales_except_7_table
	GROUP BY weekday
	)

SELECT date,
	   revenue,
	   revenue_lower_bound,
	   revenue_median,
	   revenue_higher_bound,
	   purchases,
	   purchases_median,
	   purchases_lower_bound,
	   purchases_higher_bound,
	   avg_bill,
	   avg_bill_median,
	   avg_bill_lower_bound,
	   avg_bill_higher_bound
FROM revenue_weekdays_boundaries AS rwb
JOIN sales_7_table AS s ON rwb.weekday = s.weekday
ORDER BY 1 DESC