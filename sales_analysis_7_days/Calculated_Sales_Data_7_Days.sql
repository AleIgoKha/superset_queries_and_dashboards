WITH sales_table AS (
-- данные о продажах торговой точки Зеленый рынок (id = 5) за каждый день
	SELECT (t.transaction_datetime AT TIME ZONE 'Europe/Chisinau')::date AS date,
	       t.transaction_product_name AS product,
		   t.stock_id,
	       MAX(t.product_qty) AS sold_qty, -- здесь за каждый день может быть лишь одна такая транзакция так что значения одно
	       p.product_unit,
	       MAX(t.transaction_product_price) AS product_price, -- здесь за каждый день может быть лишь одна такая транзакция так что значения одно
		   t.balance_after
	FROM public.transactions AS t
	JOIN public.stocks AS s ON t.stock_id = s.stock_id
	JOIN public.products AS p ON p.product_id = s.product_id
	WHERE t.transaction_type = 'balance'
	AND t.outlet_id = 5
	AND (transaction_datetime AT TIME ZONE 'Europe/Chisinau')::date > (SELECT MAX(report_datetime AT TIME ZONE 'Europe/Chisinau')
																		FROM public.reports
																		WHERE outlet_id = 5) - '7 days'::interval
	GROUP BY t.stock_id,
			 t.transaction_product_name,
	         t.transaction_datetime,
	         p.product_unit,
			 t.balance_after
	),

main_sales_figures_table AS (
	SELECT weekday,
		   ROUND(AVG(product_revenue), 3) AS avg_product_revenue,
		   ROUND(MAX(product_revenue), 3) AS max_product_revenue,
		   ROUND(MIN(product_revenue), 3) AS min_product_revenue
	FROM (
		SELECT (transaction_datetime AT TIME ZONE 'Europe/Chisinau')::date AS date,
			   EXTRACT(DOW FROM (transaction_datetime AT TIME ZONE 'Europe/Chisinau')::date) AS weekday,
			   SUM(product_qty * transaction_product_price) AS product_revenue
		FROM public.transactions
		WHERE transaction_type = 'balance'
		AND outlet_id = 5
		AND (transaction_datetime AT TIME ZONE 'Europe/Chisinau')::date <= (SELECT MAX(report_datetime AT TIME ZONE 'Europe/Chisinau')
																			 FROM public.reports
																			 WHERE outlet_id = 5) - '7 days'::interval
		GROUP BY (transaction_datetime AT TIME ZONE 'Europe/Chisinau')::date,
				 EXTRACT(DOW FROM (transaction_datetime AT TIME ZONE 'Europe/Chisinau')::date)
		ORDER BY 1 DESC
		)
	GROUP BY weekday)

SELECT date,
	   product,
	   product_price,
	   product_unit,
	   sold_qty,
	   balance_after,
	   SUM(sold_qty) OVER (PARTITION BY product ORDER BY date ASC) AS total_sold_qty_by_date,
	   COUNT(1) OVER (PARTITION BY product ORDER BY date ASC) AS days_in_stock,
	   min_product_revenue,
	   avg_product_revenue,
	   max_product_revenue
FROM sales_table AS s
JOIN main_sales_figures_table AS msf ON EXTRACT(DOW FROM s.date) = msf.weekday
ORDER BY 2 ASC, 1 ASC