WITH sales_table AS (
-- данные о продажах торговой точки Зеленый рынок (id = 5) за каждый день сначала ведения статистики
	SELECT (t.transaction_datetime AT TIME ZONE 'UTC' AT TIME ZONE 'Europe/Chisinau')::date AS date,
	       t.transaction_product_name AS product,
	       MAX(t.product_qty) AS sold_qty, -- здесь за каждый день может быть лишь одна такая транзакция так что значения одно
	       p.product_unit,
	       MAX(t.transaction_product_price) AS product_price, -- здесь за каждый день может быть лишь одна такая транзакция так что значения одно
		   t.balance_after
	FROM public.transactions AS t
	JOIN public.stocks AS s ON t.stock_id = s.stock_id
	JOIN public.products AS p ON p.product_id = s.product_id
	WHERE t.transaction_type = 'balance'
	AND t.outlet_id = 5
	GROUP BY t.transaction_product_name,
	         t.transaction_datetime,
	         p.product_unit,
			 t.balance_after
	)
	
SELECT date,
	   product,
	   product_price,
	   sold_qty,
	   product_unit,
	   balance_after,
	   SUM(sold_qty) OVER (PARTITION BY product ORDER BY date ASC) AS total_sold_qty_by_date,
	   COUNT(1) OVER (PARTITION BY product ORDER BY date ASC) AS days_in_stock
FROM sales_table