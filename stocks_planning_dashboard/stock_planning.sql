WITH stock_table AS (
	SELECT s.stock_id,
		   s.stock_qty,
		   p.product_unit,
		   p.product_name
	FROM public.stocks AS s
	JOIN public.products AS p ON p.product_id = s.product_id
	WHERE s.outlet_id = 5
	AND s.stock_active = True
	),

last_replenishment_datetime_table AS (
	SELECT stock_id,
		   MAX(transaction_datetime) AS last_replenishment_datetime
	FROM public.transactions
	WHERE transaction_type = 'replenishment'
	AND outlet_id = 5
	GROUP BY stock_id
	),

days_since_last_replenishment_table AS (
	SELECT t.stock_id,
		   (t.transaction_datetime AT TIME ZONE 'Europe/Chisinau')::date AS last_replenishment_date,
		   EXTRACT(DAY FROM now()::date - (t.transaction_datetime AT TIME ZONE 'Europe/Chisinau')) AS days_since_last_replenishment,
		   product_qty AS last_replenishment_qty
	FROM public.transactions AS t
	JOIN last_replenishment_datetime_table AS lrd ON lrd.stock_id = t.stock_id AND lrd.last_replenishment_datetime = t.transaction_datetime
	WHERE t.transaction_type = 'replenishment'
	AND t.outlet_id = 5
	),

median_sold_qty_by_weekdays_table AS (
-- считаем средний спросс по дням неделю для каждого товара
	SELECT stock_id,
		   EXTRACT(DOW FROM transaction_datetime AT TIME ZONE 'Europe/Chisinau') AS weekday,
		   (percentile_cont(0.025) WITHIN GROUP (ORDER BY product_qty))::numeric AS min_product_qty,
		   (percentile_cont(0.5) WITHIN GROUP (ORDER BY product_qty))::numeric AS median_sold_qty,
		   (percentile_cont(0.975) WITHIN GROUP (ORDER BY product_qty))::numeric AS max_product_qty,
		   ROUND((COUNT(1) FILTER (WHERE balance_after = 0)) * 100 / COUNT(1), 2) AS sold_out_rate
	FROM public.transactions
	WHERE transaction_type = 'balance'
	AND outlet_id = 5
	GROUP BY stock_id,
			 EXTRACT(DOW FROM transaction_datetime AT TIME ZONE 'Europe/Chisinau')
	
	UNION ALL

	-- добавляем пропущенные понедельники, чтобы при подсчете спроса на несколько дней вперед с пятницы по субботу не было неправильных значений
	SELECT DISTINCT stock_id,
		   1 AS weekday,
		   0 AS min_product_qty,
		   0 AS median_sold_qty,
		   0 AS max_product_qty,
		   0 AS sold_out_rate
	FROM public.stocks 
	WHERE outlet_id = 5
	-- на случай выхода в понедельник выделяем возможность такого исхода 
	-- (хотя если это будет уж крайне редко, то можно это просто убрать и даже исключить понедельники вообще)
	AND stock_id NOT IN (SELECT stock_id
					    FROM public.transactions
					    WHERE transaction_type = 'balance'
					      AND outlet_id = 5
					      AND EXTRACT(DOW FROM transaction_datetime AT TIME ZONE 'Europe/Chisinau') = 1)
	 ),

demand_raw AS (
	SELECT a.weekday,
		   a.stock_id,
		   a.min_product_qty,
		   a.median_sold_qty,
		   a.max_product_qty,
		   SUM(b.median_sold_qty) AS product_demand,
		   a.sold_out_rate
	FROM median_sold_qty_by_weekdays_table a
	JOIN median_sold_qty_by_weekdays_table b
	ON a.stock_id = b.stock_id
	AND (b.weekday = a.weekday 
		OR b.weekday = (a.weekday + 1) % 7
		OR b.weekday = (a.weekday + 2) % 7)
	GROUP BY a.weekday, a.stock_id, a.min_product_qty, a.median_sold_qty, a.max_product_qty, a.sold_out_rate
	),

demand_shifted AS (
	SELECT *,
    	   LEAD(product_demand) OVER (PARTITION BY stock_id ORDER BY weekday) AS shifted_raw
  	FROM demand_raw
	),
	
product_demand_3_days_forward_table AS (
	SELECT weekday,
		   stock_id,
		   min_product_qty,
		   median_sold_qty,
		   max_product_qty,
		   sold_out_rate,
		   COALESCE(
			   shifted_raw,
			   FIRST_VALUE(product_demand) OVER (PARTITION BY stock_id ORDER BY weekday)
			   ) AS product_demand_3_days_forward
	FROM demand_shifted
	)


SELECT pd.weekday,
	   s.product_name AS product,
	   s.stock_qty,
	   s.product_unit,
	   ds.days_since_last_replenishment,
	   ds.last_replenishment_qty,
	   CASE 
		  WHEN s.product_unit = 'шт.' THEN CEIL(pd.min_product_qty)
		  WHEN s.product_unit = 'кг' THEN ROUND(pd.min_product_qty, 3)
	   END AS min_product_qty,
	   CASE 
		  WHEN s.product_unit = 'шт.' THEN CEIL(pd.median_sold_qty)
		  WHEN s.product_unit = 'кг' THEN ROUND(pd.median_sold_qty, 3)
	   END AS median_sold_qty,
	   CASE 
		  WHEN s.product_unit = 'шт.' THEN CEIL(pd.max_product_qty)
		  WHEN s.product_unit = 'кг' THEN ROUND(pd.max_product_qty, 3)
	   END AS max_product_qty,
	   CASE 
		  WHEN s.product_unit = 'шт.' THEN CEIL(pd.product_demand_3_days_forward)
		  WHEN s.product_unit = 'кг' THEN ROUND(pd.product_demand_3_days_forward, 3)
	   END AS product_demand_3_days_forward,
	   CASE
	     WHEN product_unit = 'шт.' THEN CEIL(pd.product_demand_3_days_forward - s.stock_qty)
	     WHEN product_unit = 'кг' THEN ROUND(pd.product_demand_3_days_forward - s.stock_qty, 3)
	   END AS difference,
	   pd.sold_out_rate
FROM stock_table AS s
JOIN days_since_last_replenishment_table AS ds ON ds.stock_id = s.stock_id
JOIN product_demand_3_days_forward_table AS pd ON pd.stock_id = s.stock_id
