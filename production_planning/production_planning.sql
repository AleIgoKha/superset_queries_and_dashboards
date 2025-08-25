SELECT (t.transaction_datetime AT TIME ZONE 'Europe/Chisinau')::date AS date,
	   t.transaction_product_name AS product,
	   t.product_qty AS sold_qty,
	   p.product_unit,
	   (t.product_qty > 0.05)::int AS sold,
	   (t.balance_after = 0)::int AS sold_out
FROM public.transactions AS t
JOIN public.stocks AS s ON s.stock_id = t.stock_id
JOIN public.products AS p ON s.product_id = p.product_id
WHERE transaction_type = 'balance'
AND t.outlet_id = 5