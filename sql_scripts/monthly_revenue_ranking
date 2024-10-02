WITH RankedOrders AS 
(
	SELECT
		CASE 
			WHEN os.order_status_name = 'Cancelled' THEN CAST(YEAR(fo.order_cancelled) AS VARCHAR(4)) + RIGHT('0' + CAST(MONTH(fo.order_cancelled) AS VARCHAR(2)), 2)  -- When the order is cancelled, the gross (Which will be cancellation fee only) is counted on the day of cancellation 
			ELSE CAST(YEAR(do.order_delivery) AS VARCHAR(4)) + RIGHT('0' + CAST(MONTH(do.order_delivery) AS VARCHAR(2)), 2)  -- When the order is not cancelled, revenue is counted on day of delivery
		END AS Period,
		ds.supplier_name,
		SUM(fo.order_gross) AS Total_Revenue,
		ROW_NUMBER() OVER (PARTITION BY
									CASE 
										WHEN os.order_status_name = 'Cancelled' THEN CAST(YEAR(fo.order_cancelled) AS VARCHAR(4)) + RIGHT('0' + CAST(MONTH(fo.order_cancelled) AS VARCHAR(2)), 2)  -- When the order is cancelled, the gross (Which will be cancellation fee only) is counted on the day of cancellation 
										ELSE CAST(YEAR(do.order_delivery) AS VARCHAR(4)) + RIGHT('0' + CAST(MONTH(do.order_delivery) AS VARCHAR(2)), 2)  -- When the order is not cancelled, revenue is counted on day of delivery
									END
							ORDER BY SUM(fo.order_gross) DESC
		) AS RANK_DESC, -- Assigning a row number based on the total revenue, used to return the top 5 suppliers
		ROW_NUMBER() OVER (PARTITION BY
									CASE 
										WHEN os.order_status_name = 'Cancelled' THEN CAST(YEAR(fo.order_cancelled) AS VARCHAR(4)) + RIGHT('0' + CAST(MONTH(fo.order_cancelled) AS VARCHAR(2)), 2)  -- When the order is cancelled, the gross (Which will be cancellation fee only) is counted on the day of cancellation 
										ELSE CAST(YEAR(do.order_delivery) AS VARCHAR(4)) + RIGHT('0' + CAST(MONTH(do.order_delivery) AS VARCHAR(2)), 2)  -- When the order is not cancelled, revenue is counted on day of delivery
									END
							ORDER BY SUM(fo.order_gross) ASC
		) AS RANK_ASC -- As above, used to return the bottom 5 suppliers

	FROM
		dim.orders do

		JOIN fact.orders fo
			ON fo.order_id = do.order_id

		JOIN dim.suppliers ds
			ON ds.supplier_id = do.supplier_id
			AND supplier_is_parent = 1 

		JOIN dim.order_statuses os
			ON os.order_status_id = do.order_status_id

	WHERE
		CASE 
			WHEN os.order_status_name = 'Cancelled' THEN CAST(fo.order_cancelled AS DATE)
			ELSE CAST(do.order_delivery AS DATE) 
		END		
		BETWEEN DATEADD(MONTH, DATEDIFF(MONTH, 0, GETDATE()) - 12, 0) AND GETDATE() -- Restricting the query to only look at the previous 12 months

	GROUP BY
		CASE 
			WHEN os.order_status_name = 'Cancelled' THEN CAST(YEAR(fo.order_cancelled) AS VARCHAR(4)) + RIGHT('0' + CAST(MONTH(fo.order_cancelled) AS VARCHAR(2)), 2)   
			ELSE CAST(YEAR(do.order_delivery) AS VARCHAR(4)) + RIGHT('0' + CAST(MONTH(do.order_delivery) AS VARCHAR(2)), 2)  
		END,
		ds.supplier_name

)

SELECT
  Period,
	Supplier_Name,
	Total_Revenue,
	RANK_DESC as 'Rank'
FROM
	Rankedorders
WHERE 
	RANK_DESC <= 5 -- Return the 5 best performing suppliers
	OR RANK_ASC <= 5 -- Return the 5 worst performing suppliers
ORDER BY 
	Period 
	,RANK_DESC

