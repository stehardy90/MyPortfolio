-- Return previous weeks sales by supplier and date
WITH SupplierSales AS (
    SELECT 
        SUPPLIER_ID,
        CAST(SALE_DATETIME AS DATE) AS SALE_DATE,
        SUM(GROSS_AMOUNT)  AS TOTAL_SALES
    FROM 
        FACT.SALES
    WHERE
        SALE_DATE > DATEADD(week,-1,CURRENT_DATE)
    GROUP BY 
        SUPPLIER_ID,
        SALE_DATE
)

-- Dynamically pivot sales results
SELECT *
FROM SupplierSales
PIVOT (
    MIN(TOTAL_SALES) FOR SUPPLIER_ID IN (ANY)
)
ORDER BY SALE_DATE;
