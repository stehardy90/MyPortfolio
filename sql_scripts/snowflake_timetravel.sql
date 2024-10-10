SELECT 
    SUPPLIER_ID
    ,SUM(GROSS_AMOUNT) AS SALES
FROM 
    FACT.SALES AT (OFFSET => -3600) -- -- Time travel to 1 hour ago (3600 seconds)
GROUP BY 
    SUPPLIER_ID
