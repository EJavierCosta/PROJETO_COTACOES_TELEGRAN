SELECT 
    TIKER,
    ROUND(AVG(CASE WHEN DATA >= DATE_SUB(current_date(), 90) THEN PRECO END), 2) AS media_90dias,
    ROUND(AVG(CASE WHEN DATA >= DATE_SUB(current_date(), 30) THEN PRECO END), 2) AS media_30dias,
    ROUND(AVG(CASE WHEN DATA >= DATE_SUB(current_date(), 7)  THEN PRECO END), 2) AS media_7dias
FROM prod.cotacoes_silver.hist_cotacoes
GROUP BY TIKER
ORDER BY TIKER;