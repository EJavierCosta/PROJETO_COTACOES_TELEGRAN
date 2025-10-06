%sql
SELECT
    m.TIKER,
    p.PRECO AS preco_atual,
    ROUND(((p.PRECO - m.media_7dias) / m.media_7dias) * 100, 2) AS variacao_percent_7d,
    ROUND(((p.PRECO - m.media_30dias) / m.media_30dias) * 100, 2) AS variacao_percent_30d,
    ROUND(((p.PRECO - m.media_90dias) / m.media_90dias) * 100, 2) AS variacao_percent_90d
FROM
    (
        SELECT 
            TIKER,
            ROUND(AVG(CASE WHEN DATA >= DATE_SUB(current_date(), 90) THEN PRECO END), 2) AS media_90dias,
            ROUND(AVG(CASE WHEN DATA >= DATE_SUB(current_date(), 30) THEN PRECO END), 2) AS media_30dias,
            ROUND(AVG(CASE WHEN DATA >= DATE_SUB(current_date(), 7)  THEN PRECO END), 2) AS media_7dias
        FROM prod.cotacoes_silver.hist_cotacoes
        GROUP BY TIKER
    ) AS m
JOIN
    prod.cotacoes_silver.cotacoes AS p ON m.TIKER = p.TIKER
ORDER BY
    m.TIKER;