SELECT TIKER,
      AVG(PRECO) FILTER(WHERE DATA >= DATE_SUB(current_date, 90)) as media_90dias,
      AVG(PRECO) FILTER(WHERE DATA >= DATE_SUB(current_date, 30)) as media_30dias,
      AVG(PRECO) FILTER(WHERE DATA >= DATE_SUB(current_date, 7)) as media_7dias
FROM prod.cotacoes_silver.hist_cotacoes
GROUP BY TIKER
