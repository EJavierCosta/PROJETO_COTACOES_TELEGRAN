SELECT Simbolo AS TIKER,
       Cotacao AS PRECO,
       date(DATA) AS DATA,
       date_format(DATA, "HH:mm:ss") AS HORA

FROM prod.cotacoes_bronze.cotacoes