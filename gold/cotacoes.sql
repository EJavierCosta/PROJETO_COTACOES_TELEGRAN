SELECT
  TIKER,
  PRECO,
  date(DATA) AS DATA,
  date_format(DATA, "HH:mm:ss") AS HORA
FROM prod.cotacoes_silver.cotacoes