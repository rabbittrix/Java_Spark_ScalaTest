########## Proposta #################################
SELECT * FROM [single_ensaio]
         WHERE  ensaio is NULL and (
	LAG(ENSAIO) OVER (PARTITION BY ID_PAINEL LIMIT DURATION(hour, 1) WHEN ENSAIO IS NOT NULL) LIKE 'FIM') 
       or (LAG(ENSAIO) OVER (PARTITION BY ID_PAINEL LIMIT DURATION(hour, 1) WHEN ENSAIO IS NOT NULL) is NULL))
########## Tests #####################################

SELECT  YEAR(OrderDate) Date,
FIRST_VALUE(SUM(SubTotal)) OVER (
ORDER BY YEAR(OrderDate)
) AS Ensaio_E,
LAST_VALUE(SUM(SubTotal)) OVER (
ORDER BY YEAR(OrderDate)
) AS Ensaio_E
FROM Ensaio
GROUP BY YEAR(OrderDate)