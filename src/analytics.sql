/**
num dado período (i.e. entre duas datas), por
dia da semana, por concelho e no total
**/

SELECT dia_semana, concelho, SUM(unidades) AS total
FROM Vendas
WHERE ano = '2022' AND mes BETWEEN 1 AND 3
GROUP BY
GROUPING SETS((dia_semana), (concelho), ());

/**
num dado distrito (i.e. “Lisboa”), por concelho,
categoria, dia da semana e no tota
**/

SELECT concelho, cat, dia_semana, SUM(unidades) AS total
FROM Vendas
WHERE distrito = 'DISTRITO 5'
GROUP BY
GROUPING SETS((concelho), (cat), (dia_semana), ());