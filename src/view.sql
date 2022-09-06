CREATE OR REPLACE VIEW Vendas(ean, cat, ano, trimestre, mes, dia_mes, dia_semana, distrito, concelho, unidades)
AS 
SELECT  prod.ean, cat,
    EXTRACT(YEAR FROM instante) AS ano, 
    EXTRACT(QUARTER FROM instante) AS trimestre,
    EXTRACT(MONTH FROM instante) AS mes,
    EXTRACT(DAY FROM instante) AS dia_mes,
    EXTRACT(DOW FROM instante) AS dia_semana,
    distrito, concelho, unidades
FROM ponto_de_retalho AS pret
    INNER JOIN
    instalada_em AS ins
    ON pret.nome = ins.localizacao
    INNER JOIN
    evento_de_reposicao AS evrep
    ON ins.num_serie = evrep.num_serie AND ins.fabricante = evrep.fabricante
    INNER JOIN
    produto AS prod
    ON evrep.ean = prod.ean;
