/**
Qual o nome do retalhista (ou retalhistas) responsáveis
pela reposição do maior número de categorias?
**/

SELECT nome
FROM (
    SELECT nome, responsavel_por.tin, COUNT(DISTINCT nome_cat) AS num_cat
    FROM responsavel_por INNER JOIN retalhista
    ON responsavel_por.tin = retalhista.tin
    GROUP BY nome, responsavel_por.tin
) AS aux
WHERE num_cat >= ALL (
    SELECT COUNT(nome_cat) AS num_cat
    FROM responsavel_por
    GROUP BY tin
);

/**
Qual o nome do ou dos retalhistas que são responsáveis
por todas as categorias simples?
**/

SELECT nome
FROM retalhista AS rt
WHERE NOT EXISTS (
    SELECT nome AS nome_cat
    FROM categoria_simples
    EXCEPT
    SELECT nome_cat
    FROM (retalhista AS ret
        INNER JOIN
        responsavel_por AS res
        ON ret.tin = res.tin) AS rs
    WHERE rt.nome = rs.nome
);


/**
Quais os produtos (ean) que nunca foram repostos?
**/

SELECT ean
FROM produto
WHERE ean NOT IN (
    SELECT ean
    FROM evento_de_reposicao
);

/**
Quais os produtos (ean) que foram repostos sempre pelo mesmo retalhista?
**/

SELECT ean
FROM (
    SELECT ean, COUNT(DISTINCT tin) AS num_ret
    FROM evento_de_reposicao
    GROUP BY ean
) AS aux
WHERE num_ret = 1;
