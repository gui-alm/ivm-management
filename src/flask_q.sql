/**
a) Inserir e remover categorias e sub-categorias
**/

-- inserir categoria simples

INSERT INTO categoria VALUES (<nome>);

INSERT INTO categoria_simples VALUES (<nome>);

-- inserir sub-categoria

INSERT INTO categoria VALUES (<simples>);

INSERT INTO categoria_simples VALUES (<simples>);

DELETE FROM categoria_simples
WHERE nome = <mae>;

DO $$
BEGIN
    IF <mae> NOT IN (
        SELECT * FROM super_categoria
    ) THEN
        INSERT INTO super_categoria VALUES (<mae>);
    END IF;
END $$;

INSERT INTO tem_outra VALUES (<mae>, <simples>);

-- remover

CREATE TEMP TABLE cat_del (
    categoria varchar(255) NOT NULL
);

INSERT INTO cat_del VALUES (<nome>);

WITH RECURSIVE previous(super_categoria, categoria) AS (
    SELECT super_categoria, categoria
    FROM tem_outra
    WHERE super_categoria = <nome>
    UNION ALL
    SELECT cur.super_categoria, cur.categoria
    FROM tem_outra AS cur, previous
    WHERE cur.super_categoria = previous.categoria
)
INSERT INTO cat_del
SELECT categoria
FROM previous;

DELETE FROM categoria_simples
WHERE nome IN (
    SELECT * FROM cat_del
);

DELETE FROM tem_outra
WHERE categoria IN (
    SELECT * FROM cat_del
);

DELETE FROM super_categoria
WHERE nome IN (
    SELECT * FROM cat_del
);

DELETE FROM tem_categoria
WHERE ean IN (
    SELECT ean
    FROM cat_del
    INNER JOIN produto
    ON cat_del.categoria = produto.cat
);

DELETE FROM responsavel_por
WHERE nome_cat IN (
    SELECT * FROM cat_del
);

DELETE FROM evento_de_reposicao
WHERE ean IN (
    SELECT ean
    FROM cat_del
    INNER JOIN produto
    ON cat_del.categoria = produto.cat
);

DELETE FROM planograma
WHERE ean IN (
    SELECT ean
    FROM cat_del
    INNER JOIN produto
    ON cat_del.categoria = produto.cat
);

DELETE FROM produto
WHERE cat IN (
    SELECT * FROM cat_del
);

DELETE FROM prateleira
WHERE nome IN (
    SELECT * FROM cat_del
);

DELETE FROM categoria
WHERE nome IN (
    SELECT * FROM cat_del
);

DROP TABLE cat_del;

/**
b) Inserir e remover um retalhista, com todos os seus
produtos, garantindo que esta operação seja atómica;
**/

-- inserir retalhista

INSERT INTO retalhista VALUES (<tin>, <nome>);

-- inserir responsabilidade

INSERT INTO responsavel_por VALUES (<nome_cat>, <tin>, <num_serie>, <fabricante>);

-- remover

DELETE FROM responsavel_por
WHERE tin = <tin>;

DELETE FROM evento_de_reposicao
WHERE tin = <tin>;

DELETE FROM retalhista
WHERE tin = <tin>;

/**
d) Listar todas as sub-categorias de uma super-categoria,
a todos os níveis de profundidade.
**/

WITH RECURSIVE previous(super_categoria, categoria) AS (
    SELECT super_categoria, categoria
    FROM tem_outra
    WHERE super_categoria = <nome>
    UNION ALL
    SELECT cur.super_categoria, cur.categoria
    FROM tem_outra AS cur, previous
    WHERE cur.super_categoria = previous.categoria
)
SELECT categoria FROM previous;