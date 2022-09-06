CREATE OR REPLACE FUNCTION  validate_relation()
RETURNS TRIGGER AS
$$
DECLARE
    cat VARCHAR(250);
BEGIN
cat = NEW.super_categoria;

WHILE cat IS NOT NULL LOOP

    IF NEW.categoria = cat THEN
        RAISE EXCEPTION 'Uma Categoria nao pode estar contida em si propria';
    END IF;

    SELECT super_categoria
    INTO cat
    FROM tem_outra as t
    WHERE t.categoria = cat;
END LOOP;
RETURN NEW;
END;     
$$ LANGUAGE plpgsql;

CREATE TRIGGER validate_trigger
BEFORE UPDATE OR INSERT ON tem_outra
FOR EACH ROW EXECUTE PROCEDURE validate_relation();


CREATE OR REPLACE FUNCTION chk_valid_ammount()
RETURNS TRIGGER AS
$$
DECLARE
units INT;

BEGIN
SELECT unidades
INTO units
FROM planograma as p
WHERE p.ean = NEW.ean AND 
        p.nro = NEW.nro AND 
        p.num_serie = NEW.num_serie AND 
        p.fabricante = NEW.fabricante;

IF NEW.unidades > units THEN
    RAISE EXCEPTION 'O numero de unidades repostas num Evento de Reposicao nao pode exceder o numero de unidades especificado no Planograma';
END IF;

RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER chk_valid_ammount_trigger
BEFORE UPDATE OR INSERT ON evento_de_reposicao
FOR EACH ROW EXECUTE PROCEDURE chk_valid_ammount();

CREATE OR REPLACE FUNCTION chk_restock()
RETURNS TRIGGER AS
$$
DECLARE 
shelf_cat varchar(225);
match varchar(225);
BEGIN
SELECT nome
INTO shelf_cat
FROM prateleira as p
WHERE p.nro = NEW.nro AND p.num_serie = NEW.num_serie AND p.fabricante = NEW.fabricante;

SELECT nome
INTO match
FROM tem_categoria as c
WHERE c.ean = NEW.ean AND c.nome = shelf_cat;


IF match IS NULL THEN
    RAISE EXCEPTION 'Um Produto so pode ser reposto numa Prateleira que apresente (pelo menos) uma das Categorias desse produto';
END IF;
RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER chk_restock_trigger
BEFORE UPDATE OR INSERT ON planograma
FOR EACH ROW EXECUTE PROCEDURE chk_restock();