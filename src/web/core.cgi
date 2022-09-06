#!/usr/bin/python3
from multiprocessing import connection
from wsgiref.handlers import CGIHandler
from flask import Flask, render_template, request
import psycopg2
import psycopg2.extras

app = Flask(__name__)

DB_HOST="db.tecnico.ulisboa.pt"
DB_USER="" 
DB_DATABASE=DB_USER
DB_PASSWORD=""
DB_CONNECTION_STRING = "host=%s dbname=%s user=%s password=%s" % (DB_HOST, DB_DATABASE, DB_USER, DB_PASSWORD)

@app.route("/")
def home():
    return render_template("home.html", params=request.args)

@app.route("/inserir", methods = ["POST"])
def insert_simple_category():
    try:
        connection = psycopg2.connect(DB_CONNECTION_STRING)
        cursor = connection.cursor(cursor_factory=psycopg2.extras.DictCursor)
        nome = request.form['categoria']
        qry1 = "INSERT INTO categoria VALUES (%s);"
        qry2 = "INSERT INTO categoria_simples VALUES (%s);"
        cursor.execute(qry1, (nome,))
        cursor.execute(qry2, (nome,))
        connection.commit()
        return render_template("success.html")
    except Exception as e:
        print("Something went wrong. Reason: ")
        print(e)
        return str(e)
    finally:
        cursor.close()
        connection.close()

@app.route("/inserir-sub-categoria", methods=['POST'])
def insert_sub_category():
    try:
        connection = psycopg2.connect(DB_CONNECTION_STRING)
        cursor = connection.cursor(cursor_factory=psycopg2.extras.DictCursor)
        simples = request.form['simples']
        mae = request.form['mae']
        qry = "INSERT INTO categoria VALUES (%s);\
        INSERT INTO categoria_simples VALUES (%s);\
        DELETE FROM categoria_simples\
        WHERE nome = %s;\
        DO $$\
        BEGIN\
        IF %s NOT IN (\
        SELECT * FROM super_categoria\
        ) THEN\
        INSERT INTO super_categoria VALUES (%s);\
        END IF;\
        END $$;\
        INSERT INTO tem_outra VALUES (%s, %s);"
        cursor.execute(qry, (simples, simples, mae, mae, mae, mae, simples))
        connection.commit()
        return render_template("success.html")
    except Exception as e:
        print("Something went wrong. Reason: ")
        print(e)
        return str(e)
    finally:
        cursor.close()
        connection.close()

@app.route("/remover-categoria", methods=['POST'])
def remove_category():
    try:
        connection = psycopg2.connect(DB_CONNECTION_STRING)
        cursor = connection.cursor(cursor_factory=psycopg2.extras.DictCursor)
        categoria = request.form['cat_remover']
        qry = "CREATE TEMP TABLE cat_del (\
        categoria varchar(255) NOT NULL\
        );\
        INSERT INTO cat_del VALUES (%s);\
        WITH RECURSIVE previous(super_categoria, categoria) AS (\
        SELECT super_categoria, categoria\
        FROM tem_outra\
        WHERE super_categoria = %s\
        UNION ALL\
        SELECT cur.super_categoria, cur.categoria\
        FROM tem_outra AS cur, previous\
        WHERE cur.super_categoria = previous.categoria\
        )\
        INSERT INTO cat_del\
        SELECT categoria\
        FROM previous;\
        DELETE FROM categoria_simples\
        WHERE nome IN (\
        SELECT * FROM cat_del\
        );\
        DELETE FROM tem_outra\
        WHERE categoria IN (\
        SELECT * FROM cat_del\
        );\
        DELETE FROM super_categoria\
        WHERE nome IN (\
        SELECT * FROM cat_del\
        );\
        DELETE FROM tem_categoria\
        WHERE ean IN (\
        SELECT ean\
        FROM cat_del\
        INNER JOIN produto\
        ON cat_del.categoria = produto.cat\
        );\
        DELETE FROM responsavel_por\
        WHERE nome_cat IN (\
        SELECT * FROM cat_del\
        );\
        DELETE FROM evento_de_reposicao\
        WHERE ean IN (\
        SELECT ean\
        FROM cat_del\
        INNER JOIN produto\
        ON cat_del.categoria = produto.cat\
        );\
        DELETE FROM planograma\
        WHERE ean IN (\
        SELECT ean\
        FROM cat_del\
        INNER JOIN produto\
        ON cat_del.categoria = produto.cat\
        );\
        DELETE FROM produto\
        WHERE cat IN (\
        SELECT * FROM cat_del\
        );\
        DELETE FROM prateleira\
        WHERE nome IN (\
        SELECT * FROM cat_del\
        );\
        DELETE FROM categoria\
        WHERE nome IN (\
        SELECT * FROM cat_del\
        );\
        DROP TABLE cat_del;"
        cursor.execute(qry, (categoria, categoria))
        connection.commit()
        return render_template("success.html")
    except Exception as e:
        print("Something went wrong. Reason: ")
        print(e)
        return str(e)
    finally:
        cursor.close()
        connection.close()

@app.route("/inserir-retalhista", methods = ['POST'])
def insert_retalhista():
    try:
        connection = psycopg2.connect(DB_CONNECTION_STRING)
        cursor = connection.cursor(cursor_factory=psycopg2.extras.DictCursor)
        tin = request.form["ret_tin"]
        nome = request.form["ret_nome"]
        categoria = request.form["ret_cat"]
        num_serie = request.form["ret_num"]
        fabricante = request.form["ret_fab"]
        qry = "INSERT INTO retalhista VALUES (%s, %s);"
        qry2 = "INSERT INTO responsavel_por VALUES (%s, %s, %s, %s);"
        cursor.execute(qry, (tin, nome))
        cursor.execute(qry2, (categoria, tin, num_serie, fabricante))
        connection.commit()
        return render_template("success.html")
    except Exception as e:
        print("Something went wrong. Reason: ")
        print(e)
        return str(e)
    finally:
        cursor.close()
        connection.close()

@app.route("/remover-retalhista", methods = ['POST'])
def remove_retalhista():
    try:
        connection = psycopg2.connect(DB_CONNECTION_STRING)
        cursor = connection.cursor(cursor_factory=psycopg2.extras.DictCursor)
        tin = request.form["rem_tin"]
        qry = "DELETE FROM responsavel_por\
        WHERE tin = %s;\
        DELETE FROM evento_de_reposicao\
        WHERE tin = %s;\
        DELETE FROM retalhista\
        WHERE tin = %s;"
        cursor.execute(qry, (tin, tin, tin))
        cursor.commit()
        return render_template("success.html")
    except Exception as e:
        print("Something went wrong. Reason: ")
        print(e)
        return str(e)
    finally:
        cursor.close()
        connection.close()

@app.route('/eventos', methods = ["POST"])
def eventos():
    try:
        connection = psycopg2.connect(DB_CONNECTION_STRING)
        cursor = connection.cursor(cursor_factory = psycopg2.extras.DictCursor)
        num = request.form['ivm_number']
        fab = request.form['ivm_fabricante']
        if(num == "" or fab == ""):
            return render_template("input_error.html", input_error_message = "Por favor insira uma combinação (num_serie, fabricante) válida.")
        qry = "SELECT SUM(unidades) FROM evento_de_reposicao WHERE (num_serie = %s AND fabricante = %s);"
        cursor.execute(qry, (num, fab))
        n_sum = cursor.fetchall()[0]
        if(n_sum[0] == None):
            return render_template("no_events.html", num_serie=num, fabricante=fab)
        qry = "SELECT nome FROM prateleira WHERE (num_serie = %s AND fabricante = %s);"
        cursor.execute(qry, (num, fab))
        nome_categoria = cursor.fetchall()[0]
        qry = "SELECT * FROM evento_de_reposicao WHERE (num_serie = %s AND fabricante = %s);"
        cursor.execute(qry, (num, fab))
        return render_template("events.html", cursor=cursor, num_serie=num, fabricante=fab, sum_cats=n_sum[0], nome_cat=nome_categoria[0])
    except Exception as e:  
        print("Something went wrong. Reason: ")
        print(e)
        return(str(e))
    finally:
        cursor.close()
        connection.close()

@app.route("/sub-categorias", methods=['POST'])
def list_sub_categories():
    try:
        connection = psycopg2.connect(DB_CONNECTION_STRING)
        cursor = connection.cursor(cursor_factory=psycopg2.extras.DictCursor)
        super_cat = request.form['super_cat']
        qry = "WITH RECURSIVE previous(super_categoria, categoria) AS (\
        SELECT super_categoria, categoria\
        FROM tem_outra\
        WHERE super_categoria = %s\
        UNION ALL\
        SELECT cur.super_categoria, cur.categoria\
        FROM tem_outra AS cur, previous\
        WHERE cur.super_categoria = previous.categoria\
        )\
        SELECT categoria FROM previous;"
        cursor.execute(qry, (super_cat,))
        return render_template("sub_categories.html", cursor=cursor, super_categoria=super_cat)
    except Exception as e:
        print("Something went wrong. Reason: ")
        print(e)
        return str(e)
    finally:
        cursor.close()
        connection.close()


CGIHandler().run(app)