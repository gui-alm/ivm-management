from numpy import number
import psycopg2
import psycopg2.extras
import random

DB_HOST="localhost"
DB_USER="guilhermealmeida" 
DB_DATABASE=DB_USER
DB_PASSWORD=""
DB_CONNECTION_STRING = "host=%s dbname=%s user=%s password=%s" % (DB_HOST, DB_DATABASE, DB_USER, DB_PASSWORD)

Categories = ["AGUA", "BOLACHAS", "BOLACHAS DE CHOCOLATE", "BOLOS", "BOLOS DE CHOCOLATE", "BOLOS DE FRUTA", "CHOCOLATE", "GOMAS", "HIGIENE", "REFRIGERANTES", "SANDES", "SNACKS"]
ExistingCategories = list()
Products = list() # (ean, categoria, descricao)
Prateleiras = list()
Planograma = list()

number_of_IVMS = random.randint(25, 30)

populate_queries = list()

def populate_sql():
    fd = open('populate.sql', 'a')
    for q in populate_queries:
        fd.write("\n{}\n".format(q))
    fd.close()

def handle_category_values():
    for el in Categories:
        qry = "INSERT INTO categoria VALUES ('{}');".format(el)
        populate_queries.append(qry)
        cursor.execute(qry)
        connection.commit()

        if el.count(" ") > 0:
            qry = "INSERT INTO categoria_simples VALUES ('{}');".format(el)
            qry2 = "INSERT INTO tem_outra VALUES ('{}', '{}');".format(el.split()[0], el)
            populate_queries.append(qry)   
            populate_queries.append(qry2)
            cursor.execute(qry)
            cursor.execute(qry2)
            connection.commit()
        else:
            qry = "INSERT INTO super_categoria VALUES ('{}');".format(el)
            populate_queries.append(qry)
            cursor.execute(qry)
            connection.commit()

def handle_product_values():
    ean_gen = 0
    for i in range(new_random_value(int, 20, 25)):
        ean_gen += 1
        rc = new_random_value(ExistingCategories)
        qry = "INSERT INTO produto VALUES({}, '{}', '{}');".format(ean_gen, rc, "PRODUTO %s" % (i + 1))
        qry2 = "INSERT INTO tem_categoria VALUES ({}, '{}');".format(ean_gen, rc)
        Products.append( (ean_gen, rc, "PRODUTO %s" % (i + 1)) )
        populate_queries.append(qry)
        populate_queries.append(qry2)
        cursor.execute(qry)
        cursor.execute(qry2)
        connection.commit()

def handle_IVM_values():
    num_serie_gen = 100
    for i in range(number_of_IVMS):
        num_serie_gen += 1
        qry = "INSERT INTO IVM VALUES({}, '{}');".format(num_serie_gen, "FABRICANTE %s" % (i + 1))
        qry2 = "INSERT INTO ponto_de_retalho VALUES('{}', '{}', '{}');"\
            .format("PONTO %s" % (i+1), "DISTRITO %s" % (new_random_value(int,1,18)), "CONCELHO DO PONTO %s" % (i+1))
        qry3 = "INSERT INTO instalada_em VALUES ('{}', '{}', '{}');"\
            .format(num_serie_gen, "FABRICANTE %s" % (i + 1), "PONTO %s" % (i+1))
        populate_queries.append(qry)
        populate_queries.append(qry2)
        populate_queries.append(qry3)
        cursor.execute(qry)
        cursor.execute(qry2)
        cursor.execute(qry3)
        connection.commit()

def handle_prateleira_values():
    data_grab = cursor.execute("SELECT * FROM IVM;")
    data = cursor.fetchall()

    for row in data:
        nro_gen = 1
        num_serie = row[0]
        fabricante = row[1]
        nr_prateleiras = new_random_value(int, 4, 8)
        category = new_random_value(Categories)

        if(category not in ExistingCategories):
            ExistingCategories.append(category)

        for i in range(0, nr_prateleiras):
            height = new_random_value(int, 10, 15)
            qry = "INSERT INTO prateleira VALUES ({}, {}, '{}', {}, '{}');"\
            .format(nro_gen, num_serie, fabricante, height, category)
            Prateleiras.append( (nro_gen, num_serie, fabricante, height, category) )
            nro_gen += 1
            populate_queries.append(qry)
            cursor.execute(qry)
            connection.commit()

def handle_retalhista_values():
    for i in range (number_of_IVMS):
        qry = "INSERT INTO retalhista VALUES ({}, '{}');".format(i + 1, "RETALHISTA %s" % (i + 1))
        populate_queries.append(qry)
        cursor.execute(qry)
        connection.commit()

def handle_responsavelpor_values():
    data_grab = cursor.execute("SELECT * FROM IVM;")
    ivm_data = cursor.fetchall()
    tin = 0

    for item in ivm_data:
        tin += 1
        qry = "SELECT nome FROM prateleira WHERE (num_serie = {} AND fabricante = '{}');".format(item[0], item[1])
        cursor.execute(qry)
        cat = cursor.fetchall()[0]
        categoria = cat[0]
        qry = "INSERT INTO responsavel_por VALUES('{}', {}, {}, '{}');".format(categoria, tin, item[0], item[1])
        populate_queries.append(qry)
        cursor.execute(qry)
        connection.commit()

def handle_planograma_values():
    data_grab = cursor.execute("SELECT * FROM IVM;")
    ivm_data = cursor.fetchall()
     # planograma = (ean, nro, num_serie, fabricante, faces, unidades, loc)
     # produto = (ean, categoria, descricao)
     # prateleira = (nro, num_serie, fabricante, height, category)

    random.shuffle(Products)

    for prateleira in Prateleiras:
        for produto in Products:
            if(produto[1] == prateleira[4]):
                units = new_random_value(int, 7, 12)
                qry = "INSERT INTO planograma VALUES ({}, {}, {}, '{}', {}, {}, {});".format(produto[0], prateleira[0], prateleira[1], prateleira[2], 5, units, 1)
                populate_queries.append(qry)
                cursor.execute(qry)
                connection.commit()
                Planograma.append( (produto[0], prateleira[0], prateleira[1], prateleira[2], 5, units, 1) )

def handle_eventodereposicao_values():
    # planograma = (ean, nro, num_serie, fabricante, faces, unidades, loc)
    for el in Planograma:
        instant = "{}-{}-{}".format(2022, new_random_value(int, 1, 12), new_random_value(int, 1, 30))
        find_tin = "SELECT tin FROM responsavel_por WHERE (nome_cat = '%s' AND num_serie = %s);" % (find_category_of_product(el[0]), el[2])
        cursor.execute(find_tin)
        tin_s = cursor.fetchall()
        tin = tin_s[0]
        qry = "INSERT INTO evento_de_reposicao VALUES ({}, {}, {}, '{}', '{}', {}, {});".format(el[0], el[1], el[2], el[3], instant, new_random_value(int, 3, 7), tin[0])
        populate_queries.append(qry)
        cursor.execute(qry)
        connection.commit()

def find_category_of_product(ean):
    for el in Products:
        if(el[0] == ean):
            return el[1]

def insert_values_into_table(table, values):
    qry = "INSERT INTO {} VALUES ({});".format(table, ''.join(str(e + ",") for e in values))
    populate_queries.append(qry)
    cursor.execute(qry)
    connection.commit()

def new_random_value(type_d, lower_bound = None, upper_bound = None):
    if type_d == int:
        return random.randint(lower_bound, upper_bound)
    if type(type_d) == list:
        return random.choice(type_d)

def run_commands_from_file(filename):
    fd = open(filename, 'r')
    sqlCommands = fd.read()
    fd.close()
    commands = sqlCommands.split(";")
    commands.pop()

    for command in commands:
        try:
            cursor.execute(command)
            connection.commit()
        except Exception as message:
            print("Skipping command: " + message)

def destroy_db_data():
    # CAREFUL !!!!!!!!!!!!!!!! DESTROYS EVERYTHING
    destroy = "\
    DO $$ DECLARE\
    r RECORD;\
    BEGIN\
    FOR r IN (SELECT tablename FROM pg_tables WHERE schemaname = current_schema()) LOOP\
        EXECUTE 'DROP TABLE IF EXISTS ' || quote_ident(r.tablename) || ' CASCADE';\
    END LOOP;\
    END $$;\
    "
    try:
        cursor.execute(destroy)
        connection.commit()
    except Exception as msg:
        print("Error. " + msg.__cause__)

def destroy_populate_sql():
    import os

    if os.path.exists("populate.sql"):
        os.remove("populate.sql")
        print("Deleted populate.sql")
    else:
        print("Skipping destroy_populate_sql")


try:
    connection = psycopg2.connect(DB_CONNECTION_STRING)
    cursor = connection.cursor(cursor_factory = psycopg2.extras.DictCursor)
    destroy_db_data()
    destroy_populate_sql()
    run_commands_from_file("rm-core.sql")
    handle_category_values()
    handle_IVM_values()
    handle_prateleira_values()
    handle_product_values()
    handle_retalhista_values()
    handle_responsavelpor_values()
    handle_planograma_values()
    handle_eventodereposicao_values()
    populate_sql()
    print("Success!")
except Exception as e:
    print("Something went wrong. Reason: ")
    print(e)