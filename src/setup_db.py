import psycopg2
import psycopg2.extras

DB_HOST="db.tecnico.ulisboa.pt"
DB_USER="" # insert database user
DB_DATABASE=DB_USER
DB_PASSWORD="" # insert database password
DB_CONNECTION_STRING = "host=%s dbname=%s user=%s password=%s" % (DB_HOST, DB_DATABASE, DB_USER, DB_PASSWORD)

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


try:
    connection = psycopg2.connect(DB_CONNECTION_STRING)
    cursor = connection.cursor(cursor_factory = psycopg2.extras.DictCursor)
    destroy_db_data()
    run_commands_from_file("rm-core.sql")
    run_commands_from_file("ICs.sql")
    run_commands_from_file("populate.sql")
    print("Success!")
except Exception as e:
    print("Something went wrong. Reason: ")
    print(e)
