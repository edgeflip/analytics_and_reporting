import psycopg2
import static_values

def redshift_connect():
    return psycopg2.connect( **static_values.redshift)

def redshift_disconnect(connection):
    connection.close()

def execute_query(query, connection, fetchable=True):
    cursor = connection.cursor()
    cursor.execute(query)
    if fetchable:
        rows = cursor.fetchall()
    connection.commit()
    cursor.close()
    if fetchable:
        return rows

def does_table_exist(table_name, conn):
    query = """
            SELECT count(*)
            FROM information_schema.tables
            WHERE table_name = '{table_name}'
            """.format(table_name=table_name)
    return execute_query(query, conn)[0][0]

