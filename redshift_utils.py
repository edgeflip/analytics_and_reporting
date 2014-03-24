from logging import debug, info
import psycopg2

DOES_NOT_EXIST_MESSAGE_TEMPLATE = '"{0}" does not exist'

def deploy_table(table, staging_table, old_table, cursor, connection):
    # insurance in case the old table didn't get dropped before
    drop_table_if_exists(old_table, connection, cursor)

    # swap the staging table with the real one
    with connection:
        try:
            cursor.execute("ALTER TABLE {0} rename to {1}".format(table, old_table))
        except psycopg2.ProgrammingError as e:
            if DOES_NOT_EXIST_MESSAGE_TEMPLATE.format(table) in e.pgerror:
                info("Production table {0} did not exist, so no renaming was performed.".format(table))
                # roll back the transaction so the second alter can still commence
                connection.rollback()
            else:
                raise

        cursor.execute("ALTER TABLE {0} rename to {1}".format(staging_table, table))

    drop_table_if_exists(old_table, connection, cursor)

# no 'drop table if exists', so just swallow the error
def drop_table_if_exists(table, connection, cursor):
    with connection:
        try:
            debug('Dropping table {}'.format(table))
            cursor.execute("DROP TABLE {0}".format(table))
        except psycopg2.ProgrammingError as e:
            if DOES_NOT_EXIST_MESSAGE_TEMPLATE.format(table) in e.pgerror:
                info("Table {0} did not exist, so no dropping was performed.".format(table))
            else:
                raise
