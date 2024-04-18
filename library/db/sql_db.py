import psycopg2

from library.common.constants import SQL_DB_ENV, SQL_USER_ENV, SQL_HOST_ENV, SQL_PASSWORD_ENV, SQL_PORT_ENV
from library.common.utils import getenv_with_default


def execute_sql_command(sql_query, fetch_result=False):
    conn = psycopg2.connect(database=getenv_with_default(SQL_DB_ENV, "cdi"),
                            user=getenv_with_default(SQL_USER_ENV, "admin"),
                            host=getenv_with_default(SQL_HOST_ENV, "localhost"),
                            password=getenv_with_default(SQL_PASSWORD_ENV, "admin"),
                            port=getenv_with_default(SQL_PORT_ENV, 5432))

    # Open a cursor to perform database operations
    cur = conn.cursor()
    # Execute a commands
    cur.execute(sql_query)
    result = []
    if fetch_result:
        result = cur.fetchall()

    # Make the changes to the database persistent
    conn.commit()
    # Close cursor and communication with the database
    cur.close()
    conn.close()

    return result
