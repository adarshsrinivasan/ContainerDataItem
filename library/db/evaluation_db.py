import psycopg2
from library.common.constants import SQL_DB_ENV, SQL_USER_ENV, SQL_HOST_ENV, SQL_PASSWORD_ENV, SQL_PORT_ENV
from library.common.utils import getenv_with_default

def execute_sql_command(sql_query, params=None, fetch_result=False):
    conn = psycopg2.connect(database="eval",
                            user=getenv_with_default(SQL_USER_ENV, "admin"),
                            host=getenv_with_default(SQL_HOST_ENV, "localhost"),
                            password=getenv_with_default(SQL_PASSWORD_ENV, "admin"),
                            port="5432")
    
    # Open a cursor to perform database operations
    cur = conn.cursor()
    
    # Execute the SQL command
    if params:
        cur.execute(sql_query, params)
    else:
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

def create_table():
    sql_query = '''
        CREATE TABLE IF NOT EXISTS stream_metrics (
            stream_id TEXT PRIMARY KEY,
            start_time REAL,
            finish_time REAL,
            throughput REAL
        )
    '''
    execute_sql_command(sql_query)

def add_start_time(stream_id, start_time, finish_time=""):
    sql_query = '''
        INSERT INTO stream_metrics (stream_id, start_time, finish_time)
        VALUES (%s, %s, %s)
        ON CONFLICT (stream_id) DO NOTHING
    '''
    params = (stream_id, start_time, finish_time)
    execute_sql_command(sql_query, params)

def update_finish_time(stream_id, finish_time):
    sql_query = '''
        UPDATE stream_metrics
        SET finish_time = %s
        WHERE stream_id = %s
    '''
    params = (finish_time, stream_id)
    execute_sql_command(sql_query, params)

def add_throughput(stream_id, throughput):
    sql_query = '''
        UPDATE stream_metrics
        SET throughput = %s
        WHERE stream_id = %s
    '''
    params = (throughput, stream_id)
    execute_sql_command(sql_query, params)