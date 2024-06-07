import psycopg2
import logging
from psycopg2 import OperationalError

# Function to connect to PostgreSQL database
def connect_to_db():
    conn = None
    try:
        conn = psycopg2.connect(database="cdi",
                                user="admin",
                                host="postgres",
                                password="admin",
                                port=5432)
        print("Connected to postgres db successfully")
    except OperationalError as e:
        print("Exception connecting to db:", e)
    finally:
        return conn

def execute_sql_command(sql_query, fetch_result=False):
    conn = connect_to_db()
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

def init_tables():
    logging.info("Creating Tables...")

    # Create workflow_definition table
    execute_sql_command("""
        CREATE TABLE IF NOT EXISTS workflow_definition (
            id SERIAL PRIMARY KEY,
            name VARCHAR(255) NOT NULL,
            task_id INTEGER[],
            date_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            user_info VARCHAR(255) NOT NULL
        );""")
    logging.info("Created workflow_definition table!")

    # Create task_definition table
    execute_sql_command("""
        CREATE TABLE IF NOT EXISTS task_definition (
            id SERIAL PRIMARY KEY,
            name VARCHAR(255) NOT NULL,
            date_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            user_info VARCHAR(255) NOT NULL
        );
    """)
    logging.info("Created task_definition table!")

    execute_sql_command("""
        DO $$ 
        BEGIN
            IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'workflow_status') THEN
                CREATE TYPE workflow_status AS ENUM ('STARTED', 'COMPLETED', 'INPROGRESS');
            END IF;
        END $$;
        """)
    logging.info("Created workflow_status enum type")

    # Create workflow_execution table
    execute_sql_command("""
        CREATE TABLE IF NOT EXISTS workflow_execution (
            id SERIAL PRIMARY KEY,
            workflow_id INTEGER REFERENCES workflow_definition(id),
            next_task_id INTEGER,
            request JSON,
            status workflow_status
        );
    """)
    logging.info("Created workflow_execution table!")

    execute_sql_command(""" CREATE TABLE IF NOT EXISTS workers (
        worker_id SERIAL PRIMARY KEY,
        ip VARCHAR(255) NOT NULL,
        port VARCHAR(255) NOT NULL,
        queue_name VARCHAR(255) NOT NULL,
        pool_count INTEGER NOT NULL DEFAULT 2,
        current_pool INTEGER NOT NULL DEFAULT 0,
        process_id VARCHAR(255) NOT NULL
    ); """)
    logging.info("Created workers table!")

    logging.info("Tables created successfully!")