import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """
    Carries out the copy table queries.
    
    Parameters:
    - cur: Cursor - Allows Python code to execute PostgreSQL command in a database session.
    - conn: Connection - The database connector.
    """
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """
    Carries out the insert table queries.
    
    Parameters:
    - cur: Cursor - Allows Python code to execute PostgreSQL command in a database session.
    - conn: Connection - The database connector.
    """
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """
    Executes the copy and insert table queries.
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()