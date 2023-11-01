import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    """
    Carries out the drop table queries.
    
    Parameters:
    - cur: Cursor - Allows Python code to execute PostgreSQL command in a database session.
    - conn: Connection - The database connector.
    """
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """
    Carries out the create table queries.
    
    Parameters:
    - cur: Cursor - Allows Python code to execute PostgreSQL command in a database session.
    - conn: Connection - The database connector.
    """
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """
    Executes the drop and create table queries.
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()