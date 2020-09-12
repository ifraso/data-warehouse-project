import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """
    Connects to database and loads the staging tables.

    Args:
        cur: Cursor.
        conn: Connection string.

    Returns:
        True for success, False otherwise.
    """

    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """
    Connects to database and loads the target tables.

    Args:
        cur: Cursor.
        conn: Connection string.

    Returns:
        True for success, False otherwise.
    """
    
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """
    Main function.

    Returns:
        True for success, False otherwise.
    """

    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['DB'].values()))
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()