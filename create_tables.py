import psycopg2
from sql_queries import create_table_queries, drop_table_queries



def create_database():
    """
    - Creates and connects to the sparkifydb
    - Returns the connection and cursor to sparkifydb
    """
    
    # connect to default database
    conn = psycopg2.connect("host=127.0.0.1 dbname=postgres user=postgres password=123456 port=5432") # creates a new database session and returns a new connection instance
    # The class connection encapsulates a database session. It allows to: 
    # (1) create new cursor instances using the cursor() method to execute database commands and queries,
    # (2) terminate transactions using the methods commit() or rollback().
    
    conn.set_session(autocommit=True)  # commit: Make the changes to the database persistent
    
    cur = conn.cursor()
    # The class cursor allows interaction with the database:
    # (1) send commands to the database using methods such as execute() and executemany(),
    # (2) retrieve data from the database by iteration or using methods such as fetchone(), fetchmany(), fetchall().
    
    # create sparkify database with UTF8 encoding
    cur.execute("DROP DATABASE IF EXISTS sparkifydb")
    cur.execute("CREATE DATABASE sparkifydb WITH ENCODING 'utf8' TEMPLATE template0")#template

    # close connection to default database
    conn.close()    
    
    # connect to sparkify database
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=postgres password=123456 port=5432")
    cur = conn.cursor()
    
    return cur, conn


def drop_tables(cur, conn):
    """
    Drops each table using the queries in `drop_table_queries` list.
    """
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """
    Creates each table using the queries in `create_table_queries` list. 
    """
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """
    - Drops (if exists) and Creates the sparkify database. 
    
    - Establishes connection with the sparkify database and gets
    cursor to it.  
    
    - Drops all the tables.  
    
    - Creates all tables needed. 
    
    - Finally, closes the connection. 
    """
    cur, conn = create_database()
    
    #drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()

