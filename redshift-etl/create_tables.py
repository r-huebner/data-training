"""
Table Setup Script for AWS Redshift Data Warehouse

This script connects to an AWS Redshift cluster and sets up the database schema
by dropping existing tables (if any) and creating new ones.

Functions:
    - drop_tables(cur, conn): Executes all DROP TABLE statements to remove existing tables.
    - create_tables(cur, conn): Executes all CREATE TABLE statements to define the schema.
    - main(): Establishes a database connection using configuration parameters,
      and orchestrates the dropping and creation of tables.

Resources Used:
    - Configuration: 'dwh.cfg' file containing Redshift credentials and connection info.
    - SQL Statements: imported queries from the 'sql_queries' module
"""
import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    print(f"\nDropping tables...")
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()
    print(f"\nDropped.")


def create_tables(cur, conn):
    print(f"\nCreating tables...")
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()
    print(f"\nCreated.")


def main():

    config = configparser.ConfigParser()
    config.read('src/sparkify_redshift/dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()