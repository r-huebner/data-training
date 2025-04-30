"""
ETL Pipeline for AWS Redshift Data Warehouse

This script orchestrates the end-to-end process of loading, transforming, 
and validating data in an AWS Redshift data warehouse.

Functions:
    - load_staging_tables: Copies raw data from S3 into Redshift staging tables.
    - insert_tables: Transforms and inserts data into the star schema.
    - run_data_quality_checks: Validates data integrity through customizable SQL checks.
    - run_sample_queries: Executes predefined analytical queries for verification.
    - main: Manages the configuration, database connection, and ETL workflow.

Resources Used:
    - Configuration: 'dwh.cfg' file with Redshift credentials and connection parameters.
    - SQL Statements: imported queries from the 'sql_queries' module 
"""
import configparser
import psycopg2
import pandas as pd
from sql_queries import copy_table_queries, insert_table_queries, sample_queries, check_queries


def load_staging_tables(cur, conn):
    """
    Loads data from S3 into Redshift staging tables using COPY commands.

    Parameters:
        - cur: Cursor object for executing SQL queries.
        - conn: Database connection object.
    """
    print(f"\nCopying into staging tables...")
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()
    print(f"Copied.")


def insert_tables(cur, conn):
    """
    Inserts data from staging tables into analytics (fact and dimension) tables.

    Parameters:
        - cur: Cursor object for executing SQL queries.
        - conn: Database connection object.
    """
    print(f"\nInserting tables...")
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()
    print(f"Inserted.")

def run_data_quality_checks(cur):
    """
    Runs a series of data quality checks against a database connection.

    Each check is a dictionary containing:
        - 'description' (str): A short description of the check.
        - 'sql' (str): A SQL SELECT statement that returns a single value to validate.
        - 'expected_result' (callable): A function that takes the SQL result and returns True if valid, otherwise False.

    Parameters:
        - cur: Cursor object for executing SQL queries.
    """
    print("\nChecking data quality...")
    for check in check_queries:
        cur.execute(check['sql'])
        result = cur.fetchone()[0]
        if not check['expected_result'](result):
            print(f"FAILED: {check['description']}. Got {result}")
        print(f"PASSED: {check['description']}; Count: {result}")


def run_sample_queries(cur):
    """
    Executes sample analytical queries and prints the results.

    Parameters:
        - cur: Cursor object for executing SQL queries.
    """
    print("\nRunning sample analytical queries:")
    for desc, query in sample_queries.items():
        print(f"\nQuery: {desc}")
        cur.execute(query)
        results = cur.fetchall()

        # Get column names
        colnames = [desc[0] for desc in cur.description]

        # Use column names in the DataFrame
        df = pd.DataFrame(results, columns=colnames)

        print(f"\nResult:")
        print(df)
        print("-" * 60)


def main():
    """
    Main entry point of the ETL pipeline.
    
    Reads database configuration, connects to Redshift, runs staging and insert operations 
    and sample queries, then closes the connection.
    """

    print("\nStarting ETL Pipeline:")
    config = configparser.ConfigParser()
    config.read('src/sparkify_redshift/dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)
    run_data_quality_checks(cur)
    run_sample_queries(cur)

    conn.close()

    print("\nETL Pipeline finished sucessfully.")


if __name__ == "__main__":
    main()