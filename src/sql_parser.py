import argparse
import logging
import sys
from pathlib import Path
from typing import Dict, Any, List
import pandas as pd
import os

from sqlmetadata.sqlmetadata import SQLMetadata


LOGGER = logging.getLogger()
LOGGER.setLevel(logging.INFO)

# Create a handler that logs to stdout
stdout_handler = logging.StreamHandler(sys.stdout)
stdout_handler.setLevel(logging.INFO)

# Create a formatter and set it for the handler
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
stdout_handler.setFormatter(formatter)

LOGGER.addHandler(stdout_handler)
LOGGER.propagate = True


def convert_sql_to_metadata(filename: str,
                            sql_stmt: str,
                            database_name: str,
                            cluster_name: str,
                            schema_name: str,
                            table_name: str,
                            source_database_name: str,
                            source_cluster_name: str,
                            dialect: str = None) -> pd.DataFrame:

    metadata = pd.DataFrame(columns=['filename',
                                     'database_name',
                                     'cluster_name',
                                     'schema_name',
                                     'table_name',
                                     'column_name',
                                     'column_data_type',
                                     'expression',
                                     'message',
                                     'source_database_name',
                                     'source_cluster_name',
                                     'source_schema_name',
                                     'source_table_name',
                                     'source_column_name'])

    try:
        lineage = SQLMetadata.extract_sql_statements_lineage(
            sql_stmt=sql_stmt,
            dialect=dialect
        )

        if lineage:

            rows = []
            for fq_table_name, table_details in lineage.items():

                if table_details['columns']:
                    for column_name, column_details in table_details['columns'].items():
                        row = {}
                        row['filename'] = filename
                        row['database_name'] = database_name
                        row['cluster_name'] = cluster_name
                        row['schema_name'] = table_details.get('schema')
                        row['table_name'] = table_details.get('table')
                        row['column_name'] = column_name
                        row['column_data_type'] = column_details.get('data_type') if column_details.get('data_type') else 'NA'

                        if 'lineage' in column_details:

                            for lineage_item in column_details['lineage']:

                                lineage_row = row
                                lineage_row['source_database_name'] = source_database_name
                                lineage_row['source_cluster_name'] = source_cluster_name
                                lineage_row['source_schema_name'] = lineage_item['schema']
                                lineage_row['source_table_name'] = lineage_item['table']
                                lineage_row['source_column_name'] = lineage_item['column']
                                lineage_row['expression'] = lineage_item['expression']
                                lineage_row['filter_type'] = lineage_item['filter_type']
                                lineage_row['filter'] = lineage_item['filter']
                                lineage_row['message'] = lineage_item['message']

                                rows.append(lineage_row)
                else:
                    row = {}
                    row['filename'] = filename
                    row['database_name'] = database_name
                    row['cluster_name'] = cluster_name
                    row['schema_name'] = table_details['schema']
                    row['table_name'] = table_details['table']
                    rows.append(row)

            metadata = pd.concat([metadata, pd.DataFrame(rows)], ignore_index=True)

        else:
            raise Exception(f'Column Lineage not found')

        LOGGER.info(f"Completed sql parsing")
    except Exception as e:
        raise e

    return metadata

def process_sql_file(sql_file: str,
                     database_name: str,
                     cluster_name: str,
                     schema_name: str,
                     source_database_name: str,
                     source_cluster_name: str,
                     dialect: str = None) -> pd.DataFrame:

    LOGGER.info(f"SQL Parsing {sql_file}")

    with open(sql_file, 'r') as file:
        sql_stmt = file.read()

    return convert_sql_to_metadata(filename=sql_file,
                                   sql_stmt=sql_stmt,
                                   database_name=database_name,
                                   cluster_name=cluster_name,
                                   schema_name=schema_name,
                                   table_name=Path(sql_file).stem,
                                   source_database_name=source_database_name,
                                   source_cluster_name=source_cluster_name,
                                   dialect=dialect)

def process(input: str,
            output: str,
            database_name: str,
            cluster_name: str,
            schema_name: str,
            table_name: str,
            source_database_name: str = None,
            source_cluster_name: str = None,
            dialect: str = None):
    dfs = []

    if os.path.isfile(input):
        df = process_sql_file(sql_file=input,
                              database_name=database_name,
                              cluster_name=cluster_name,
                              schema_name=schema_name,
                              source_database_name=source_database_name,
                              source_cluster_name=source_cluster_name,
                              dialect=dialect)
        dfs.append(df)
    elif os.path.isdir(input):
        input_dir = Path(input)
        for sql_file in input_dir.rglob('*.sql'):
            df = process_sql_file(sql_file=sql_file,
                                  database_name=database_name,
                                  cluster_name=cluster_name,
                                  schema_name=schema_name,
                                  source_database_name=source_database_name,
                                  source_cluster_name=source_cluster_name,
                                  dialect=dialect)
            dfs.append(df)
    else:
        dfs.append(convert_sql_to_metadata(
                filename=None,
                sql_stmt=input,
                database_name=database_name,
                cluster_name=cluster_name,
                schema_name=schema_name,
                table_name=table_name,
                source_database_name=source_database_name,
                source_cluster_name=source_cluster_name,
                dialect=dialect
            )
        )

    combined_df = pd.concat(dfs, ignore_index=True)
    combined_df.to_csv(output, index=False)

def main():
    # Setup argument parser
    parser = argparse.ArgumentParser(description="Script to generate column lineage from sql")

    # Add arguments
    parser.add_argument('--input', '-i', required=True, help='Input file path, dir path, or sql stmt.  Can be a directory.')
    parser.add_argument('--database', '-d', required=False, help='The database type (ie "hive", "oracle", etc)')
    parser.add_argument('--cluster', '-c', required=False, help='The logical grouping of datasets in a database. (ie "cobra")')
    parser.add_argument('--schema', '-s', required=False, help='The schema name. (ie "credw_cre_curated")')
    parser.add_argument('--table', '-t', required=False, help='The table name.  Requred if --input is a sql statement.  Ignored if --input is sql file/dir')
    parser.add_argument('--source_database', '-ud', required=False, help='The source database type (ie "hive", "oracle", etc)')
    parser.add_argument('--source_cluster', '-uc', required=False, help='The logical grouping of datasets in the source database. (ie "wells_fargo")')
    parser.add_argument('--dialect', '-dt', required=False, default=None, help='The sql dialect name (ie "hive", "oracle"). Default is None')
    parser.add_argument('--output', '-o', required=True, help='Output file path. (ie ./output/sql_parsed_output.csv)')

    # Parse arguments
    args = parser.parse_args()

    if not args.input:
        raise Exception('Missing required arg --input')
    if not args.output:
        raise Exception('Missing required arg --output')
    if not os.path.isfile(args.input) and not os.path.isdir(args.input) and not args.table:
        raise Exception('Missing required arg --table when --input is a sql statement')
    if (os.path.isfile(args.input) or os.path.isdir(args.input)) and args.table:
        LOGGER.warning('Arg --table ignored when --input is file or dir')

    process(
        input=args.input,
        output=args.output,
        database_name=args.database,
        cluster_name=args.cluster,
        schema_name=args.schema,
        table_name=args.table,
        source_database_name=args.source_database,
        source_cluster_name=args.source_cluster,
        dialect=args.dialect
    )

def _test():
    process(
        input="""
            -- Set the schema search path
            SET search_path TO public;

            -- Drop a table if it exists
            DROP TABLE IF EXISTS users;

            -- Create a new table
            CREATE TABLE users (
                id SERIAL PRIMARY KEY,
                username VARCHAR(50) NOT NULL,
                email VARCHAR(100) NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );

            -- Create another table and insert data into it using a SELECT statement
            CREATE TABLE active_users AS
            SELECT id, username, email
            FROM users
            WHERE created_at > CURRENT_DATE - INTERVAL '30 days';

            -- Insert data into users from another table or a select query
            INSERT INTO users (username, email)
            SELECT username, email
            FROM old_users
            WHERE active = true;

            -- Drop database if it exists
            DROP DATABASE IF EXISTS archive_db;

            -- Create a new database
            CREATE DATABASE archive_db;

            -- Use a conditional expression in a select query
            SELECT
                id,
                username,
                CASE
                    WHEN created_at > CURRENT_DATE - INTERVAL '30 days' THEN 'recent'
                    ELSE 'older'
                END AS account_status
            FROM users;
        """,
        output="./test.csv",
        database_name='test_db',
        cluster_name='test_cluster',
        schema_name='test_schema',
        table_name='test_table',
        # source_database_name='test_source_database',
        # source_cluster_name='test_source_cluster',
        dialect='postgres'
    )

    # process(
    #     input="""
    #         -- Create a temporary table using a Common Table Expression (CTE)
    #         WITH sales_summary AS (
    #             SELECT
    #                 s.sales_id,
    #                 s.customer_id,
    #                 s.product_id,
    #                 s.sale_date,
    #                 s.quantity,
    #                 s.total_amount,
    #                 p.category,
    #                 ROW_NUMBER() OVER (PARTITION BY s.customer_id ORDER BY s.sale_date DESC) AS row_num
    #             FROM test_xxx_schema.sales s
    #             JOIN test_yyy_schema.products p ON s.product_id = p.product_id
    #             WHERE s.sale_date >= '2023-01-01'
    #         ),

    #         -- CTE to calculate total sales per category
    #         category_totals AS (
    #             SELECT
    #                 category,
    #                 SUM(total_amount) AS total_sales
    #             FROM sales_summary
    #             GROUP BY category
    #         )

    #         -- Final query to get the results
    #         SELECT
    #             ss.customer_id,
    #             ss.product_id,
    #             ss.sale_date,
    #             ss.quantity,
    #             ss.total_amount,
    #             ss.category,
    #             ct.total_sales AS category_total_sales
    #         FROM sales_summary ss
    #         JOIN category_totals ct ON ss.category = ct.category
    #         WHERE ss.row_num = 1
    #         ORDER BY ss.sale_date DESC;
    #     """,
    #     output="./test.csv",
    #     database_name='test_db',
    #     cluster_name='test_cluster',
    #     schema_name='test_schema',
    #     table_name='test_table',
    #     # source_database_name='test_source_database',
    #     # source_cluster_name='test_source_cluster',
    #     dialect='hive'
    # )



if __name__ == '__main__':
    try:
        # main()

        _test()

    except Exception as e:
        LOGGER.exception(f"Failed to parse sql: ")
    finally:
        LOGGER.info("Done!")
