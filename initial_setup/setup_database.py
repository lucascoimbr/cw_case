from utils.helpers import load_data_from_csv_url, load_data_from_json
from db_utils import PostgresConnectionManager, generate_create_table_from_schema, join_fk_tables

import os
from dotenv import load_dotenv
load_dotenv()

create_tb_queries = {}

def create_all_enums(conn, schema_path, drop_schema=True):
    schemas = load_data_from_json(schema_path)
    
    print(schemas['enums'])
    if conn:
        
        if drop_schema:
            print("\nDropping schema\n")
            conn.execute_query( "DROP SCHEMA IF EXISTS public CASCADE;")
            conn.execute_query( "CREATE SCHEMA public;")
        
        # create uuid extension 
        conn.execute_query( "CREATE EXTENSION IF NOT EXISTS \"uuid-ossp\";")
        
        # Creates all ENUMs
        print("\nCreating enums\n")
        for enum in schemas['enums']:
            
            enum_name = enum['name']
            enum_values = enum['values']
            enum_values = ', '.join([f"'{val}'" for val in enum_values])
            enum_query = f"CREATE TYPE {enum_name} AS ENUM ({enum_values});"
            conn.execute_query( enum_query)
    
def create_all_tables(conn, schema_path):
    
    # Create tables from schemas.json ############################################################################
    schemas = load_data_from_json(schema_path)
    if conn:
        
        print("\nCreating tables\n")
        
        all_schemas = schemas['tables']
        
        # List of SQL statements to drop and recreate tables
        all_queries = [
            [f"DROP TABLE IF EXISTS {schema['name']} CASCADE;",
             generate_create_table_from_schema(schema)] for schema in all_schemas
            ]
        
        all_queries = [item for row in all_queries for item in row]
        
        for query in all_queries:
            result = conn.execute_query(query)
            if not result:
                print(query,'\n')
                pass

def insert_initial_transaction_data(conn, csv_url = os.getenv("CSV_URL", ""), table_name = "transactions"):

    # Insert data into each table using the existing connection ###############################################
    # Load initial data from the provided CSV URL
    data = load_data_from_csv_url(csv_url)

    data = conn.insert_data(table_name, data)

def main():

    conn = PostgresConnectionManager()
    
    schema_path = "initial_setup/cons/schemas.json"

    create_all_enums(conn, schema_path)
    
    create_all_tables(conn, schema_path)

    csv_url = os.getenv("CSV_URL", "")
    insert_initial_transaction_data(conn, csv_url, "transactions")
        
    conn.close()

if __name__ == '__main__':
    main()



