from psycopg2.extras import RealDictCursor
import psycopg2
import pandas as pd

def execute_queries(conn, queries):
    """Executes a list of SQL queries using the provided database connection."""
    try:
        cursor = conn.cursor()
        for query in queries:
            cursor.execute(query)
        conn.commit()
        print("Tables created successfully.")
    except Exception as e:
        conn.rollback()
        print(f"An error occurred: {e}")

def generate_create_table_from_schema(table_dict):
    """
    Generates a create table query from a schema dictionary.
    """
    table_name = table_dict["name"]
    columns = table_dict["columns"]

    column_definitions = []
    fk_keys = []
    
    for column in columns:
        column_def = f"{column['name']} {column['type']}"
        
        if column.get("pk"):
            column_def += " PRIMARY KEY"

        if column.get("not_null"):
            column_def += " NOT NULL"
        
        if column.get("unique"):
            column_def += " UNIQUE"
            
        if column.get("check"):
            column_def += f" CHECK ({column['name']} IN ({','.join([repr(item) for item in column['check']])}))"

        if "default" in column:
            column_def += f" DEFAULT {column['default']}"

        if "fk" in column:
            fk = column["fk"]
            
            fk_keys.append(f"FOREIGN KEY ({column['name']}) REFERENCES {fk['table']}({fk['column']})")

        column_definitions.append(column_def)
        
    column_definitions.extend(fk_keys)
        
    create_table_query = f"CREATE TABLE {table_name} (\n    " + ",\n    ".join(column_definitions) + "\n);"
    
    return create_table_query

def get_fk_columns_from_initial_setup(schema):
    """
    Extracts foreign key columns from the initial setup schema.
    """
    
    all_keys = {key for row in schema for key in row.keys()}
    
    dotted_columns = [{"table": dotted_col.split(".")[0], "col": dotted_col.split(".")[1], "dotted_name": dotted_col} for dotted_col in all_keys if "." in dotted_col]
    
    return dotted_columns 

def join_fk_tables(conn, schema):
    """
    Joins tables with foreign key relationships.
    """
    
    fk_columns = get_fk_columns_from_initial_setup(schema)
    
    schema_df = pd.DataFrame(schema)
    
    for fk_col in fk_columns:
        
        query = f"select * from {fk_col['table']}"
        new_id_name = f"{fk_col['table']}_id"
        f_df = conn.query_to_dataframe(query)[["id", fk_col['col']]].rename(columns={'id': new_id_name, fk_col['col']: fk_col['dotted_name']})
        
        schema_df = schema_df.merge(f_df, on=fk_col['dotted_name'], how='left')
        
    schema_df = schema_df.drop(columns=[col for col in schema_df.columns if "." in col]).astype('object')
    
    schema_dict = schema_df.to_dict(orient='records')
    
    return schema_dict

def update_data(conn, update_dict):
    """
    Updates data in a table generically.
    """
    
    status = False
    table_name = update_dict["table"]
    set_values = update_dict["set"]
    where_values = update_dict["where"]
    
    update_timestamp = "update_timestamp = NOW()"
    
    set_query = ", ".join([f"{k} = '{v}'" for k, v in set_values.items()])+ f", {update_timestamp}"
    
    where_query = " AND ".join([f"{k} = '{v}'" for condition in where_values for k, v in condition.items()])
    
    update_query = f"UPDATE {table_name} SET {set_query} WHERE {where_query}"
    
    try:
        cursor = conn.cursor()
        cursor.execute(update_query)
        conn.commit()
        print(f"Data updated in table '{table_name}'.")
        status = True
    except Exception as e:
        conn.rollback()
        print(f"Error updating data in table '{table_name}': {e}")
    finally:
        return status
