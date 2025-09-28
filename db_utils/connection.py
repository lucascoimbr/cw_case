from abc import ABC, abstractmethod
import os
import psycopg2
import pandas as pd
from psycopg2 import sql, extras
from psycopg2.extras import RealDictCursor
from enum import Enum

class DBConnectionBase(ABC):
    """
    Abstract Base Class for database connection managers.
    """

    @abstractmethod
    def connect(self):
        """
        Establishes a connection to the database.
        """
        pass

    @abstractmethod
    def ensure_connection(self):
        """
        Ensures the database connection is active. Reconnects if the connection is closed.
        """
        pass

    @abstractmethod
    def cursor(self, cursor_factory=None):
        """
        Returns a database cursor. Ensures connection is active.
        """
        pass

    @abstractmethod
    def close(self):
        """
        Closes the database connection.
        """
        pass

    @abstractmethod
    def rollback(self):
        """
        Rolls back the current transaction.
        """
        pass

    @abstractmethod
    def commit(self):
        """
        Commits the current transaction.
        """
        pass
    
    @abstractmethod
    def insert_data(self,conn_manager, table_name, data):
        """
        Inserts data into a table generically.

        :param conn_manager: DBConnectionBase instance
        :param table_name: Name of the table to insert data into
        :param data: List of dictionaries representing rows to insert
        """
        pass
    
    @abstractmethod
    def update_data(self, table_name, data, condition_column):
        """
        Updates data in a table generically based on a specified condition column.

        :param table_name: Name of the table to update data in
        :param data: List of dictionaries representing rows to update. Each dictionary should contain the column names and values.
        :param condition_column: The column name to use as the condition for identifying the rows to update
        """
        pass
    
    @abstractmethod
    def execute_query(self, query):
        """Executes a single SQL query using the provided database connection."""
        pass
    
    @abstractmethod
    def query_to_dataframe(self, query: str, params=None) -> pd.DataFrame:
        """
        Executes a query and returns the result as a pandas DataFrame.
        """
        pass

    @abstractmethod
    def debug_where_am_i(self):
        """
        Debugging method to print current database connection details.
        """
        pass

class PostgresConnectionManager(DBConnectionBase):
    def __init__(self):
        self.db_config = {
        "dbname": os.getenv("POSTGRES_DB"),
        "user": os.getenv("POSTGRES_USER"),
        "password": os.getenv("POSTGRES_PASSWORD"),
        "host": os.getenv("POSTGRES_HOST"),
        "port": os.getenv("POSTGRES_PORT")
    }
        self.connect()

    def connect(self):
        """
        Establish a connection to the PostgreSQL database using provided configuration.
        """
        self.db_connection = psycopg2.connect(**self.db_config)

    def ensure_connection(self):
        """
        Ensures the PostgreSQL connection is active. Reconnects if the connection is closed.
        """
        if self.db_connection.closed != 0:
            self.connect()

    def cursor(self, cursor_factory=None):
        self.ensure_connection()
        if cursor_factory:
            return self.db_connection.cursor(cursor_factory=cursor_factory)
        return self.db_connection.cursor()

    def close(self):
        if self.db_connection:
            self.db_connection.close()

    def rollback(self):
        if self.db_connection:
            self.db_connection.rollback()

    def commit(self):
        if self.db_connection:
            self.db_connection.commit()
    
    def update_data(self, table_name, data, condition_column = "id"):
        """
        Updates data in a table generically based on a specified condition column.

        :param table_name: Name of the table to update data in
        :param data: List of dictionaries representing rows to update. Each dictionary should contain the column names and values.
        :param condition_column: The column name to use as the condition for identifying the rows to update
        """
        
        query_status = False
        if not data:
            print(f"No data to update for table '{table_name}'.")
            return query_status

        # Convert NaN values to None in the data and Enum values to their string representation
        data = [
            {k: (None if pd.isna(v) else (v.value if isinstance(v, Enum) else v)) for k, v in item.items()} 
            for item in data
        ]

        # Obtain column names from the first dictionary, excluding the condition column
        column_names = [col for col in data[0].keys() if col != condition_column]

        # Build the SQL query dynamically
        update_query = sql.SQL("""
            UPDATE {table}
            SET {fields}
            WHERE {condition} = %s;
        """).format(
            table=sql.Identifier(table_name),
            fields=sql.SQL(', ').join([sql.Identifier(col) + sql.SQL(" = %s") for col in column_names]),
            condition=sql.Identifier(condition_column)
        )
        
        try:
            with self.cursor() as cursor:
                # Execute update for each row
                for item in data:
                    values = [item.get(col) for col in column_names]
                    condition_value = item.get(condition_column)
                    cursor.execute(update_query.as_string(self.db_connection), values + [condition_value])

                self.commit()
                query_status = True
        except Exception as e:
            self.rollback()
            print(f"Error updating data in table '{table_name}': {e}")
        finally:
            return query_status
            
    def insert_data(self, table_name, data):
        """
        Inserts data into a table generically.

        :param conn_manager: DBConnectionBase instance
        :param table_name: Name of the table to insert data into
        :param data: List of dictionaries representing rows to insert
        """
        
        query_status = False
        if not data:
            print(f"No data to insert for table '{table_name}'.")
            return query_status

        # Convert NaN values to None in the data and Enum values to their string representation
        data = [{k: (None if pd.isna(v) else (v.value if isinstance(v, Enum) else v)) for k, v in item.items()  if k not in ["cache_manager", "db_connection"]} for item in data] 
        
        # Obtain column names from the first dictionary
        column_names = list({key for item in data for key in item.keys() if key not in ["cache_manager", "db_connection"]})

        # Create a list of tuples with the values for each row
        values = [tuple(item.get(col) for col in column_names) for item in data]
        
        # Build the SQL query dynamically
        insert_query = sql.SQL("""
            INSERT INTO {table} ({fields}) VALUES %s
            ON CONFLICT DO NOTHING;
        """).format(
            table=sql.Identifier(table_name),
            fields=sql.SQL(', ').join(map(sql.Identifier, column_names))
        )
        
        try:
            with self.cursor() as cursor:
                extras.execute_values(cursor, insert_query.as_string(self.db_connection), values)
            self.commit()
            # print(f"Data inserted into table '{table_name}'.")
            query_status = True
        except Exception as e:
            self.rollback()
            print(f"Error inserting data into table '{table_name}': {e}")
        finally:
            return query_status
    
    def execute_query(self, query):
        """Executes a single SQL query using the provided database connection."""

        status = False
        try:
            cursor = self.cursor()
            cursor.execute(query)
            self.commit()
            print("Query executed successfully.")
            status = True
        except Exception as e:
            self.rollback()
            print(f"An error occurred: {e}")
        finally:
            return status

    def query_to_dataframe(self, query: str, params=None) -> pd.DataFrame:
        """
        Executes a query and returns the result as a pandas DataFrame.
        """
        try:
            with self.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute(query, params)
                records = cursor.fetchall()
                df = pd.DataFrame(records)
                return df
        except psycopg2.Error as e:
            print(f"Error executing query: {e}")
            raise
        
    def get_element_from_id(self, table_name, element_id):
        """
        Fetches a single element by ID from a specified table.
        
        :param conn: Active database connection (psycopg2 connection object)
        :param table_name: Name of the table to fetch the element from
        :param element_id: ID of the element to fetch
        :return: Dictionary containing the element data, or None if not found
        """
        query = f"SELECT * FROM {table_name} WHERE id = %s"
        
        with self.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute(query, (element_id,))
            result = cursor.fetchone()  # Fetch a single row as a dictionary

        return dict(result)
    
    def connection(self):
        return self.db_connection
    
    def debug_where_am_i(self):
        with self.cursor() as cur:
            cur.execute("""
                SELECT
                current_database()   AS db,
                current_user         AS user,
                inet_server_addr()   AS host_ip,
                inet_server_port()   AS port,
                current_schema()     AS schema;
            """)
            print(cur.fetchone())
        
    
        
        
