import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import SQLAlchemyError

# Database connection parameters
DB_HOST = 'localhost'        # Host address of the PostgreSQL database
DB_USER = 'postgres'         # Username for database authentication
DB_PASSWORD = 'Password'     # Password for the database user
DB_PORT = "5432"             # Port number for PostgreSQL (default: 5432)
DB_NAME = 'health'           # Name of the target database

def create_sqlalchemy_engine():
    """
    Creates a SQLAlchemy engine for database connection.
    
    Returns:
        sqlalchemy.engine.Engine: The engine object to interact with the database.
    Raises:
        SQLAlchemyError: If there is an error creating the engine.
    """
    try:
        # Construct the connection string using the provided database credentials
        connection_string = f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
        
        # Create the SQLAlchemy engine
        engine = create_engine(connection_string)
        return engine
    except SQLAlchemyError as e:
        # Print and raise the error for debugging purposes
        print(f"Error creating database engine: {str(e)}")
        raise

def insert_to_sql(df, table_name, if_exists='replace'):
    """
    Insert a pandas DataFrame into a PostgreSQL table using SQLAlchemy.

    Args:
        df (pandas.DataFrame): The DataFrame containing data to be inserted.
        table_name (str): Name of the target table in the database.
        if_exists (str, optional): Specifies what to do if the table already exists.
                                   Options: 'fail', 'replace', 'append'. Default is 'replace'.

    Raises:
        SQLAlchemyError: If there is an error during the insertion process.
    """
    try:
        # Create a database engine
        engine = create_sqlalchemy_engine()
        
        # Use pandas' to_sql method to insert the DataFrame into the database
        df.to_sql(
            name=table_name, 
            con=engine, 
            if_exists=if_exists, 
            index=False  # Do not include the DataFrame index as a database column
        )
        print(f"Successfully inserted data into table: {table_name}")
    except SQLAlchemyError as e:
        # Print and raise the error if the insertion fails
        print(f"Error inserting data into {table_name}: {e}")
        raise
    finally:
        # Dispose of the engine to release database resources
        engine.dispose()

def test_connection():
    """
    Test the connection to the PostgreSQL database.

    Returns:
        bool: True if the connection is successful, False otherwise.
    """
    try:
        # Create a database engine
        engine = create_sqlalchemy_engine()
        
        # Establish a connection using the engine
        with engine.connect() as connection:
            print("Database connection successful!")
            return True
    except SQLAlchemyError as e:
        # Print the error if the connection fails
        print(f"Database connection failed: {e}")
        return False
