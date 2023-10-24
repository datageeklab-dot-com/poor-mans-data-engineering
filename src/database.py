import sqlite3
import pandas as pd
from sqlalchemy import create_engine

def create_database(db_name):
    """
    Create a SQLite database and return an SQLAlchemy engine.

    Args:
        db_name (str): The name of the SQLite database.

    Returns:
        sqlalchemy.engine.base.Engine: SQLAlchemy engine connected to the database.
    """
    engine = create_engine(f'sqlite:///sample_db/{db_name}')
    return engine

def save_data_to_database(engine, data, table_name,mode):
    """
    Save DataFrame data to a SQLite database table using SQLAlchemy.

    Args:
        engine (sqlalchemy.engine.base.Engine): SQLAlchemy engine connected to the database.
        data (pandas.DataFrame): Data to be saved to the database.
        table_name (str): Name of the table in the database.
    """
    if mode=="backfill":
        data.to_sql(table_name, con=engine, if_exists='replace', index=False)
    elif mode=="increment":
        data.to_sql(table_name, con=engine, if_exists='append', index=False)
