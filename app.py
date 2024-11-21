import pandas as pd
import psycopg2
from sqlalchemy import create_engine

def process_data(engine):
    connect = engine.connect()

    data = pd.read_sql('SELECT * FROM test_table WHERE CHAR_LENGTH(name) < 6', connect)

    grouped = data.groupby('name').agg({'age': ['min', 'max']}).reset_index()

    grouped.columns = ['name', 'min_age', 'max_age']

    min_record = grouped.loc[grouped['min_age'].idxmin()]
    max_record = grouped.loc[grouped['max_age'].idxmax()]

    min_max_data = pd.DataFrame({'extreme': ['min', 'max'], 'name': [min_record['name'], max_record['name']], 'age': [min_record['min_age'], max_record['max_age']]})

    return min_max_data

if __name__ == '__main__':
    db_user = 'postgres'
    db_pass = '12345'
    db_host = 'db'
    db_port = '5432'
    db_name = 'neretina'

    engine = create_engine(f'postgresql://{db_user}:{db_pass}@{db_host}:{db_port}/{db_name}')

    result = process_data(engine)

    print(result)
