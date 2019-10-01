from sqlalchemy import create_engine, Table, Column, Integer, Float, String, MetaData, ForeignKey
import datetime as dt
import pandas as pd

# Create Database called 'inglewood.db'
engine = create_engine('sqlite:///inglewood.db') 

# Create tables within the Database
metadata = MetaData()

nodes = Table('nodes', metadata,
    Column('id', Integer, primary_key=True, nullable=False),
    Column('lat', Float),
    Column('lon', Float),
    Column('user', String),
    Column('uid', Integer),
    Column('version', String),
    Column('changeset', Integer),
    Column('timestamp', String)
)

nodes_tags = Table('nodes_tags', metadata,
    Column('id', Integer, ForeignKey('nodes.id'), nullable=False),
    Column('key', String),
    Column('value', String),
    Column('type', String),
)

ways = Table('ways', metadata,
    Column('id', Integer, primary_key=True, nullable=False),
    Column('user', String),
    Column('uid', Integer),
    Column('version', String),
    Column('changeset', Integer),
    Column('timestamp', String)
)

ways_tags = Table('ways_tags', metadata,
    Column('id', Integer, ForeignKey('ways.id'), nullable=False),
    Column('key', String),
    Column('value', String),
    Column('type', String),
)

ways_nodes = Table('ways_nodes', metadata,
    Column('id', Integer, ForeignKey('ways.id'), nullable=False),
    Column('node_id', Integer, ForeignKey('nodes.id'), nullable=False),
    Column('position', Integer, nullable=False)
)

metadata.create_all(engine)

# Load csv files into Python Pandas DataFrames, then load them into SQLite database 
def csv_to_db(csvfile, table):
    print('Loading {}'.format(csvfile))
    start = dt.datetime.now()
    chunksize = 200000
    j = 0
    for df in pd.read_csv(csvfile, chunksize=chunksize, iterator=True, encoding='utf-8'):
        j+=1
        df.to_sql(table, engine, if_exists='append', index=False)

csv_to_db('nodes.csv', 'nodes')
csv_to_db('nodes_tags.csv', 'nodes_tags')
csv_to_db('ways.csv', 'ways')
csv_to_db('ways_tags.csv', 'ways_tags')
csv_to_db('ways_nodes.csv', 'ways_nodes')