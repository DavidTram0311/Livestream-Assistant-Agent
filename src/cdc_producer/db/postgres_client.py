import os
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from dotenv import load_dotenv
from .base import Base
import pandas as pd

load_dotenv()

class PostgresSQLClient:
    def __init__(self, database, user, password, host, port):
        self.database = database
        self.user = user
        self.password = password
        self.host = host
        self.port = port
        self.engine = None
        self.session = None
        self._connect()

    def _connect(self):
        url = f"postgresql://{self.user}:{self.password}@{self.host}:{self.port}/{self.database}"
        print(f"Connecting to {url}")
        self.engine = create_engine(f"postgresql://{self.user}:{self.password}@{self.host}:{self.port}/{self.database}")
        self.session = sessionmaker(bind=self.engine)

    def get_session(self):
        return self.session()
    
    def create_tables(self):
        Base.metadata.create_all(self.engine)

    def drop_tables(self):
        Base.metadata.drop_all(self.engine)

    def execute_query(self, query, values=None):
        with self.engine.connect() as connection:
            if values:
                return connection.execute(query, values)
            else:
                return connection.execute(query)

    def get_columns(self, table_name):
        df = pd.read_sql(f"select * from {table_name} LIMIT 0", self.engine)
        return df.columns
