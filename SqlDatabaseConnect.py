import psycopg2
import pandas as pd
from sqlalchemy import create_engine
from typing import Any
import csv
from io import StringIO


## use psycopg2 to connect to database
class SqlDataWork:
    def __init__(self, host, port, username, password, database_name):
        self.host = host
        self.port = port
        self.username = username
        self.password = password
        self.database_name = database_name
        
    def get_data(self, sql_statement: str):
        with psycopg2.connect(host=self.host, user=self.username,
                                password=self.password, dbname=self.database_name, 
                                port=self.port
                            ) as conn:
            with conn.cursor() as cur:
                cur.execute(sql_statement)
                records = cur.fetchall()
                
            return records
        


## Q1 Create class to connect to data
class PsqlEngine:
    def __init__(self, username: str, password: Any,
                 hostname: Any, port: int,
                 database_name: str):
        self.username = username
        self.password = password
        self.hostname = hostname
        self.database_name = database_name
        self.port = port
        # %%  define connection string
        self.connection_path = ("postgresql+psycopg2://{username}:{password}"
                         "@{hostname}:{port}/{database_name}"
                         )

#%% fill in parameters to create database engine
        self.engine = create_engine(self.connection_path.format(
                                                                username=self.username, 
                                                                password=self.password, 
                                                                hostname=self.hostname,
                                                                port=self.port, 
                                                                database_name=self.database_name
                                                            )
                                    )
        
    def get_table_as_dataframe(self, database_table_name: str) -> pd.DataFrame:
        return pd.read_sql_table(table_name=database_table_name, con=self.engine)
    
    def fetch_sqlquery_as_dataframe(self, sqlquery_statement: Any) -> pd.DataFrame:
        return pd.read_sql_query(sql=sqlquery_statement, con=self.engine)
    
    
    def send_data_to_sqldatabase(self, data, table_name: str, index=False, if_exists='replace'):
        #%% write to database
        data.to_sql(name=table_name, con=self.engine,
                    index=index, if_exists=if_exists
                )
        
        
    #def send_large_data_to_sqldatabase(table, conn, keys, data_iter):
        #%%
    def psql_copy_method(self, table, conn, keys, data_iter):
        self.table = table
        # get a DNAPI connection that can provide a cursor
        dbapi_conn = conn.connection
        with dbapi_conn.cursor() as cur:
            s_buf = StringIO()
            writer = csv.writer(s_buf)
            writer.writerows(data_iter)
            s_buf.seek(0)
            
            columns = ', '.join('"{}"'.format(k) for k in keys)
            if self.table.schema:
                table_name = '{}.{}'.format(self.table.schema, self.table.name)
            else:
                table_name = self.table.name
                
            sql = 'COPY {} ({}) FROM STDIN WITH CSV'.format(
                        table_name, columns
                    )
            cur.copy_expert(sql=sql, file=s_buf)
    
    def copy_data_to_psqldatabase(self, data, table_name, psql_copy_method):
        
            data.to_sql(name=table_name, con=self.engine, 
                        index=False, if_exists='replace', 
                        method=psql_copy_method
                    )
            
            print('Success')
            
    ###### throws internal error
    # def create_psql_database(self, database_name: str):
    #     query = f"CREATE DATABASE {database_name}"
    #     self.engine.execute(query)
    #     print(f'Database {database_name} created!')

    



